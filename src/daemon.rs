// SPDX-License-Identifier: GPL-3.0-or-later

pub mod core;
pub mod startup;

use dashmap::DashMap;
use nix::sys::signal::kill as nixkill;
use nix::unistd::Pid;
use reqwest::Client;
use serde::Serialize;
use std::collections::HashMap;
use std::fmt;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, atomic::AtomicU16};
use tokio::net::{UnixListener, UnixStream};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::{
    mpsc::{Receiver, Sender},
    oneshot,
};
use tokio::task::JoinHandle;
use tokio_rusqlite::Connection as SqlConn;
use tracing::{info, instrument};

use crate::ansible::destroy_server;
use crate::cli::{
    CloudSummary, DestroyOpts, ExpandOpts, RebaseOpts, ReplyFormat, ServerCreds, ServerSummary,
    ShowSecrets, StatusOpts, SvcHealthSummary, SvcSummary, TaskSummary,
};
use crate::cloud::reconcile::{RebaseData, ReconMode, create_rebase_data};
use crate::cloud::{parse_cloud_config, spawn_task};
use crate::constants::{EXIT_REBASE, EXIT_RELOAD, EXIT_REMAP, EXIT_RESTART, PID_NAME, SOCKET_NAME};
use crate::db::create_backup;
use crate::types::{
    BoxError, Cloud, Daemon, DashAction, ServerState, SvcEntry, SvcHealth, TaskEntry, TaskHandle,
    TaskMap, WorkSpace,
};

/// The list of commands that are used for channel communication with the the
/// daemon. Most are used in forwarding CLI commands that are sent to the
/// socket. See [`crate::cli::CliComm`] and refer to
/// [`crate::cloud::reconcile::reconcile_cloud`] for more information.
pub enum DaemonComm {
    Reload,
    Remap,
    Rebase(RebaseOpts),
    Status(StatusOpts),
    Shutdown,
    Restart,
    ResetFix,
    Expand(ExpandOpts),
    Credentials(ShowSecrets),
    Clients,
    /// Used for a real-time check if a sibling full setup or destruction task
    /// is running. This is an internal command and is not coupled with any
    /// CliComm.
    SetupInquiry(TaskEntry),
    Destroy(DestroyOpts),
    /// Any unwanted or misplaced command from the socket is matched to this
    Unknown,
}

/// Holds information about a request that is sent to the daemon channel
pub struct DaemonRequest {
    pub reply: oneshot::Sender<Result<DaemonReply, BoxError>>,
    pub command: DaemonComm,
}

/// The reply enum that the daemon emits in its communication channel
#[derive(Serialize, Debug)]
pub enum DaemonReply {
    Message(String),
    Status(CloudSummary),
    Credentials(HashMap<String, ServerCreds>),
    Clients(HashMap<String, String>),
    SetupInquiry(bool),
}

/// When daemon exits, this struct carries data to be used in the next run
pub struct DaemonNext {
    code: i8,
    mode: ReconMode,
    old_cloud: Option<Cloud>,
    new_cloud: Option<Cloud>,
    rebase_data: Option<HashMap<String, RebaseData>>,
}

/// Signals that are sent to the service monitoring tasks
pub enum SvcComm {
    /// Resets the fix_try for every service. This signal can be used to give
    /// the daemon a chance to retry fix actions.
    ResetFix,

    /// Collects summaries of all major services from all servers (i.e., SSH,
    /// Nginx, and Xray)
    GetSummary(oneshot::Sender<SvcSummary>),
}

/// When the daemon starts and acquires the socket and PID locks, this struct
/// holds the daemon channel information and the joinhandle of socket listener
/// task
pub struct DaemonLock {
    channel: (Sender<DaemonRequest>, Receiver<DaemonRequest>),
    socket_jh: JoinHandle<Result<(), BoxError>>,
}

/// A struct holding the prerequisites to the daemon runtime data
#[derive(Clone)]
pub struct DaemonPrereq {
    sqlconn: Arc<SqlConn>,
    cf_list: Option<String>,
    cf_client: Client,
}

/// A struct holding the runtime state of the daemon
pub struct DaemonRuntime {
    taskmap: TaskMap,
    svc_chat: HashMap<TaskEntry, Sender<SvcComm>>,
    action_map: Arc<DashMap<TaskEntry, DashAction>>,
    atomic_port: Arc<AtomicU16>,
    aggregator_ch: (Sender<(usize, SvcEntry)>, Receiver<(usize, SvcEntry)>),
}

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

impl fmt::Display for DaemonComm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DaemonComm::Unknown => write!(f, "Unknown"),

            DaemonComm::Expand(ExpandOpts { server: Some(name) }) => write!(f, "Expand on {name}"),
            DaemonComm::Expand(ExpandOpts { server: None }) => write!(f, "Expand"),

            DaemonComm::Rebase(RebaseOpts { forced: true }) => write!(f, "Forced Rebase"),
            DaemonComm::Rebase(RebaseOpts { forced: false }) => write!(f, "Rebase"),

            DaemonComm::Reload => write!(f, "Reload"),
            DaemonComm::Remap => write!(f, "Remap"),
            DaemonComm::ResetFix => write!(f, "ResetFix"),
            DaemonComm::Restart => write!(f, "Restart"),
            DaemonComm::Shutdown => write!(f, "Shutdown"),
            DaemonComm::Status(StatusOpts { full: false }) => write!(f, "Status"),
            DaemonComm::Status(StatusOpts { full: true }) => write!(f, "Full Status"),
            DaemonComm::Credentials(ShowSecrets { show_all: true }) => {
                write!(f, "Credentials-All")
            }
            DaemonComm::Credentials(ShowSecrets { show_all: false }) => write!(f, "Credentials"),
            DaemonComm::Clients => write!(f, "Clients"),
            DaemonComm::SetupInquiry(task_entry) => {
                write!(f, "Inquiry on {}", task_entry.nice_display())
            }
            DaemonComm::Destroy(DestroyOpts { server }) => write!(f, "Destroy {server}"),
        }
    }
}

impl DaemonReply {
    /// Decides which ReplyFormat should be used when daemon replies back to a
    /// request from socket
    pub fn reply_format(&self) -> ReplyFormat {
        match self {
            DaemonReply::Credentials(_) => ReplyFormat::Toml,
            DaemonReply::Clients(_) => ReplyFormat::Toml,
            _ => ReplyFormat::Json,
        }
    }
}

impl Daemon {
    // Since the methods are used during startup, using sync versions are
    // fine, except those that handle socket connection
    /// Initialize the Daemon struct with defined paths
    fn new(runtime_dir: &Path) -> Self {
        Self {
            pid_file: runtime_dir.join(PID_NAME),
            sock_file: runtime_dir.join(SOCKET_NAME),
        }
    }
    // =============================================================
    /// The default DeamonNext state when starting up
    fn init_state() -> DaemonNext {
        // When starting up, there is no cloud yet, and the default reconciliation
        // mode is always to perform a Reload.
        DaemonNext {
            code: 0,
            mode: ReconMode::Reload,
            old_cloud: None,
            new_cloud: None,
            rebase_data: None,
        }
    }
    // =============================================================
    /// Ensures that we are the only instance running
    async fn is_unique(&self) -> Result<(), BoxError> {
        // 1. Check PID file
        if self.pid_file.exists() {
            let content = std::fs::read_to_string(&self.pid_file)?;
            if let Ok(pid) = content.trim().parse::<i32>() {
                // Signal 0 or None checks if the process exists, and we have
                // permission to ping it, too
                if nixkill(Pid::from_raw(pid), None).is_ok() {
                    return Err(
                        format!("Error: Daemon is already running with PID: {}", pid).into(),
                    );
                }
            }
            // If not, PID was stale
            std::fs::remove_file(&self.pid_file)?;
        }

        // 2. Check socket
        if self.sock_file.exists() {
            if UnixStream::connect(&self.sock_file).await.is_ok() {
                return Err("Error: Daemon socket is active. Another instance is running.".into());
            }
            // If not, socket was stale
            std::fs::remove_file(&self.sock_file)?;
        }

        Ok(())
    }
    // =============================================================
    /// Atomically writes the PID file
    fn create_pid_file(&self) -> Result<(), BoxError> {
        let tmp_pid = self.pid_file.with_extension("tmp");

        // First write the current PID to a temp file
        let mut f = std::fs::File::create(&tmp_pid)?;
        writeln!(f, "{}", std::process::id())?;
        f.sync_all()?; // Ensure it's on disk

        // OS atomically renames it to prevent from race conditions
        std::fs::rename(tmp_pid, &self.pid_file)?;

        Ok(())
    }
    // =============================================================
    /// Creates the Unix Socket
    fn create_socket(&self) -> Result<UnixListener, BoxError> {
        let listener = UnixListener::bind(&self.sock_file)?;
        Ok(listener)
    }
    // =============================================================
    /// Removes socket and PID files
    fn cleanup(&self) -> Result<(), BoxError> {
        std::fs::remove_file(&self.pid_file)?;
        std::fs::remove_file(&self.sock_file)?;
        info!("cleaned up socket & PID files");

        Ok(())
    }
}
// =============================================================
impl TaskMap {
    /// Lists the tasks names and their status
    fn status(&self) -> Vec<TaskSummary> {
        let mut status = Vec::new();

        for (entry, handle) in &self.tasks {
            status.push(TaskSummary {
                task: entry.nice_display(),
                running: !handle.is_finished(),
            });
        }

        status
    }

    /// Aborts all the spawned monitoring tasks
    #[instrument(name = "daemon", skip_all)]
    fn abort_all(&mut self) {
        for (_id, handle) in self.tasks.drain() {
            handle.abort();
        }

        info!("aborted all tasks")
    }
}
// =============================================================
/// Before applying Restart/Reload/Remap/Rebase, this function checks the cloud
/// config file, evaluates validity of the supplied command line for a Rebase
/// (if intended), creates a backup, and then returns with the new cloud in
/// DaemonNext.
pub async fn check_and_backup(
    curr_cloud: &Cloud,
    workspace: Arc<WorkSpace>,
    sql_conn: Arc<SqlConn>,
    taskmap: &mut TaskMap,
    cmd: DaemonRequest,
) -> Result<Option<DaemonNext>, BoxError> {
    let action = cmd.command.to_string();
    log_command(&action);

    // Note that we don't want to abort the whole daemon if check was
    // unsuccessful. Therefore, we return Ok(Some(cloud)) for a successful check
    // and Ok(None) if the check failed.
    match parse_cloud_config(&workspace).await {
        Ok(new_cloud) => {
            let code: i8;
            let mode: ReconMode;

            match cmd.command {
                DaemonComm::Restart => {
                    code = EXIT_RESTART;
                    mode = ReconMode::Reload;
                }

                DaemonComm::Reload => {
                    code = EXIT_RELOAD;
                    mode = ReconMode::Reload;
                }

                DaemonComm::Remap => {
                    code = EXIT_REMAP;
                    mode = ReconMode::Remap;
                }

                DaemonComm::Rebase(opts) => {
                    code = EXIT_REBASE;
                    mode = ReconMode::Rebase;

                    match create_rebase_data(curr_cloud, &new_cloud, &opts) {
                        Ok(rebase_data) => {
                            let mes = format!("{} is being applied.", action);
                            create_backup(curr_cloud, sql_conn, workspace.clone(), action).await?;

                            let _ = cmd.reply.send(Ok(DaemonReply::Message(mes)));
                            taskmap.abort_all();
                            let daemon_next = DaemonNext {
                                code,
                                mode,
                                old_cloud: Some(curr_cloud.to_owned()),
                                new_cloud: Some(new_cloud),
                                rebase_data: Some(rebase_data),
                            };

                            return Ok(Some(daemon_next));
                        }
                        Err(e) => {
                            let _ = cmd.reply.send(Err(e));
                            return Ok(None);
                        }
                    }
                }

                _ => return Err("Unsuitable DaemonComm variant in check_and_config.".into()),
            }

            let mes = format!("{} is being applied.", action);
            create_backup(curr_cloud, sql_conn, workspace.clone(), action).await?;

            let _ = cmd.reply.send(Ok(DaemonReply::Message(mes)));
            taskmap.abort_all();
            let daemon_next = DaemonNext {
                code,
                mode,
                old_cloud: Some(curr_cloud.to_owned()),
                new_cloud: Some(new_cloud),
                rebase_data: None,
            };

            Ok(Some(daemon_next))
        }

        // The encountered error is sent back to the socket
        Err(e) => {
            let _ = cmd.reply.send(Err(format!(
                "Cloud config file has failed the check. \
		 We can't proceed with '{}'. Error: {}",
                action, e
            )
            .into()));

            Ok(None)
        }
    }
}
// =============================================================
/// Logs received commands
#[instrument(name = "daemon", skip_all)]
fn log_command(cmd: &str) {
    info!(command = cmd, "received");
}
// =============================================================
/// Creates the table of servers' credentials which is given as the response of
/// 'credentials' CLI argument
pub fn build_servers_creds(cloud: &Cloud, show_all: bool) -> Result<DaemonReply, BoxError> {
    let mut creds_hm = HashMap::<String, ServerCreds>::new();
    for server in cloud
        .servers
        .iter()
        .filter(|s| s.enabled && s.state.load() == ServerState::Production)
    {
        let server_creds = ServerCreds {
            doh: format!(
                "https://{}.{}/{}",
                &server.dns_subdomain,
                server.domain.to_string(),
                &server.secrets.doh_endpoint
            ),
            ui: format!(
                "https://{}/{}/",
                server.uihostname(),
                &server.secrets.xui_webpath
            ),
            xui_username: {
                if show_all {
                    Some(server.secrets.xui_username.clone())
                } else {
                    None
                }
            },
            xui_password: {
                if show_all {
                    Some(server.secrets.xui_password.clone())
                } else {
                    None
                }
            },
            xui_token: {
                if show_all {
                    server.secrets.xui_token.0.get().cloned()
                } else {
                    None
                }
            },
        };

        creds_hm.entry(server.name.clone()).or_insert(server_creds);
    }

    Ok(DaemonReply::Credentials(creds_hm))
}
// =============================================================
/// Retrieves all the clients from Production servers via a call to the
/// application panel. This is required because clients are not fully managed by
/// xcplane and the user can freely add/delete clients to/from an inbound.
pub async fn get_cloud_clients(cloud: &Cloud) -> Result<DaemonReply, BoxError> {
    let mut clients = HashMap::<String, String>::new();
    for server in cloud
        .servers
        .iter()
        .filter(|s| s.enabled && s.state.load() == ServerState::Production)
    {
        clients
            .entry(server.name.clone())
            .or_insert(server.xui_call_clients().await?);
    }

    Ok(DaemonReply::Clients(clients))
}
// =============================================================
/// Checks the given server to be destroyed and applies the destruction
pub async fn perform_server_destruction(
    cloud: &Cloud,
    conn: Arc<SqlConn>,
    name: &str,
    taskmap: &mut TaskMap,
    workspace: Arc<WorkSpace>,
    txd: Sender<DaemonRequest>,
) -> Result<DaemonReply, BoxError> {
    if let Some(server) = cloud.servers.iter().find(|s| s.name == name) {
        match (server.state.load(), server.enabled) {
            (ServerState::Offgrid, _) => {
                return Err(format!("Server '{}' is already Offgrid.", server.name).into());
            }

            (ServerState::Production, false) => {
                return Err(format!("Server '{}' is disabled.", server.name).into());
            }

            (ServerState::Production, true) => {
                // Unlike cloud expansion, we match for destruction on exactly the
                // same server
                let entry_opt = taskmap
                    .tasks
                    .keys()
                    .find(|key| *key == &TaskEntry::DestroyServer(server.clone()))
                    .cloned();

                if let Some(entry) = entry_opt {
                    let handle = taskmap
                        .tasks
                        .get(&entry)
                        .ok_or("There is no TaskHandle associated with TaskEntry::DestroyServer")?;

                    if handle.is_finished() {
                        /*
                        If the task has finished, and we still got here, it
                        means our attempt for destroying the server has been
                        unsuccessful. Unlike full setup, we won't opt for a
                        retry and return an error instead.
                         */
                        return Err(format!(
                            "Destruction operation has failed for server \
			     '{}' and it is still Production.",
                            server.name
                        )
                        .into());
                    } else {
                        let mes = format!(
                            "Destruction operation is already going on for server '{}'",
                            server.name
                        );

                        return Ok(DaemonReply::Message(mes));
                    }
                } else {
                    // The destruction operation has never been performed on
                    // the server (in this state of DaemonNext)
                    let entry = TaskEntry::DestroyServer(server.clone());
                    let jh = spawn_task(
                        destroy_server(conn, server.clone(), workspace, txd),
                        entry.clone(),
                    );

                    taskmap
                        .tasks
                        .entry(entry)
                        .or_insert(TaskHandle::Detached(jh));

                    return Ok(DaemonReply::Message(format!(
                        "Server '{}' is being destroyed.",
                        server.name
                    )));
                }
            }
        }
    } else {
        return Err(format!("Server '{}' does not exist in the cloud.", name).into());
    }
}
// =============================================================
/// Prepares a summary of the running cloud which is later given to the user as
/// the response of 'status' CLI argument
pub fn get_summary(
    services: Vec<SvcSummary>,
    tasks_summary: Vec<TaskSummary>,
    status_opts: StatusOpts,
) -> DaemonReply {
    let mut servers: HashMap<String, Vec<SvcHealthSummary>> = HashMap::new();

    for svc in services {
        servers
            .entry(svc.server)
            .or_default()
            .push(SvcHealthSummary {
                service: svc.service.nice_display(),
                health: svc.health,
            });
    }

    let cloud_summary = CloudSummary {
        tasks: tasks_summary,
        cloud: servers
            .into_iter()
            .map(|(server, services)| ServerSummary { server, services })
            .collect(),
    };

    if status_opts.full {
        DaemonReply::Status(cloud_summary)
    } else {
        DaemonReply::Message(humanize_status_report(&cloud_summary))
    }
}
// =============================================================
/// Creates a human-readable report from the summary of cloud
pub fn humanize_status_report(summary: &CloudSummary) -> String {
    let mut lines = Vec::new();

    // Report crashed tasks
    for task in &summary.tasks {
        if !task.running {
            lines.push(format!("Task '{}' has crashed.", task.task));
        }
    }

    // Report unhealthy services
    for server in &summary.cloud {
        for service in &server.services {
            if service.health != SvcHealth::Ok {
                lines.push(format!(
                    "Service {} on server '{}' is {}.",
                    service.service, server.server, service.health,
                ));
            }
        }
    }

    match (lines.is_empty(), summary.cloud.is_empty()) {
        (false, _) => lines.join("\n"),
        (true, true) => "All tasks are running.".to_string(), // No prod. cloud
        (true, false) => "All tasks are running. All services are Ok.".to_string(),
    }
}
// =============================================================
/// Watches for SIGINT, SIGTERM & SIGHUP signals and sends them to the daemon core
pub async fn sigx_watcher(tx: Sender<DaemonRequest>) -> Result<(), BoxError> {
    // We check for sigint, sighup & sigterm
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    let mut sighup = signal(SignalKind::hangup())?;

    let (reply_tx, reply_rx) = oneshot::channel();
    let mut request = DaemonRequest {
        reply: reply_tx,
        command: DaemonComm::Shutdown,
    };

    let _dreply = tokio::select! {
        _ = sigint.recv() => {
            info!(signal = "SIGINT");
            tx.send(request).await?;
            reply_rx.await??
        }
        _ = sigterm.recv() => {
            info!(signal = "SIGTERM");
            tx.send(request).await?;
            reply_rx.await??
        }
        _ = sighup.recv() => {
            info!(signal = "SIGHUP");
            request.command = DaemonComm::Reload;
            tx.send(request).await?;
            reply_rx.await??
        }
    };

    Ok(())
}
// =============================================================
