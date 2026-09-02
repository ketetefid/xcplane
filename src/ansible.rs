// SPDX-License-Identifier: GPL-3.0-or-later

use chrono::Utc;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::env::var_os;
use std::fs::{Permissions, set_permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Output, Stdio};
use std::sync::Arc;
use strum::IntoEnumIterator;
use strum_macros::{AsRefStr, EnumIter};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{mpsc::Sender, oneshot};
use tokio::time::{Duration, Instant, interval as intrvl};
use tokio::{fs, process::Command};
use tokio_rusqlite::Connection as SqlConn;
use tracing::{Instrument, Span, debug, error, info, instrument, warn};

use crate::cloud::reconcile::RebaseData;
use crate::cloudflare::CFState;
use crate::constants::{
    ANSIBLE_CFG, ANSIBLE_DIR, CF_AUTH, JSON_SUBLINKS_SUFFIX, MASTER_PLAYBOOK, OUTBLOCKED_UPDATE,
    PANEL_SETTINGS, SUB_PORT, SUBLINKS_SUFFIX, UI_PORT, UNBOUND_PORT,
};
use crate::daemon::{DaemonComm, DaemonReply, DaemonRequest};
use crate::db::{mark_server_offgrid, mark_server_production};
use crate::nft::generate_nftops;
use crate::types::{
    BoxError, CFAuth, Inbound, KetServer, Secrets, SvcKind, TaskEntry, WorkSpace, Xui, XuiToken,
};
use crate::xui::db::{delete_xui_backups, last_xui_db};

// All Ansible task filenames
use crate::constants::{
    ACME, ADD_INBOUND, BASE_SETUP, BASICS, BOOTSTRAP, CLOUDFLARE, CLOUDFLARE_DEL, DEL_INBOUND,
    DESTROY_SERVER, DNS_RESET, DOH_SETUP, FAIL2BAN, FIREWALL, FIREWALL_BOOTSTRAP, FULL_SETUP,
    NGINX_FIX1, NGINX_FIX2, NGINX_SETUP, PORTS_CHECK, SSH_SETUP, XRAY_FIX1, XRAY_FIX2, XUI_AUTH,
};

/// All the data need for running Ansible on a server
pub struct AnsibleRun {
    pub workspace: Arc<WorkSpace>,
    pub server: Arc<KetServer>,
    pub actions: Vec<AnsibleAction>,
    pub stream_it: bool,
    pub rebase_data: Option<RebaseData>,
}

/// An Ansible Run creates AnsibleOutput which holds the output and the paths to
/// The written log files
#[derive(Debug)]
pub struct AnsibleOutput {
    pub output: Output,
    pub actions: Vec<AnsibleAction>,
    pub stdout: PathBuf,
    pub stderr: PathBuf,
}

/// Holds the data that will be passed as extra variables to Ansible:
/// ansible_vars as a json string, and the path to the inventory file
pub struct AnsibleData {
    pub ansible_vars: String,
    pub inventory_path: PathBuf,
    pub stdout_path: PathBuf,
    pub stderr_path: PathBuf,
}

/// Constructs needed information for performing an Ansible action on the server
#[derive(Debug, Serialize)]
pub struct AnsibleVars {
    /// The list of actions from [`AnsibleAction`] to be performed on the server
    pub ansible_tasks: Vec<&'static str>, // vector of tasks to be performed
    pub full_setup: bool,                 // is this a full setup
    pub data_dir: PathBuf,                // workspace.dirs.data_dir
    pub node_name: String,                // ketserver.name
    pub base_hostname: String,            // ketserver.domain
    pub node_region: String,              // ketserver.region
    pub ssh_port: u16,                    // ketserver.ssh_port
    pub dns_subdomain: String,            // ketserver.dns_subdomain
    pub ui_subdomain: String,             // ketserver.ui_subdomain
    pub sub_subdomain: String,            // ketserver.sub_subdomain
    pub secrets: Secrets,                 // ketserver.secrets
    pub cloudflare_proxied: bool,         // ketserver.cloudflare_proxied
    pub cloudflare_state: Arc<CFState>,   // ketserver.cfstate
    pub unbound_https_port: u16,          // defaults to UNBOUND_PORT
    pub ui_webport: u16,                  // defaults to UI_PORT
    pub sub_port: u16,                    // defaults to SUB_PORT
    pub new_ports: Vec<u16>,              // prospective global & local ports
    pub nftops: HashMap<String, PathBuf>, // paths to all nft rule files
    pub inbound_data: Vec<Inbound>,       // ketserver.inbounds
    pub sublinks_suffix: &'static str,    // SUBLINKS_SUFFIX
    pub jsublinks_suffix: &'static str,   // JSON_SUBLINKS_SUFFIX
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rebase_data: Option<RebaseData>, // generated when reconciliation
    /// A stash of extra variables to be used in the calls. This field can
    /// contain arbitrary data, and while in the Rust program they are
    /// validated, we can't reliably implement a one-check-for-all in
    /// var_loading.yaml in Ansible for every task.
    pub extra: HashMap<String, serde_json::Value>,
}

/// The struct that holds information for building an Ansible inventory
#[derive(Debug, Clone, Serialize, Default)]
pub struct AnsibleConn {
    /// The Ansible inventory hostname. It is reconciled with the server's IP
    /// address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ansible_host: Option<String>,

    /// ansible_port is the port to which the program will establish an SSH
    /// connection. The value might be different than the server's ssh_port
    /// initially, but will eventually be reconciled with it (e.g. in a full
    /// setup or a Rebase).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ansible_port: Option<u16>,

    /// The ansible_user will always be selected as 'root'
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ansible_user: Option<&'static str>,

    /// The type of connection in case we include localhost in the inventory
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ansible_connection: Option<String>,
}

impl AnsibleVars {
    /// Replaces the secrets in AnsibleVars with dummy strings
    pub fn hide_secrets(&mut self) {
        let hidden_secret = "******".to_string();
        self.secrets = Secrets {
            health_path: hidden_secret.clone(),
            doh_endpoint: hidden_secret.clone(),
            xui_username: hidden_secret.clone(),
            xui_password: hidden_secret.clone(),
            xui_token: XuiToken::from(hidden_secret.clone()),
            xui_webpath: hidden_secret.clone(),
            xui_subpath: hidden_secret.clone(),
            xui_jsubpath: hidden_secret.clone(),
            mother_subids: HashMap::<String, String>::new(),
        }
    }
}

/// The enum of Ansible actions in order of priority, meaning if several of them
/// are invoked, the earlier ones take precedence over the later tasks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, EnumIter, AsRefStr)]
#[repr(usize)]
pub enum AnsibleAction {
    PortsCheck,         // id: 0
    DnsReset,           // id: 1
    Basics,             // id: 2
    CloudflareDel,      // id: 3
    Cloudflare,         // id: 4
    Acme,               // id: 5
    Nginx,              // id: 6
    DoH,                // id: 7
    FirewallBootstrap,  // id: 8
    OutblockedUpdate,   // id: 9
    BaseSetup,          // id: 10 prepares everything up to 3XUI installation
    FullSetup,          // id: 11 contains BaseSetup + every other needed action
    NginxRestart,       // id: 12
    NginxRestoreConfig, // id: 13
    XrayRestart,        // id: 14
    XrayRestoreDB,      // id: 15
    Bootstrap,          // id: 16
    XuiAuth,            // id: 17
    DelInbound,         // id: 18
    AddInbound,         // id: 19
    PanelSettings,      // id: 20
    Fail2ban,           // id: 21
    Firewall,           // id: 22
    /*
    SSH is critical and we commit such a destructive change at the end,
    so that in case of a failure we are not locked out for a re-run.
    */
    SSH,           // id: 23
    DestroyServer, // id: 24
}

/// A struct holding a vector of task filenames that correspond to a series of
/// AnsibleAction's, along with a generated ID which is the representative of
/// the actions it includes
pub struct AnsibleTasks {
    pub tasks: Vec<&'static str>,
    pub id: String,
}

/// Ansible inventory file structure
#[derive(Serialize)]
struct AnsibleInventory {
    all: InventoryGroup,
}

#[derive(Serialize)]
struct InventoryGroup {
    hosts: HashMap<String, Arc<AnsibleConn>>,
}

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

impl AnsibleAction {
    /// All Ansible actions in one vector
    pub fn all() -> Vec<Self> {
        Self::iter().collect()
    }
    // =============================================================
    /// Builds the vector of all Ansible task filenames used for validation
    pub fn all_taskfiles() -> Vec<&'static str> {
        Self::iter().map(|a| a.taskname()).collect()
    }
    // =============================================================
    /// Sorts and deduplicates the vector of AnsibleAction
    pub fn sort_unique(actions: &mut Vec<Self>) {
        actions.sort();
        actions.dedup();
    }
    // =============================================================
    /// Creates AnsibleTasks which holds a vector of Ansible task filenames and
    /// a deterministic ID
    pub fn task_gen(actions: &Vec<Self>) -> AnsibleTasks {
        let (tasks, id_parts): (Vec<&str>, Vec<String>) = actions
            .iter()
            .map(|a| (a.taskname(), (*a as usize).to_string()))
            .unzip();

        let id = id_parts.join("_");

        AnsibleTasks { tasks, id }
    }
    // =============================================================
    /// Validates the corresponding tasks for all AnsibleAction's
    pub fn validate(workspace: &WorkSpace) -> Result<(), BoxError> {
        let mut actions_hs: HashSet<_> = HashSet::new();

        for task_filename in AnsibleAction::all_taskfiles() {
            let path = workspace
                .dirs
                .data_dir
                .join(ANSIBLE_DIR)
                .join("tasks")
                .join(Path::new(task_filename));

            if !actions_hs.insert(task_filename) {
                return Err("The tasks filenames are not unique.".into());
            }
            if task_filename.is_empty() || !path.exists() {
                return Err(format!(
                    "The path for task '{}' is invalid or doesn't exist. Path: {}",
                    task_filename,
                    path.display()
                )
                .into());
            }

            if std::fs::File::open(&path).is_err() {
                return Err(
                    format!("The path for task '{}' is not readable.", task_filename).into(),
                );
            }
        }

        Ok(())
    }
    // =============================================================
    /// Maps the action to its task filename
    pub fn taskname(&self) -> &'static str {
        match self {
            AnsibleAction::PortsCheck => PORTS_CHECK,
            AnsibleAction::DnsReset => DNS_RESET,
            AnsibleAction::Basics => BASICS,
            AnsibleAction::CloudflareDel => CLOUDFLARE_DEL,
            AnsibleAction::Cloudflare => CLOUDFLARE,
            AnsibleAction::Acme => ACME,
            AnsibleAction::SSH => SSH_SETUP,
            AnsibleAction::Nginx => NGINX_SETUP,
            AnsibleAction::DoH => DOH_SETUP,
            AnsibleAction::Bootstrap => BOOTSTRAP,
            AnsibleAction::Firewall => FIREWALL,
            AnsibleAction::FirewallBootstrap => FIREWALL_BOOTSTRAP,
            AnsibleAction::OutblockedUpdate => OUTBLOCKED_UPDATE,
            AnsibleAction::Fail2ban => FAIL2BAN,
            AnsibleAction::BaseSetup => BASE_SETUP,
            AnsibleAction::FullSetup => FULL_SETUP,
            AnsibleAction::XuiAuth => XUI_AUTH,
            AnsibleAction::AddInbound => ADD_INBOUND,
            AnsibleAction::DelInbound => DEL_INBOUND,
            AnsibleAction::PanelSettings => PANEL_SETTINGS,
            AnsibleAction::NginxRestart => NGINX_FIX1,
            AnsibleAction::NginxRestoreConfig => NGINX_FIX2,
            AnsibleAction::XrayRestart => XRAY_FIX1,
            AnsibleAction::XrayRestoreDB => XRAY_FIX2,
            AnsibleAction::DestroyServer => DESTROY_SERVER,
        }
    }
}

impl AnsibleRun {
    /// Performs the action(s) it includes on the server
    #[instrument(name = "ansible", fields(server = self.server.name), skip_all)]
    pub async fn run(mut self) -> Result<AnsibleOutput, BoxError> {
        // Prepare general variables for Ansible
        let ansible_data = self.write_ansible_vars().await?;

        let playbook_path = self
            .workspace
            .dirs
            .data_dir
            .join(ANSIBLE_DIR)
            .join(MASTER_PLAYBOOK);

        let cf_auth_path = self.workspace.dirs.config_dir.join(CF_AUTH);
        let cf_auth_str = fs::read_to_string(&cf_auth_path).await?;
        let cf_auth = toml::from_str::<CFAuth>(&cf_auth_str)?;

        let ansible_cfg_path = self.workspace.dirs.config_dir.join(ANSIBLE_CFG);

        // Can we really connect?
        Self::test_ssh(&ansible_data.inventory_path, &ansible_cfg_path).await?;

        // Using tokio::process::Command
        // As well as the field which selectively enables streaming,
        // XCPLANE_STREAM will enable streaming for all Ansible runs
        let output = if self.stream_it || var_os("XCPLANE_STREAM").is_some() {
            let mut child = Command::new("ansible-playbook")
                .arg(&playbook_path)
                .arg("-i")
                .arg(&ansible_data.inventory_path)
                .arg("-e")
                .arg(&ansible_data.ansible_vars)
                .env("ANSIBLE_CONFIG", &ansible_cfg_path)
                .env("CF_API_TOKEN", &cf_auth.cloudflare_api_token)
                // Don't buffer stdout in Ansible
                .env("PYTHONUNBUFFERED", "1")
                // Stream to program so we can watch it live
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                // .output() will wait and buffer
                .spawn()
                .map_err(|e| Box::new(e))?;

            let stdout = child
                .stdout
                .take()
                .ok_or("Couldn't take stdout in ansible_call")?;
            let stderr = child
                .stderr
                .take()
                .ok_or("Couldn't take stderr in ansible_call")?;

            let mut out_reader = BufReader::new(stdout).lines();
            let mut err_reader = BufReader::new(stderr).lines();

            let current_span = Span::current();
            // Now we read both stdout & stderr concurrently
            let stdout_task = tokio::spawn(
                async move {
                    let mut buf = Vec::new();

                    while let Ok(Some(line)) = out_reader.next_line().await {
                        info!(message = line);
                        buf.extend_from_slice(line.as_bytes());
                        buf.push(b'\n');
                    }

                    buf
                }
                .instrument(current_span.clone()),
            );

            let stderr_task = tokio::spawn(
                async move {
                    let mut buf = Vec::new();

                    while let Ok(Some(line)) = err_reader.next_line().await {
                        info!(message = line);
                        buf.extend_from_slice(line.as_bytes());
                        buf.push(b'\n');
                    }

                    buf
                }
                .instrument(current_span),
            );

            // Reassembling Output
            let status = child.wait().await?;

            let collected_stdout = stdout_task.await?;
            let collected_stderr = stderr_task.await?;

            let output = Output {
                status,
                stdout: collected_stdout,
                stderr: collected_stderr,
            };

            output
        } else {
            let output = Command::new("ansible-playbook")
                .arg(&playbook_path)
                .arg("-i")
                .arg(&ansible_data.inventory_path)
                .arg("-e")
                .arg(&ansible_data.ansible_vars)
                .env("ANSIBLE_CONFIG", &ansible_cfg_path)
                .env("CF_API_TOKEN", &cf_auth.cloudflare_api_token)
                .kill_on_drop(true)
                .output()
                .await
                .map_err(|e| Box::new(e))?;

            output
        };

        // Writing the output to log files
        fs::write(&ansible_data.stdout_path, &output.stdout).await?;
        fs::write(&ansible_data.stderr_path, &output.stderr).await?;

        let ansible_output = AnsibleOutput {
            output,
            actions: self.actions,
            stdout: ansible_data.stdout_path,
            stderr: ansible_data.stderr_path,
        };

        Ok(ansible_output)
    }
    // =============================================================
    /// Prepares needed variables for an Ansible run
    pub async fn write_ansible_vars(&mut self) -> Result<AnsibleData, BoxError> {
        let server = &self.server;
        let workspace = &self.workspace;

        // If it doesn't exist, create the server directory where the produced
        // Ansible files will reside
        let server_dir = workspace.dirs.data_dir.join(&server.name);
        fs::create_dir_all(&server_dir).await?;
        // Further security in depth
        set_permissions(&server_dir, Permissions::from_mode(0o700))?;

        let ansible_inbound_data = server
            .inbounds
            .iter()
            .filter(|inb| inb.name != SvcKind::super_name())
            .cloned()
            .collect::<Vec<_>>();

        // The extra variable, for now, is only used when we want to restore x-ui DB
        // as the second fix action of Xray. It stores the path to the DB.
        let extra = if self.actions.contains(&AnsibleAction::XrayRestoreDB) {
            let mut hm = HashMap::<String, serde_json::Value>::new();
            let latest_backup_path = last_xui_db(&workspace, &server).await??;
            hm.insert(
                "latest_backup".into(),
                latest_backup_path.to_string_lossy().to_string().into(),
            );

            hm
        } else {
            HashMap::<String, serde_json::Value>::new()
        };

        let cf_state = server
            .cfstate
            .load_full()
            .ok_or("Couldn't unwrap Cloudflare state data in write_ansible_vars.")?;

        // Disable Cloudflare proxy if the domain is a too deep subdomain of its zone
        let cloudflare_proxied = if cf_state.too_deep && server.cloudflare_proxied {
            warn!("deploying on a deeply nested domain--Cloudflare proxy is being disabled");
            false
        } else {
            server.cloudflare_proxied
        };

        // Sort & dedup the actions
        AnsibleAction::sort_unique(&mut self.actions);

        // Is this a full setup?
        let is_full_setup = if self.actions.contains(&AnsibleAction::FullSetup) {
            true
        } else {
            false
        };

        // While the ports managed by xcplane are already checked for any clash
        // with each other, they must be checked on the server itself too, as
        // there might be local services running on those ports.
        let this_ansible_conn = server.ansible_conn.load_full().ok_or::<BoxError>(
            format!(
                "Couldn't unwrap AnsibleConn of server {} for AnsibleVars.",
                server.name
            )
            .into(),
        )?;

        let mut new_ports = vec![];
        // ssh_port is checked when it is a different port than
        // ansible_port. Also, NGINX_PORT is a dedicated port and is always
        // excluded.
        if this_ansible_conn.ansible_port != Some(server.ssh_port) {
            new_ports.push(server.ssh_port);
        }

        if is_full_setup {
            // In a full setup, [existing] inbound ports don't need to be
            // included since the panel is uninstalled first anyway
            new_ports.extend([UNBOUND_PORT, UI_PORT, SUB_PORT]);
        } else {
            // Checking is needed if this is a Rebase. The other remote
            // operation types don't need it.
            if let Some(data) = &self.rebase_data {
                // The added inbounds' ports must be included provided that they
                // are not deleted first.
                // UNBOUND_PORT, UI_PORT and SUB_PORT have already been checked
                // during full setup.
                let mut added_inbound_ports = data
                    .added_inbounds
                    .iter()
                    .map(|inb| inb.port)
                    .collect::<Vec<_>>();
                let removed_inbound_ports = data
                    .removed_inbounds
                    .iter()
                    .map(|inb| inb.port)
                    .collect::<Vec<_>>();

                added_inbound_ports.retain(|p| !removed_inbound_ports.contains(p));

                new_ports.append(&mut added_inbound_ports);
            }
        }

        // Computing which nft rule operations should be included
        let nftops = generate_nftops(&self.actions, workspace.clone(), server.clone()).await?;

        // Creating the vector of needed Ansible task filenames, and an ID for the
        // inventory filename
        let tasks_and_id = AnsibleAction::task_gen(&self.actions);

        let mut ansible_vars = AnsibleVars {
            ansible_tasks: tasks_and_id.tasks,
            full_setup: is_full_setup,
            data_dir: workspace.dirs.data_dir.clone(),
            node_name: server.name.clone(),
            base_hostname: server.domain.to_string(),
            node_region: server.region.alpha2().to_string(),
            ssh_port: server.ssh_port,
            dns_subdomain: server.dns_subdomain.clone(),
            ui_subdomain: server.ui_subdomain.clone(),
            sub_subdomain: server.sub_subdomain.clone(),
            secrets: server.secrets.clone(),
            cloudflare_proxied,
            cloudflare_state: cf_state,
            ui_webport: UI_PORT,
            sub_port: SUB_PORT,
            unbound_https_port: UNBOUND_PORT,
            new_ports,
            nftops,
            inbound_data: ansible_inbound_data,
            sublinks_suffix: SUBLINKS_SUFFIX,
            jsublinks_suffix: JSON_SUBLINKS_SUFFIX,
            rebase_data: self.rebase_data.clone(),
            extra,
        };

        // Creating the inventory file
        let mut ansible_conns = HashMap::new();
        ansible_conns.insert(server.name.clone(), this_ansible_conn);
        // Including localhost as well
        ansible_conns.insert(
            "localhost".to_string(),
            Arc::new(AnsibleConn {
                ansible_host: None,
                ansible_port: None,
                ansible_user: None,
                ansible_connection: Some("local".to_string()),
            }),
        );

        let inventory = AnsibleInventory {
            all: InventoryGroup {
                hosts: ansible_conns,
            },
        };

        let inventory_filename = format!("{}-{}-inventory.json", &server.name, &tasks_and_id.id);
        let inventory_path = server_dir.join(&inventory_filename);

        let inventory_json = serde_json::to_string_pretty(&inventory)?;

        fs::write(&inventory_path, &inventory_json).await?;

        // Prepare the needed variables to be passed with '-e'. Ansible takes
        // --extra-vars from a json struct
        let ansible_vars_json = serde_json::to_string(&ansible_vars)?;

        // Writing the vars to a yaml file in the server directory for reference
        // as well
        ansible_vars.hide_secrets();
        let ansible_vars_yaml = serde_yaml::to_string(&ansible_vars)?;
        let ansible_vars_filename =
            server.name.clone() + "-" + &tasks_and_id.id + "-general_vars.yaml";
        let ansible_vars_path = workspace
            .dirs
            .data_dir
            .join(&server.name)
            .join(&ansible_vars_filename);

        fs::write(&ansible_vars_path, &ansible_vars_yaml).await?;

        // We prepare paths for logging output later
        let stdout_name = format!(
            "{}-{}-{}-ansible-stdout.log",
            server.name,
            &tasks_and_id.id,
            &Utc::now().format("%Y_%m_%d_%H_%M_%S")
        );

        let stderr_name = format!(
            "{}-{}-{}-ansible-stderr.log",
            server.name,
            &tasks_and_id.id,
            &Utc::now().format("%Y_%m_%d_%H_%M_%S")
        );

        let stdout_path = workspace.dirs.state_dir.join(stdout_name);
        let stderr_path = workspace.dirs.state_dir.join(stderr_name);

        Ok(AnsibleData {
            ansible_vars: ansible_vars_json,
            inventory_path,
            stdout_path,
            stderr_path,
        })
    }
    // =============================================================
    /// Checks full SSH connectivity using Ansible ping/pong
    async fn test_ssh(inventory_file: &Path, ansible_cfg_path: &Path) -> Result<(), BoxError> {
        let output = Command::new("ansible")
            .arg("all")
            .arg("-i")
            .arg(inventory_file)
            .arg("-m")
            .arg("ping")
            .env("ANSIBLE_CONFIG", ansible_cfg_path)
            .kill_on_drop(true)
            .output()
            .await
            .map_err(|e| Box::new(e))?;

        if output.status.success() {
            return Ok(());
        } else {
            let stdout = String::from_utf8_lossy(&output.stdout);
            // stderr is empty on an Error from ping check
            // let stderr = String::from_utf8_lossy(&output.stderr);
            debug!(error = %stdout, "ping result");

            if stdout.contains("Permission denied") {
                return Err("problem with the SSH key--either it is not properly \
			installed on the server, or password is required"
                    .into());
            } else if stdout.contains("Connection timed out") || stdout.contains("No route to host")
            {
                return Err(
                    "network/firewall issue or port mismatch--the remote server cannot be reached"
                        .into(),
                );
            } else if stdout.contains("Host key verification failed") {
                // This if arm should never happen, because we have disabled key
                // verification
                return Err("host key verification failed".into());
            } else {
                // Let the user see the full ping output
                return Err(stdout.into());
            }
        }
    }
}
// =============================================================
/// Fully provisions a server for production using Ansible
pub async fn full_setup(
    conn: Arc<SqlConn>,
    txd: Sender<DaemonRequest>,
    server: Arc<KetServer>,
    workspace: Arc<WorkSpace>,
) -> Result<(), BoxError> {
    warn!(
        "\n######################################################################\n\
        # THIS ACTION WILL ERASE DATA ON THE SERVER. If this is not desired, #\n\
        # you have 10 seconds to abort it by pressing Ctrl + C or sending a  #\n\
        # shutdown/restart/reload to the daemon.                             #\n\
	#                                                                    #\n\
        # The controller's SSH public key must be installed on the server    #\n\
        # and SSH must be listening on port 22.                              #\n\
        ######################################################################",
    );
    let mut ticker = intrvl(Duration::from_secs(1));
    for i in 0..11 {
        ticker.tick().await;
        warn!("{i}");
    }

    let start_time = Instant::now();

    // We intentionally enable streaming, because full setup is a unique, long
    // process
    let ansible_run = AnsibleRun {
        workspace: workspace.clone(),
        server: server.clone(),
        actions: vec![AnsibleAction::FullSetup],
        stream_it: true, // Full setup is a long action
        rebase_data: None,
    };
    // If the user aborts the program now, in the next run it may do setup
    // on another server. A warning should be issued.
    warn!("proceeding");

    match ansible_run.run().await {
        Ok(res) => {
            if res.output.status.success() {
                // Reading xui_token from Ansible-produced files
                let cred_file = workspace
                    .dirs
                    .data_dir
                    .join(&server.name)
                    .join("xui_credentials.yaml");
                let cred_str = fs::read_to_string(&cred_file).await?;
                let cred: Xui = serde_yaml::from_str(&cred_str)?;

                mark_server_production(workspace.clone(), conn, server.clone(), cred.xui_token)
                    .await?;

                // We delete older backups in case this is a reinstall
                delete_xui_backups(&workspace, &server).await??;

                let elapsed_time = start_time.elapsed();

                // Delete the full setup entry from the taskmap, and Reload if
                // there is not any other running full setup or destruction task
                let (inquiry_tx, inquiry_rx) = oneshot::channel();
                let request = DaemonRequest {
                    reply: inquiry_tx,
                    command: DaemonComm::SetupInquiry(TaskEntry::FullSetup(server)),
                };

                txd.send(request).await?;

                if let DaemonReply::SetupInquiry(found) = inquiry_rx.await?? {
                    if found {
                        // Daemon Reload will be left to the last running full setup
                        // or destruction task.
                        warn!(
                            spent_time = elapsed_time.as_secs(),
                            reason = "another full setup or destruction task is running",
                            "completed--reload is delayed"
                        );
                    } else {
                        warn!(spent_time = elapsed_time.as_secs(), "completed--reloading");
                        // Now we send a manual Reload command to the daemon, and
                        // the updated cloud will be carried to the next daemon
                        // cycle.
                        let (reload_tx, reload_rx) = oneshot::channel();
                        let request = DaemonRequest {
                            reply: reload_tx,
                            command: DaemonComm::Reload,
                        };

                        txd.send(request).await?;
                        let _dreply = reload_rx.await??;
                    }
                }
            } else {
                error!(
                    stdout = %res.stdout.display(),
                    stderr = %res.stderr.display(),
                    "failed"
                );
            }
        }

        Err(e) => {
            error!(error = e, "failed")
        }
    }

    Ok(())
}
// =============================================================
/// Destroys a Production server and turns it into Offgrid. Note that this is
/// not part of xcplane workflow, as changes to the declarative cloud
/// configuration are already handled through xcplane reconciliation modes.
pub async fn destroy_server(
    conn: Arc<SqlConn>,
    server: Arc<KetServer>,
    workspace: Arc<WorkSpace>,
    txd: Sender<DaemonRequest>,
) -> Result<(), BoxError> {
    warn!(
        "\n#########################################################################\n\
        # THE SERVER IS ABOUT TO LOSE ITS IDENTITY AND ALL ITS DATA. If this is #\n\
        # not desired, you have 10 seconds to abort it by pressing Ctrl + C or  #\n\
        # sending a shutdown/restart/reload to the daemon.                      #\n\
        #########################################################################",
    );
    let mut ticker = intrvl(Duration::from_secs(1));
    for i in 0..11 {
        ticker.tick().await;
        warn!("{i}");
    }

    let ansible_run = AnsibleRun {
        workspace: workspace.clone(),
        server: server.clone(),
        actions: vec![AnsibleAction::DestroyServer],
        stream_it: true,
        rebase_data: None,
    };

    warn!("proceeding");

    match ansible_run.run().await {
        Ok(res) => {
            if res.output.status.success() {
                // Change the state in the DB and cloud config to Offgrid
                mark_server_offgrid(workspace, conn, server.clone()).await?;

                // Delete the entry from the taskmap, and Reload if there is not
                // any other unfinished full setup or destruction operation
                let (inquiry_tx, inquiry_rx) = oneshot::channel();
                let request = DaemonRequest {
                    reply: inquiry_tx,
                    command: DaemonComm::SetupInquiry(TaskEntry::DestroyServer(server)),
                };

                txd.send(request).await?;

                if let DaemonReply::SetupInquiry(found) = inquiry_rx.await?? {
                    if found {
                        // The last running full setup or destruction operation
                        // will do a reload with no deadlock, as the inquirer's
                        // task entry is removed from the taskmap upon the inquiry.
                        warn!(
                            reason = "another full setup or destruction task is running",
                            "completed--reload is delayed"
                        );
                    } else {
                        warn!("completed--reloading");

                        let (reload_tx, reload_rx) = oneshot::channel();
                        let request = DaemonRequest {
                            reply: reload_tx,
                            command: DaemonComm::Reload,
                        };

                        txd.send(request).await?;
                        let _dreply = reload_rx.await??;
                    }
                }
            } else {
                error!(stdout = %res.stdout.display(), stderr = %res.stderr.display(), "failed");
            }
        }

        Err(e) => {
            error!(error = e, "failed")
        }
    }

    Ok(())
}
// =============================================================
