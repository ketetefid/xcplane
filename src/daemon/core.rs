// SPDX-License-Identifier: GPL-3.0-or-later

use std::sync::{Arc, atomic::Ordering};
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    oneshot,
};
use tokio::time::{Duration, interval as intrvl};
use tracing::{debug, error, info_span, warn};

use super::{
    DaemonComm, DaemonLock, DaemonNext, DaemonPrereq, DaemonReply, DaemonRequest, DaemonRuntime,
    ReconMode, SvcComm, build_servers_creds, check_and_backup, get_cloud_clients, get_summary,
    log_command, perform_server_destruction, sigx_watcher,
    startup::{acquire_lock, check_prereq, prepare_runtime, prepare_workspace, validate_workspace},
};

use crate::cloud::reconcile::reconcile_cloud;
use crate::cloud::service::{
    SvcContext, run_acme_checker, run_aggregator, run_geoip_updater, run_svc_monitor,
    run_xuidb_pruner,
};
use crate::cloud::{expand_cloud, parse_cloud_config, spawn_task};
use crate::constants::{
    EXIT_ERROR, EXIT_OK, EXIT_REBASE, EXIT_RELOAD, EXIT_REMAP, EXIT_RESTART, NGINX_PORT,
};
use crate::db::{create_db, load_cloud, update_db};
use crate::log::init_log;
use crate::types::{
    BoxError, Daemon, DashAction, KetServer, ServerState, SvcInfo, SvcKind, TaskEntry, TaskHandle,
    WorkSpace,
};

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

/// Runs the daemon core and listens to its exit codes
pub async fn daemonize(workspace: Arc<WorkSpace>) -> Result<(), BoxError> {
    // Starting logging
    let _log_guard = init_log(&workspace.dirs.state_dir)?;

    let mut daemon_next = Daemon::init_state();
    validate_workspace(&workspace)?;

    let DaemonLock {
        channel: (txd, mut rxd),
        socket_jh,
    } = acquire_lock(&workspace).await?;

    let mut prereq = check_prereq(&workspace).await?;
    let mut the_workspace = workspace;

    loop {
        daemon_next = daemon_core(
            the_workspace.clone(),
            daemon_next,
            prereq.clone(),
            txd.clone(),
            &mut rxd,
        )
        .await?;

        match daemon_next.code {
            EXIT_OK => break,

            EXIT_ERROR => return Err("Daemon exited with error.".into()),

            EXIT_RESTART => {
                // in Restart we re-prepare the workspace and recheck
                // prerequisites
                the_workspace = prepare_workspace()?;
                validate_workspace(&the_workspace)?;
                prereq = check_prereq(&the_workspace).await?;
                continue;
            }

            EXIT_RELOAD | EXIT_REMAP | EXIT_REBASE => continue,

            _ => return Err("Unknown exit code".into()),
        }
    }

    let _shutdown_enter = info_span!("daemon").entered();

    socket_jh.abort();
    the_workspace.daemon.cleanup()?;

    Ok(())
}
// =============================================================
/// The core of daemon which starts monitoring every service, and manages signals
async fn daemon_core(
    workspace: Arc<WorkSpace>,
    daemon_init: DaemonNext,
    prereq: DaemonPrereq,
    txd: Sender<DaemonRequest>,
    rxd: &mut Receiver<DaemonRequest>,
) -> Result<DaemonNext, BoxError> {
    // 'destruct'ing for easier access
    let DaemonPrereq {
        sqlconn,
        mut cf_list,
        cf_client,
    } = prereq;

    // Prepare and destruct runtime data
    let runtime_data = prepare_runtime().await?;

    let DaemonRuntime {
        mut taskmap,
        mut svc_chat,
        action_map,
        atomic_port,
        aggregator_ch: (txa, rxa),
    } = runtime_data;

    // Parse the config file and get the new declarative cloud, if this is a
    // bare startup. The reason we read the config first here, is to avert
    // running dangling tasks if the declarative cloud cannot be computed.
    let new_cloud = match daemon_init.new_cloud {
        Some(cloud) => cloud,
        None => parse_cloud_config(&workspace).await?,
    };

    let recon_mode = &daemon_init.mode;

    /////////////////////// Signal watcher ///////////////////////

    let watcher_task = TaskEntry::SignalWatcher;
    if !taskmap.tasks.contains_key(&watcher_task) {
        let jh = spawn_task(sigx_watcher(txd.clone()), watcher_task.clone());
        taskmap
            .tasks
            .entry(watcher_task)
            .or_insert(TaskHandle::Detached(jh));
    }

    /////////////////////// GeoIP DB updater ///////////////////////

    let geoip_task = TaskEntry::GeoipUpdater;

    if !taskmap.tasks.contains_key(&geoip_task) {
        let jh = spawn_task(run_geoip_updater(workspace.clone()), geoip_task.clone());

        taskmap
            .tasks
            .entry(geoip_task)
            .or_insert(TaskHandle::Detached(jh));
    }

    // Set up the schema for the DB if it hasn't already been created
    create_db(sqlconn.clone()).await?;

    // Load the existing cloud from DaemonNext or from DB if missing
    let old_cloud = match daemon_init.old_cloud {
        None => load_cloud(sqlconn.clone()).await?,
        Some(cloud) => cloud,
    };

    // Reconcile the existing cloud with the new cloud according to the rules
    // described in reconcile_cloud
    let the_cloud = match reconcile_cloud(
        &old_cloud,
        &new_cloud,
        recon_mode,
        workspace.clone(),
        &cf_client,
        &mut cf_list,
        daemon_init.rebase_data,
    )
    .await
    {
        Ok(recon_cloud) => {
            // Write the reconciled cloud back to DB
            update_db(workspace.clone(), sqlconn.clone(), recon_cloud.clone()).await?;

            recon_cloud
        }
        Err(e) => {
            let _daemon_enter = info_span!("daemon").entered();
            error!(error = e, "applying the new cloud state failed");

            old_cloud
        }
    };

    /*
    info!(
    "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n{:#?}\n\
    @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@",
    the_cloud
    );
     */

    // Collect the production servers which are enabled
    let prod_servers: Vec<Arc<KetServer>> = the_cloud
        .servers
        .iter()
        .filter(|s| s.state.load() == ServerState::Production && s.enabled)
        .cloned() // convert &Arc to Arc for lifetime considerations
        .collect();

    // If we have at least one production server, we start monitoring
    // concurrently
    if !prod_servers.is_empty() {
        // The aggregator task which writes service status data to DB. We accept
        // that a monitoring task should never crash; therefore, we intentionally
        // don't reload a crashed service, and the underlying error must be fixed
        // instead.
        let aggregator_task = TaskEntry::Aggregator;
        let sql_conn = sqlconn.clone();

        if !taskmap.tasks.contains_key(&aggregator_task) {
            let jh = spawn_task(
                run_aggregator(sql_conn, the_cloud.settings.monitor_interval, rxa),
                aggregator_task.clone(),
            );

            taskmap
                .tasks
                .entry(aggregator_task)
                .or_insert(TaskHandle::Detached(jh));
        }

        // Get the total number of services the cloud has
        let ndata = prod_servers.iter().map(|s| s.svc_num()).sum();
        debug!(service_num = ndata, "total number of cloud services");

        //////////////////////////////////////////////////////////////////////////

        for server in prod_servers {
            // Prepare a client for xui API calls
            server.new_xui_client().await?;

            // Setting the monitoring interval for all services
            let duration = Duration::from_secs(the_cloud.settings.monitor_interval);

            /////////////////////// SSH monitoring task ///////////////////////

            let ssh_service_info = Arc::new(SvcInfo {
                server: server.clone(),
                port: server.ssh_port,
                kind: SvcKind::Ssh,
                link: None,
            });

            let task_entry = TaskEntry::SshMonitor(ssh_service_info.clone());

            action_map
                .entry(task_entry.clone())
                .or_insert(DashAction::default());

            // Only start the SSH task for this server if it is not being
            // monitored already
            if !taskmap.tasks.contains_key(&task_entry) {
                // Storing the sender side of channel between daemon <-> service task
                let (tx_svc, rx_svc) = mpsc::channel::<SvcComm>(5);
                svc_chat.entry(task_entry.clone()).or_insert(tx_svc);

                let ssh_context = SvcContext {
                    workspace: workspace.clone(),
                    sqlconn: sqlconn.clone(),
                    interval: intrvl(duration),
                    auto_fix: the_cloud.settings.auto_fix,
                    fix_threshold: the_cloud.settings.fix_threshold,
                    svc_info: ssh_service_info,
                    socks5_address: None,
                    tx_aggr: txa.clone(),
                    rx_svc,
                    ndata,
                    task_entry: task_entry.clone(),
                    action_map: action_map.clone(),
                };

                // The async spawn for SSH
                let jh = spawn_task(run_svc_monitor(ssh_context), task_entry.clone());
                // Insert the new server's monitoring joinhandle in the tasks
                // hashmap
                taskmap
                    .tasks
                    .entry(task_entry)
                    .or_insert(TaskHandle::Detached(jh));
            }

            /////////////////////// Nginx monitoring task ///////////////////////

            let nginx_service_info = Arc::new(SvcInfo {
                server: server.clone(),
                port: NGINX_PORT,
                kind: SvcKind::Nginx,
                link: None,
            });

            let task_entry = TaskEntry::NginxMonitor(nginx_service_info.clone());

            action_map
                .entry(task_entry.clone())
                .or_insert(DashAction::default());

            if !taskmap.tasks.contains_key(&task_entry) {
                let (tx_svc, rx_svc) = mpsc::channel::<SvcComm>(5);
                svc_chat.entry(task_entry.clone()).or_insert(tx_svc);

                let nginx_context = SvcContext {
                    workspace: workspace.clone(),
                    sqlconn: sqlconn.clone(),
                    interval: intrvl(duration),
                    auto_fix: the_cloud.settings.auto_fix,
                    fix_threshold: the_cloud.settings.fix_threshold,
                    svc_info: nginx_service_info,
                    socks5_address: None,
                    tx_aggr: txa.clone(),
                    rx_svc,
                    ndata,
                    task_entry: task_entry.clone(),
                    action_map: action_map.clone(),
                };

                // The async spawn for Nginx
                let jh = spawn_task(run_svc_monitor(nginx_context), task_entry.clone());

                taskmap
                    .tasks
                    .entry(task_entry)
                    .or_insert(TaskHandle::Detached(jh));
            }

            /////////////////////// Xray monitoring tasks ///////////////////////

            // For all the mother inbounds of each production server
            for inbound in &server.inbounds {
                let inbound_name = inbound.name.clone();

                let a_port = if &inbound_name == &SvcKind::super_name() {
                    // The super Xray is meta and doesn't need a port
                    0
                } else {
                    let portx = atomic_port.clone();
                    // Get the current shared counter; add 1 to it and
                    // create a new unique port for the next caller.
                    portx.fetch_add(1, Ordering::Relaxed)
                };

                let xray_service_info = Arc::new(SvcInfo {
                    server: server.clone(),
                    port: a_port,
                    kind: SvcKind::Xray(inbound_name),
                    link: Some(inbound.get_jsub(&server)?),
                });

                let task_entry = TaskEntry::XrayMonitor(xray_service_info.clone());

                action_map
                    .entry(task_entry.clone())
                    .or_insert(DashAction::default());

                if !taskmap.tasks.contains_key(&task_entry) {
                    let (tx_svc, rx_svc) = mpsc::channel::<SvcComm>(5);
                    svc_chat.entry(task_entry.clone()).or_insert(tx_svc);

                    let xray_context = SvcContext {
                        workspace: workspace.clone(),
                        sqlconn: sqlconn.clone(),
                        interval: intrvl(duration),
                        auto_fix: the_cloud.settings.auto_fix,
                        fix_threshold: the_cloud.settings.fix_threshold,
                        svc_info: xray_service_info,
                        socks5_address: None,
                        tx_aggr: txa.clone(),
                        rx_svc,
                        ndata,
                        task_entry: task_entry.clone(),
                        action_map: action_map.clone(),
                    };

                    // The async spawn for this Xray inbound
                    let jh = spawn_task(run_svc_monitor(xray_context), task_entry.clone());

                    taskmap
                        .tasks
                        .entry(task_entry)
                        .or_insert(TaskHandle::Detached(jh));
                }
            }

            /////////////////////// x-ui DB backup pruner ///////////////////////

            let task_entry = TaskEntry::XuiDbPruner(server.clone());

            if !taskmap.tasks.contains_key(&task_entry) {
                let jh = spawn_task(
                    run_xuidb_pruner(workspace.clone(), server.clone()),
                    task_entry.clone(),
                );

                taskmap
                    .tasks
                    .entry(task_entry)
                    .or_insert(TaskHandle::Detached(jh));
            }

            /////////////////////// Acme certificate checker ///////////////////////

            let task_entry = TaskEntry::AcmeChecker(server.clone());

            if !taskmap.tasks.contains_key(&task_entry) {
                let jh = spawn_task(
                    run_acme_checker(workspace.clone(), server.clone()),
                    task_entry.clone(),
                );

                taskmap
                    .tasks
                    .entry(task_entry)
                    .or_insert(TaskHandle::Detached(jh));
            }
        }
    } else {
        let _daemon_enter = info_span!("daemon").entered();
        warn!(
            "the cloud has no production servers--\
	     to provision a server and add it to the cloud, \
	     issue 'xcplane expand' in another terminal"
        );
    }

    /*
    The daemon command processor

    1. The daemon is listening for DaemonRequest which includes a variant of
       DaemonComm (shutdown, status, etc.)
    2. The socket listener task is listening for the enum of CLI commands: CliComm
    3. The socket listener receives a command; forwards the corresponding
       DaemonComm here, and waits for Result<DaemonReply>
    4. The socket listener tasks receives the Result<DaemonReply> and converts it to
       SocketMessage which is later processed by process_socket_reply
     */
    loop {
        #[rustfmt::skip]
        tokio::select! {
            Some(cmd) = rxd.recv() => {
		let cmd_str = cmd.command.to_string();
                match cmd.command {
                    DaemonComm::Shutdown => {
			log_command(&cmd_str);
			let _ = cmd.reply.send(Ok(DaemonReply::Message("Shutting down".to_string())));
			taskmap.abort_all();

			let shutdown_exit = DaemonNext {
			    code: EXIT_OK,
			    mode: ReconMode::Reload,
			    old_cloud: None,
			    new_cloud: None,
			    rebase_data: None,
			};
                        return Ok(shutdown_exit);
                    },

                    DaemonComm::Status(status_opts) => {
			log_command(&cmd_str);
			let mut svc_summary = Vec::new();

			for (_name, tx) in svc_chat.iter() {
			    let (svc_reply_tx, svc_reply_rx) = oneshot::channel();
			    tx.send(SvcComm::GetSummary(svc_reply_tx)).await?;
			    svc_summary.push(svc_reply_rx.await?);
			}

			let tasks_summary = taskmap.status();
			let cloud_summary = get_summary(svc_summary, tasks_summary, status_opts);
			let _ = cmd.reply.send(Ok(cloud_summary));
                    },

                    DaemonComm::ResetFix => {
			log_command(&cmd_str);
			for (_name, tx) in svc_chat.iter() {
			    tx.send(SvcComm::ResetFix).await?;
			}
			let _ = cmd.reply.send(Ok(DaemonReply::Message("Reset done.".to_string())));
                    },

                    DaemonComm::Expand(expand_opts) => {
			log_command(&cmd_str);

			let expansion_result = expand_cloud(
			    &the_cloud,
			    workspace.clone(),
			    expand_opts,
			    &mut taskmap,
			    sqlconn.clone(),
			    txd.clone(),
			).await;

			let _ = cmd.reply.send(expansion_result);
                    },

		    DaemonComm::Credentials(show_secrets) => {
			log_command(&cmd_str);
			let creds_result = build_servers_creds(&the_cloud, show_secrets.show_all);

			let _ = cmd.reply.send(creds_result);
		    },

		    DaemonComm::Clients => {
			log_command(&cmd_str);
			let clients_result = get_cloud_clients(&the_cloud).await;

			let _ = cmd.reply.send(clients_result);
		    }

		    DaemonComm::Destroy(destroy_opts) => {
			log_command(&cmd_str);
			let destroy_result = perform_server_destruction(
			    &the_cloud,
			    sqlconn.clone(),
			    &destroy_opts.server,
			    &mut taskmap,
			    workspace.clone(),
			    txd.clone()
			).await;

			let _ = cmd.reply.send(destroy_result);
		    }

		    DaemonComm::SetupInquiry(entry) => {
			log_command(&cmd_str);
			// The inquiry is always made when full setup or
			// destruction has been completed successfully.
			// Therefore, it is removed from taskmap.
			taskmap.tasks.remove_entry(&entry);

			// Is there any other running full setup or destruction
			// operation?
			let found = taskmap.tasks.iter().any(|(task, handle)| {
			    matches!(task, TaskEntry::FullSetup(_) | TaskEntry::DestroyServer(_))
				&& !handle.is_finished()
			});

			let _ = cmd.reply.send(Ok(DaemonReply::SetupInquiry(found)));
		    }

		    // Unsupported CliComm interactions are matched with this
		    // Unknown variant
		    DaemonComm::Unknown => {},

		    ref _comm  => {
			// If check returns Some(daemon_next), then we can
			// proceed. If it returned None, the check has
			// encountered an error and we do nothing.
			if let Some(daemon_next) = check_and_backup (
			    &the_cloud,
                            workspace.clone(),
                            sqlconn.clone(),
                            &mut taskmap,
			    cmd,
			).await? {
			    return Ok(daemon_next);
			}
                    },

                }
            }

            else => {
                // The channel was closed
		let shutdown_exit = DaemonNext {
		    code: EXIT_OK,
		    mode: ReconMode::Reload,
		    old_cloud: None,
		    new_cloud: None,
		    rebase_data: None,
		};
                return Ok(shutdown_exit);
            }
        }
    }
}
// =============================================================
