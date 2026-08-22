// SPDX-License-Identifier: GPL-3.0-or-later

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::stream::{FuturesUnordered, StreamExt};
use rand::seq::SliceRandom;
use rand::thread_rng;
use reqwest;
use reqwest::Client;
use rustls::pki_types::ServerName;
use serde_json::Value;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::Command;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinSet;
use tokio::time::{Duration, Interval, interval as intrvl, sleep, timeout};
use tokio_rusqlite::Connection as SqlConn;
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::ClientConfig;
use tokio_socks::tcp::Socks5Stream;
use tracing::{Instrument, Span, debug, error, info, info_span};
use url::Url;
use webpki_roots;

use crate::ansible::{AnsibleAction, AnsibleRun};
use crate::cli::SvcSummary;
use crate::constants::{IPV4_ENDPOINTS, IPV6_ENDPOINTS, SVC_TIMEOUT, XCPLANE_AGENT, XRAY_BIN};
use crate::daemon::SvcComm;
use crate::db::{read_status_data, write_status_data};
use crate::nft::{create_geoip_db, dl_geoip_data};
use crate::types::{
    AsyncStream, BoxError, DashAction, KetServer, SvcEntry, SvcError, SvcHealth, SvcInfo, SvcKind,
    SvcStatus, TaskEntry, WorkSpace,
};
use crate::xui::db::{last_xui_db, prune_xui_backups};

/// All the needed information for monitoring a major service
pub struct SvcContext {
    pub workspace: Arc<WorkSpace>,
    pub sqlconn: Arc<SqlConn>,
    pub interval: Interval,
    pub svc_info: Arc<SvcInfo>,
    pub socks5_address: Option<&'static str>,
    pub tx_aggr: Sender<(usize, SvcEntry)>,
    pub rx_svc: Receiver<SvcComm>,
    pub task_entry: TaskEntry,
    pub action_map: Arc<DashMap<TaskEntry, DashAction>>,
    pub auto_fix: bool,
    pub fix_threshold: u64,
    pub ndata: usize,
}

/// The needed information for applying a service checker in one step
#[derive(Clone)]
pub struct SvcChecker {
    pub workspace: Arc<WorkSpace>,
    pub entry: SvcEntry,
    pub action_map: Arc<DashMap<TaskEntry, DashAction>>,
    pub socks5_address: Option<&'static str>,
    pub txa: Sender<(usize, SvcEntry)>,
    pub tx_checker: Sender<SvcEntry>,
    pub ndata: usize,
    pub auto_fix: bool,
    pub fix_threshold: u64,
}

/// The context residuals after taking [`SvcChecker`] struct from [`SvcContext`]
pub struct SvcContextOut {
    pub task_entry: TaskEntry,
    pub interval: Interval,
    pub rx_svc: Receiver<SvcComm>,
    pub rx_checker: Receiver<SvcEntry>,
}

impl SvcContext {
    /// Builds SvcChecker & the residuals from SvcContext
    fn build_checker(self, svc_status: SvcStatus) -> (SvcChecker, SvcContextOut) {
        let (tx_checker, rx_checker) = mpsc::channel::<SvcEntry>(10);

        let checker = SvcChecker {
            workspace: self.workspace,
            entry: SvcEntry {
                info: self.svc_info,
                status: svc_status,
            },
            action_map: self.action_map,
            socks5_address: self.socks5_address,
            txa: self.tx_aggr,
            tx_checker,
            ndata: self.ndata,
            auto_fix: self.auto_fix,
            fix_threshold: self.fix_threshold,
        };

        let context_out = SvcContextOut {
            task_entry: self.task_entry,
            interval: self.interval,
            rx_svc: self.rx_svc,
            rx_checker,
        };

        (checker, context_out)
    }
}

impl SvcInfo {
    pub fn task(self: &Arc<Self>) -> TaskEntry {
        match self.kind {
            SvcKind::Ssh => TaskEntry::SshMonitor(self.clone()),
            SvcKind::Nginx => TaskEntry::NginxMonitor(self.clone()),
            SvcKind::Xray(_) => TaskEntry::XrayMonitor(self.clone()),
        }
    }
}

impl SvcError {
    fn print_error(&self, extra: &str) {
        match DateTime::<Utc>::from_timestamp(self.service.status.failed_since as i64, 0) {
            Some(since) => match &self.service.info.kind {
                SvcKind::Ssh => {
                    let endpoint =
                        &format!("{}:{}", self.service.info.server.ip, self.service.info.port);

                    self.log_error(&endpoint, since, extra, "unconnectable");
                }

                SvcKind::Nginx => {
                    let endpoint = format!(
                        "{}:{}",
                        self.service.info.server.uihostname(),
                        self.service.info.port
                    );

                    self.log_error(&endpoint, since, extra, "unreachable");
                }

                SvcKind::Xray(inbound) => {
                    let svc_inbound_opt = self
                        .service
                        .info
                        .server
                        .inbounds
                        .iter()
                        .find(|inb| &inb.name == inbound);

                    if let Some(svc_inbound) = svc_inbound_opt {
                        let endpoint = format!(
                            "{}:{}",
                            self.service.info.server.subhostname(),
                            svc_inbound.port
                        );

                        self.log_error(&endpoint, since, extra, "unhealthy");
                    } else {
                        error!(inbound = inbound, "inbound not found in the server")
                    }
                }
            },
            None => error!("failed to convert failed_since to human-readable format"),
        }
    }

    fn log_error(&self, endpoint: &str, since: DateTime<Utc>, extra: &str, msg: &str) {
        if extra.is_empty() {
            error!(endpoint = endpoint,
                   since = %since.format("%Y-%m-%d %H:%M:%S"),
                   failed_count = self.service.status.failed_count,
                   error = self.errmsg,
		   message = msg)
        } else {
            error!(endpoint = endpoint,
                   since = %since.format("%Y-%m-%d %H:%M:%S"),
                   failed_count = self.service.status.failed_count,
                   error = self.errmsg,
		   extra_info = extra,
		   message = msg)
        }
    }
}
///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

/// Runs the monitoring task for a service
pub async fn run_svc_monitor(context: SvcContext) -> Result<(), BoxError> {
    if let Some(ref url) = context.svc_info.link {
        info!(sublink = %url, "monitoring via sublink");
    } else {
        info!("monitoring");
    }

    let svc_status = read_status_data(context.sqlconn.clone(), context.svc_info.clone()).await?;

    if let Some(since) = DateTime::<Utc>::from_timestamp(svc_status.failed_since as i64, 0) {
        info!(failed_count = svc_status.failed_count, since = %since.format("%Y-%m-%d %H:%M:%S"),  "history");
    }

    // Prepare the action_map with the values loaded from DB
    if let Some(mut action) = context.action_map.get_mut(&context.task_entry) {
        action.svc_health = svc_status.health.clone();
        action.fix_try = svc_status.fix_try;
        action.fix_action = None;
    }

    let (mut checker, mut context_out) = context.build_checker(svc_status);

    let mut check_completed = true;

    // Utilizing a JoinSet so that we can match on the joinhandle of spawned
    // checker in tokio::select
    let mut check_tasks = JoinSet::new();

    loop {
        #[rustfmt::skip]
        tokio::select! {
	    // Interval has arrived and the prior check has been completed
            _ = context_out.interval.tick(), if check_completed => {
		check_completed = false;

		let this_checker = checker.clone();
		let current_span = Span::current();

		check_tasks.spawn(async move {
		    Ok::<(), BoxError>(apply_checker(this_checker).await?)
		}.instrument(current_span));
            }

	    // The check was successful and we received the updated SvcEntry
            Some(entry) = context_out.rx_checker.recv() => checker.entry = entry,

	    // A service command from daemon has arrived
            Some(cmd) = context_out.rx_svc.recv() => {
		match cmd {
                    SvcComm::ResetFix => {
			checker.entry.status.fix_try = 0;

			// Flush to the DB immediately
			checker.txa.send((checker.ndata, checker.entry.clone())).await?;

			// And update action_map, as Xray inbounds get their
			// fix_try synchronized with the supervisor's in it.
			if let Some(mut action) = checker.action_map.get_mut(&context_out.task_entry) {
			    action.svc_health = checker.entry.status.health.clone();
			    action.fix_try = checker.entry.status.fix_try;
			    action.fix_action = None;
			}
		    },

		    SvcComm::GetSummary(reply) => {
			let _ = reply.send(SvcSummary {
			    service: checker.entry.info.kind.clone(),
			    server: checker.entry.info.server.name.clone(),
			    health: checker.entry.status.health.clone(),
			});
		    },
		}
            }

	    // The checker of checker task
	    Some(res) = check_tasks.join_next() => {
		match res {
		    Ok(Ok(())) => {
			check_completed = true;
			debug!("checker tested service");
		    },
		    Ok(Err(e)) => {
			error!(error = e, "checker failed");
			check_completed = true;
		    }
		    Err(e) => error!(error = %e, "checker crashed"),
		}
	    }
        }
    }
}
// =============================================================
async fn apply_checker(checker: SvcChecker) -> Result<(), BoxError> {
    let response = check_service(
        &checker.workspace,
        checker.entry,
        checker.action_map.clone(),
        checker.socks5_address,
    )
    .await;

    let svc_entry = handle_response(
        response,
        checker.workspace,
        checker.action_map,
        checker.auto_fix,
        checker.fix_threshold,
    )
    .await?;

    checker.txa.send((checker.ndata, svc_entry.clone())).await?;
    checker.tx_checker.send(svc_entry).await?;

    Ok(())
}
// =============================================================
/// Writes the batch of data received from the aggregator channel to DB
pub async fn run_aggregator(
    conn: Arc<SqlConn>,
    inter: u64,
    mut rx: Receiver<(usize, SvcEntry)>,
) -> Result<(), BoxError> {
    // A ticker as a future which acts as a timeout when aggregating data
    let mut ticker = intrvl(Duration::from_secs(inter));
    let mut stats_vec: Vec<SvcEntry> = Vec::with_capacity(100);

    loop {
        #[rustfmt::skip]
        tokio::select! {
            // Either we get the full batch of data
            Some((nstats,stat)) = rx.recv() => {
                stats_vec.push(stat);
                if stats_vec.len() >= nstats {
                    write_status_data(conn.clone(), stats_vec).await?;
                    stats_vec = vec![];
                }
            },
            // Or we reach a timeout and process what we have anyway
            _i = ticker.tick() => {
                if !stats_vec.is_empty() {
                    write_status_data(conn.clone(), stats_vec).await?;
                    stats_vec = vec![];
		}
            },
        }
    }
}
// =============================================================
/// Runs xui DB backup pruning
pub async fn run_xuidb_pruner(
    workspace: Arc<WorkSpace>,
    server: Arc<KetServer>,
) -> Result<(), BoxError> {
    // Prunes every hour
    let mut interval = intrvl(Duration::from_hours(1));
    interval.tick().await; // the first tick is instant always

    loop {
        prune_xui_backups(&workspace, &server).await??;
        info!("pruned");
        interval.tick().await;
    }
}
// =============================================================
/// Checks the certificate state of a production server and does the renewal if needed
pub async fn run_acme_checker(
    workspace: Arc<WorkSpace>,
    server: Arc<KetServer>,
) -> Result<(), BoxError> {
    // Acme cert checking/renewal every day
    let mut interval = intrvl(Duration::from_hours(24));
    // Consume the first tick
    interval.tick().await;

    loop {
        // We won't let an Acme error crash the renewal service, as the problem
        // might only be intermittent.
        let ansible_run = AnsibleRun {
            workspace: workspace.clone(),
            server: server.clone(),
            actions: vec![AnsibleAction::Acme],
            stream_it: false,
            rebase_data: None,
        };

        match ansible_run.run().await {
            Ok(res) => {
                if res.output.status.success() {
                    info!("checked/renewed");
                } else {
                    let stdout = res.stdout.display();
                    let stderr = res.stderr.display();
                    error!(stdout = %stdout, stderr = %stderr, "check/renewal failed");
                }
            }
            // Ansible .run itself should never return an error
            Err(e) => {
                return Err(e);
            }
        }
        interval.tick().await;
    }
}
// =============================================================
pub async fn run_geoip_updater(workspace: Arc<WorkSpace>) -> Result<(), BoxError> {
    // Updating GeoIP DB every week
    let mut interval = intrvl(Duration::from_hours(168));
    interval.tick().await;

    loop {
        // During startup we already create the DB, therefore, we immediately
        // await the second tick
        interval.tick().await;

        // We won't let an error here crash the task as the problem might only
        // be intermittent
        let geoip_data = match dl_geoip_data().await {
            Ok(data) => data,
            Err(e) => {
                error!(error = e, "failed to download GeoIP data");
                continue;
            }
        };

        let _ = create_geoip_db(&workspace, geoip_data).await;
    }
}
// =============================================================
/// A function to check a TCP connection to a server either directly or through
/// a socks5 proxy. We will use our SvcError struct to hold extra information
/// handling error propagation.
async fn check_service(
    workspace: &WorkSpace,
    service: SvcEntry,
    action_map: Arc<DashMap<TaskEntry, DashAction>>,
    socks5_address: Option<&str>,
) -> Result<SvcEntry, SvcError> {
    let time_out = Duration::from_secs(SVC_TIMEOUT);
    match service.info.kind {
        SvcKind::Ssh => {
            let mut conn = create_tcp_stream(
                &service.info.server.ip.to_string(),
                service.info.port,
                socks5_address,
                time_out,
            )
            .await
            .map_err(|e| SvcError {
                service: service.clone(),
                errmsg: e,
            })?;
            check_ssh_stream(&mut conn, time_out)
                .await
                .map_err(|e| SvcError {
                    service: service.clone(),
                    errmsg: e,
                })?
        }

        SvcKind::Nginx => {
            let mut conn = create_tcp_stream(
                &service.info.server.uihostname(),
                service.info.port,
                socks5_address,
                time_out,
            )
            .await
            .map_err(|e| SvcError {
                service: service.clone(),
                errmsg: e,
            })?;
            check_tls_stream(&mut conn, service.info.clone(), time_out)
                .await
                .map_err(|e| SvcError {
                    service: service.clone(),
                    errmsg: e,
                })?
        }

        SvcKind::Xray(_) => check_xray(workspace, service.info.clone(), action_map)
            .await
            .map_err(|e| SvcError {
                service: service.clone(),
                errmsg: e,
            })?,
    }

    Ok(service)
}
// =============================================================
/// A function to handle the output of check_service and perform required
/// action. Here we will try automated fix actions whose joinhandles will be
/// stored in a shared registry named action_map.
async fn handle_response(
    res: Result<SvcEntry, SvcError>,
    workspace: Arc<WorkSpace>,
    action_map: Arc<DashMap<TaskEntry, DashAction>>,
    auto_fix: bool,
    fix_threshold: u64,
) -> Result<SvcEntry, BoxError> {
    let super_xray = SvcKind::super_xray();
    // Ok or Err, this function consumes SvcEntry and returns Ok(SvcEntry) in
    // both match arms
    match res {
        Ok(mut service) => {
            // Printing differently for better readability
            match &service.info.kind {
                SvcKind::Ssh => {
                    info!(
                        endpoint = format!("{}:{}", service.info.server.ip, service.info.port),
                        "connectable",
                    )
                }
                SvcKind::Nginx => {
                    info!(
                        endpoint =
                            format!("{}:{}", service.info.server.uihostname(), service.info.port),
                        "reachable"
                    )
                }
                SvcKind::Xray(inbound) if inbound == &SvcKind::super_name() => {
                    info!("all inbounds are healthy")
                }
                SvcKind::Xray(inbound) => {
                    let server_inbound = service
                        .info
                        .server
                        .inbounds
                        .iter()
                        .find(|inb| &inb.name == inbound)
                        .ok_or("The inbound cannot be found in the server.")?;

                    info!(
                        endpoint = format!(
                            "{}:{}",
                            service.info.server.subhostname(),
                            server_inbound.port
                        ),
                        "healthy"
                    )
                }
            }

            /* Now that everything is or has been fine, reset the stats in the
            running program which will be reflected in the DB later by the
            aggregator. We don't reset failed_since so that it can remain as
            an indicator of the service's most recent failure that is no
            longer present.
             */
            if service.status.failed_count > 0 || service.status.health != SvcHealth::Ok {
                service.status.health = SvcHealth::Ok;
                service.status.failed_count = 0;
                // service.status.failed_since = 0;
                service.status.fix_try = 0;
            }

            // Perform the exclusive steps for Ok result for the supervisor (DB backup)
            if &service.info.kind == &super_xray {
                super_xray_actions(&mut service, workspace, action_map).await?;
            } else {
                // For the other service kinds, we update action_map with no
                // action, and directly overwrite its value with no checking to
                // avoid extra contention.
                if let Some(mut action) = action_map.get_mut(&service.info.task()) {
                    action.svc_health = SvcHealth::Ok;
                    action.fix_try = 0;
                    action.fix_action = None;
                }
            }

            Ok(service)
        }

        Err(mut err) => {
            // Here we will only update the failed_count and won't touch the
            // failed_since unless it's the first time it is failing.
            err.service.status.failed_count += 1;
            if err.service.status.failed_count == 1 {
                err.service.status.failed_since =
                    SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            }
            if err.service.status.health != SvcHealth::Sick {
                err.service.status.health = SvcHealth::Sick;
            }

            // Regardless of the service, we update the status in action_map
            // immediately
            if let Some(mut action) = action_map.get_mut(&err.service.info.task()) {
                action.svc_health = SvcHealth::Sick;
            }

            let mut extra_info = String::from("");
            let mut some_ansible_action = None;

            // fix_try field in SvcStatus remains the source of truth which is
            // written to DB. But Fixing actions determine the level and update
            // both the fix_try in action_map, and the fix_try in SvcEntry.
            let svc_kind = &err.service.info.kind;
            let failed_count = err.service.status.failed_count;
            match (svc_kind, auto_fix, failed_count >= fix_threshold) {
                ///////////////////////////////////////////////////////////////
                (SvcKind::Ssh, true, true) => {
                    // If SSH on the server is not working, we're in deep
                    // trouble. Nothing can be done; however, it might be only
                    // SSH and other services could be working normally
                    // hopefully.
                    err.service.status.fix_try = 1;

                    // For SSH, only svc_health is read by the others. But we
                    // fully update it in action_map anyway.
                    let ssh_action = DashAction {
                        svc_health: err.service.status.health.clone(),
                        fix_try: err.service.status.fix_try,
                        fix_action: None,
                    };

                    if let Some(mut action) = action_map.get_mut(&err.service.info.task()) {
                        *action = ssh_action;
                    }

                    extra_info = "critical connection issue; needs manual inspection".into();
                }
                ///////////////////////////////////////////////////////////////
                (SvcKind::Nginx, true, true) => {
                    /*
                    If Nginx isn't working, provided that SSH connectivity is
                    there AND we haven't tried everything, as the first step, we
                    could try to restart it via Ansible and hope that it will
                    fix the issue. The second step would be re-configuring Nginx
                    and if that didn't work either, it's out of our hands.

                    To avoid keeping action_map locked for the other
                    threads, all mutations/access must not happen in a shared
                    lifetime with an async function. So we will define an action
                    flag, and will do the Ansible calls later.
                     */

                    let nginx_entry = err.service.info.task();
                    // Only if SSH on this server is fine, we can try fix actions.
                    let ssh_ok = sibling_health_check(&err.service.info, &action_map, SvcKind::Ssh);

                    // Only try to do something if SSH is working fine AND
                    // there's still a chance left to do something

                    if ssh_ok {
                        if err.service.status.fix_try < 2 {
                            // Only define action calls if we haven't already got
                            // one running. So the joinhandle should either be None
                            // or finished.

                            let (mut jh_none, jh_finished) = {
                                let action = action_map.get(&nginx_entry).ok_or::<BoxError>(
                                    "Couldn't get the Nginx 'action' from the \
				     action_map to see if the fix has finished."
                                        .into(),
                                )?;
                                if let Some(jh) = &action.fix_action {
                                    (false, jh.is_finished())
                                } else {
                                    (true, false)
                                }
                            };
                            // While is_finished method works on a reference, to
                            // await a joinhandle and check its output, we need
                            // to take the ownership. But we only do it when jh
                            // has truly finished.
                            if jh_finished {
                                let jh_opt = {
                                    let mut action =
                                        action_map.get_mut(&nginx_entry).ok_or::<BoxError>(
                                            "Couldn't take the Nginx 'action' \
					     from the action_map."
                                                .into(),
                                        )?;
                                    action.fix_action.take()
                                };
                                jh_none = true;

                                if let Some(jh) = jh_opt {
                                    // If a task's joinhandle has finished, since
                                    // we're in an error matching arm, it means our
                                    // fixing attempt was not successful.
                                    err.service.status.fix_try += 1;

                                    let output = jh.await??;
                                    let stdout = &output.stdout;
                                    let stderr = &output.stderr;
                                    let ansible_action = output.actions[0];

                                    let current_span = Span::current();
                                    let action_span = info_span!(parent: &current_span, "action",
                                        action = %ansible_action.as_ref(),
                                        stdout = %stdout.display(),
                                        stderr = %stderr.display(),
                                    );
                                    let _enter = action_span.enter();
                                    info!("fix result");
                                }
                            }

                            match (jh_none, err.service.status.fix_try) {
                                // If jh_none isn't true, we're in the middling of
                                // applying a fix, so no action should be set.
                                (true, 0) => {
                                    some_ansible_action = Some(AnsibleAction::NginxRestart);
                                    extra_info += "fix action: try to restart Nginx";
                                }
                                (false, 0) => {
                                    some_ansible_action = Some(AnsibleAction::NginxRestart);
                                    extra_info += "fix action: trying to restart Nginx";
                                }
                                (true, 1) => {
                                    some_ansible_action = Some(AnsibleAction::NginxRestoreConfig);
                                    extra_info += "fix action: restore Nginx config";
                                }
                                (false, 1) => {
                                    some_ansible_action = Some(AnsibleAction::NginxRestoreConfig);
                                    extra_info += "fix action: restoring Nginx config";
                                }
                                // matches n = 2 when processing the last fix
                                (_, n) if n > 1 => extra_info += "fixing attempts failed",
                                // We have run out of our fixing tries.
                                _ => {}
                            }

                            // Only add action when either there was no action
                            // in action_map, or there was one and it finished
                            // and was taken.
                            if jh_none {
                                // Only do something if there's something left to do
                                let the_action = if let Some(ansible_action) = some_ansible_action {
                                    // Store the joinhandle in action_map with
                                    // the new action
                                    let ansible_run = AnsibleRun {
                                        workspace: workspace.clone(),
                                        server: err.service.info.server.clone(),
                                        actions: vec![ansible_action],
                                        stream_it: false,
                                        rebase_data: None,
                                    };

                                    let jh = tokio::spawn(async move {
                                        Ok::<_, BoxError>(ansible_run.run().await?)
                                    });

                                    Some(jh)
                                } else {
                                    None
                                };

                                let nginx_action = DashAction {
                                    svc_health: err.service.status.health.clone(),
                                    fix_try: err.service.status.fix_try,
                                    fix_action: the_action,
                                };
                                if let Some(mut action) = action_map.get_mut(&nginx_entry) {
                                    *action = nginx_action;
                                }
                            }
                        } else {
                            extra_info += "automated fix attempts have been futile--\
					   inspect the server manually";
                        }
                    } else {
                        extra_info += "SSH is not working--no fixing attempt could be done";
                    }
                }
                ///////////////////////////////////////////////////////////////
                (SvcKind::Xray(inbound), true, true) if inbound == &SvcKind::super_name() => {
                    // The fixing actions are only done in super_xray_actions
                    // where it gets the current value of fix_try from SvcEntry
                    // and then mutates it inside the struct.
                    (some_ansible_action, extra_info) =
                        super_xray_actions(&mut err.service, workspace.clone(), action_map.clone())
                            .await?;
                }

                (SvcKind::Xray(_), true, true) => {
                    let xray_fix_try = super_sibling_fixtry(&err.service.info, &action_map)?;
                    // While each Xray inbound has its own status, all will
                    // get their fix_try values synchronized with the
                    // supervisor Xray.
                    err.service.status.fix_try = xray_fix_try;

                    // For the inbound itself, just update fix_try and the
                    // status, leaving the joinhandle only for the super_xray.
                    let xray_action = DashAction {
                        svc_health: err.service.status.health.clone(),
                        fix_try: xray_fix_try,
                        fix_action: None,
                    };

                    let key = err.service.info.task();
                    if let Some(mut action) = action_map.get_mut(&key) {
                        *action = xray_action;
                    }
                }

                // No action until we have reached the threshold
                (_, true, false) => {}

                // No action whatsoever when the user has disabled corrective actions
                (_, false, _) => {}
            }

            if let Some(ansible_action) = some_ansible_action {
                let current_span = Span::current();
                let action_span = info_span!(
                    parent: &current_span,
                    "action",
                    action = %ansible_action.as_ref(),
                );
                let _enter = action_span.enter();
                err.print_error(&extra_info);
            } else {
                err.print_error(&extra_info);
            }

            Ok(err.service)
        }
    }
}
// =============================================================
/// A function to check the validity of Xray service for an inbound
async fn check_xray(
    workspace: &WorkSpace,
    service_info: Arc<SvcInfo>,
    action_map: Arc<DashMap<TaskEntry, DashAction>>,
) -> Result<(), BoxError> {
    // Make sure the service given is an xray one
    match service_info.kind {
        SvcKind::Xray(ref inbound) => {
            if inbound == &SvcKind::super_name() {
                check_super_xray(service_info, action_map)?;
            } else {
                // Step 1: download the mother json sublink and modify its socks port
                let fname = format!("jsub_{}_{}.json", service_info.server.name, inbound);

                // Although ephemeral, the downloaded inbound is stored in data_dir for
                // security reasons
                let fpath = workspace
                    .dirs
                    .data_dir
                    .join(&service_info.server.name)
                    .join(&fname);

                if let Some(xurl) = &service_info.link {
                    dl_sublink(xurl.clone(), &fpath, service_info.port).await?;
                    // Step 2: create an Xray socks5h service using xray-bin and the
                    // downloaded & modified config. The socks5h proxy will run at
                    // 127.0.0.1 using the unique port stored in service_info.port
                    let mut xray_socks5_process = Command::new(XRAY_BIN)
                        .arg("run")
                        .arg("-c")
                        .arg(&fpath)
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .kill_on_drop(true)
                        .spawn()?;

                    // Step 3: we wait until the created proxy is ready to accept
                    // connections. If a timeout occurs, it means proxy creation
                    // wasn't successful. 3 seconds in total is plenty for checking.
                    let mut is_reachable = false;
                    for _ in 0..10 {
                        match create_tcp_stream(
                            "127.0.0.1",
                            service_info.port,
                            None,
                            Duration::from_millis(200),
                        )
                        .await
                        {
                            Ok(_) => {
                                is_reachable = true;
                                break;
                            }
                            Err(_) => sleep(Duration::from_millis(100)).await,
                        }
                    }

                    if !is_reachable {
                        // Kill the created socks5h process and don't care about
                        // the output
                        let _ = xray_socks5_process.kill().await;
                        let _ = xray_socks5_process.wait().await;

                        return Err("The proxy couldn't be created via Xray binary, \
				    or it was unreachable."
                            .into());
                    }
                    /*
                    Step 4: we will get our global IP by a visit to several IP
                    returning web services around the world through the created
                    proxy, and compare them with the server IP itself. A minimum
                    number of matches is required to conclude the service is
                    running fine.

                    We first construct a proxy client via reqwest. Kudos to
                    reqwest for supporting DNS through socks5 feature aka socks5h
                    which is a pillar of Xray. It comes in very handy here.

                    Note: 'socks' feature must be enabled for reqwest or it will
                    silently fail and will not forward the traffic.
                     */
                    let proxy =
                        reqwest::Proxy::all(format!("socks5h://127.0.0.1:{}", service_info.port))?;
                    let client = reqwest::Client::builder()
                        .proxy(proxy)
                        .user_agent(XCPLANE_AGENT)
                        .timeout(Duration::from_secs(SVC_TIMEOUT))
                        .build()?;

                    let mut ip_endpoints: [&str; IPV4_ENDPOINTS.len()];
                    let svc_ip = service_info.server.ip;
                    if svc_ip.is_ipv4() {
                        ip_endpoints = IPV4_ENDPOINTS;
                    } else {
                        ip_endpoints = IPV6_ENDPOINTS;
                    }
                    // Shuffle the endpoint so that we try them in different orders each time
                    ip_endpoints.shuffle(&mut thread_rng());
                    let mut matches = 0;
                    // If we get this number of matches, our Xray service works fine.
                    let required_matches = 2;
                    // A concurrent set of futures for simultaneous IP checking
                    let mut ip_futures = FuturesUnordered::new();

                    for endpoint in ip_endpoints {
                        let fclient = client.clone();

                        ip_futures.push(async move {
                            // We're not interested in any error here--just get the
                            // data if there is any and mark it done
                            let fetched_ip =
                                fclient.get(endpoint).send().await.ok()?.text().await.ok()?;

                            Some(fetched_ip)
                        });
                    }
                    let mut ip_ok = false;
                    while let Some(Some(ip)) = ip_futures.next().await {
                        if ip.trim() == service_info.server.ip.to_string() {
                            matches += 1;
                            if matches >= required_matches {
                                ip_ok = true;
                                break;
                            }
                        }
                    }
                    // Step 5: First kill the created socks5h process to make sure
                    // we won't have any left overs
                    let _ = xray_socks5_process.kill().await;
                    let _ = xray_socks5_process.wait().await;

                    // Step 6. Then check if we have succeeded
                    if !ip_ok {
                        return Err("The returned IP is not the same as the server IP".into());
                    }
                } else {
                    return Err("The link in an Xray service shouldn't be empty".into());
                }
            }
        }
        _ => {
            return Err("The service given is not an Xray one".into());
        }
    }

    Ok(())
}
// =============================================================
/// A function to check if all Xray inbounds are working properly
fn check_super_xray(
    svc_info: Arc<SvcInfo>,
    action_map: Arc<DashMap<TaskEntry, DashAction>>,
) -> Result<(), BoxError> {
    // This is called when svc_info is for the super xray
    action_map.iter().try_for_each(|entry| {
        let task = entry.key();
        let action = entry.value();

        if let TaskEntry::XrayMonitor(svc) = task {
            if svc.kind != svc_info.kind
                && svc.server == svc_info.server
                && action.svc_health != SvcHealth::Ok
            {
                return Err("one or more unhealthy inbounds".into());
            }
        }

        Ok(())
    })
}
// =============================================================
/// Handles the fixing actions for Xray services as a whole. All the Xray
/// inbounds are already being monitored individually.
async fn super_xray_actions(
    super_service: &mut SvcEntry,
    workspace: Arc<WorkSpace>,
    action_map: Arc<DashMap<TaskEntry, DashAction>>,
) -> Result<(Option<AnsibleAction>, String), BoxError> {
    let super_key = TaskEntry::XrayMonitor(super_service.info.clone());

    let mut some_ansible_action = None;
    let mut extra_info = String::from("");

    match &super_service.status.health {
        SvcHealth::Ok => {
            // If the supervisor Xray is marked as Ok, we will backup the
            // server's x-ui DB.
            let this_server = super_service.info.server.clone();
            let this_workspace = workspace.clone();

            // The context already inherits its parent, but for the sake of
            // being explicit we set the parent
            let current_span = Span::current();
            let action_span = info_span!(
                parent: &current_span,
                "action",
                action = "DbBackup",
            );

            tokio::spawn(
                async move {
                    // We fully check the backup result here, because we use the
                    // dashmap only for storing error joinhandles (since transitions
                    // from Ok to Sick can cause bugs in processing response).

                    match this_server.xui_call_db(&this_workspace).await {
                        Ok(_) => {
                            info!("succeeded");
                        }
                        Err(e) => {
                            error!(error = e, "failed");
                        }
                    }
                }
                .instrument(action_span),
            );
        }

        SvcHealth::Sick => {
            // If the supervisor is Sick, provided that SSH & Nginx are fine, we will
            // try to, first, restart the x-ui service, and if it didn't work, we will
            // restore the last known good DB into the server.

            let ssh_nginx_ok = sibling_health_check(&super_service.info, &action_map, SvcKind::Ssh)
                && sibling_health_check(&super_service.info, &action_map, SvcKind::Nginx);

            if ssh_nginx_ok {
                if super_service.status.fix_try < 2 {
                    // Checking the supervisor for any action
                    let (jh_none, jh_finished) = {
                        let action = action_map.get(&super_key).ok_or::<BoxError>(
                            "Couldn't get the super Xray 'action' from the \
			     action_map to see if the fix has finished."
                                .into(),
                        )?;

                        if let Some(jh) = &action.fix_action {
                            (false, jh.is_finished())
                        } else {
                            (true, false)
                        }
                    };

                    // Taking ownership if the action has finished
                    if jh_finished {
                        let jh_opt = {
                            let mut action = action_map.get_mut(&super_key).ok_or::<BoxError>(
                                "Couldn't take the super Xray 'action' \
				 from the action_map."
                                    .into(),
                            )?;
                            action.fix_action.take()
                        };

                        if let Some(jh) = jh_opt {
                            // fix_try is updated only when a prior action has completed and failed.
                            super_service.status.fix_try += 1;

                            let output = jh.await??;
                            // Since the fix action itself might have had a
                            // successful exit code, we need to see both stdout
                            // and stderr.
                            let stdout = &output.stdout;
                            let stderr = &output.stderr;

                            // For fixing actions, one AnsibleAction is put in
                            // the vector
                            let ansible_action = output.actions[0];

                            let current_span = Span::current();
                            let action_span = info_span!(parent: &current_span, "action",
                                action = %ansible_action.as_ref(),
                                stdout = %stdout.display(),
                                stderr = %stderr.display(),
                            );
                            let _enter = action_span.enter();
                            info!("fix result");

                            // If we are going to try the second fix action
                            // which is restoring the DB, we need to check if
                            // any backup has already been made or not. If not,
                            // no fixing action will be done.
                            if super_service.status.fix_try == 1 {
                                if last_xui_db(&workspace, &super_service.info.server)
                                    .await?
                                    .is_err()
                                {
                                    extra_info += "no backup exits, ";
                                    super_service.status.fix_try += 1;
                                }
                            }
                        }

                        /*
                        If there was a finished action which we took out, we
                        want to avoid immediate selection of the next try. This
                        allows the super xray status to be computed against
                        fully up-to-data inbound status data in the next
                        interval, and prevents from racing conditions in shorter
                        checking intervals.

                        Therefore, even though we have taken the jh out, we
                        don't set jh_none to true.

                        jh_none = true;
                         */
                    }

                    match (jh_none, super_service.status.fix_try) {
                        (true, 0) => {
                            some_ansible_action = Some(AnsibleAction::XrayRestart);
                            extra_info += "fix action: try to restart Xray";
                        }
                        (true, 1) => {
                            some_ansible_action = Some(AnsibleAction::XrayRestoreDB);
                            extra_info += "fix action: restore the last known good DB";
                        }
                        _ => {}
                    }

                    if jh_none {
                        let the_action = if let Some(ansible_action) = some_ansible_action {
                            let ansible_run = AnsibleRun {
                                workspace: workspace.clone(),
                                server: super_service.info.server.clone(),
                                actions: vec![ansible_action],
                                stream_it: false,
                                rebase_data: None,
                            };
                            let jh =
                                tokio::spawn(
                                    async move { Ok::<_, BoxError>(ansible_run.run().await?) },
                                );

                            Some(jh)
                        } else {
                            None
                        };

                        // Store the joinhandle in action_map for the supervisor
                        if let Some(mut action) = action_map.get_mut(&super_key) {
                            action.fix_action = the_action;
                            action.fix_try = super_service.status.fix_try;
                        }
                    }
                } else {
                    extra_info += "automated fix attempts have been futile--\
				   inspect the server manually";
                }
            } else {
                extra_info += "SSH and/or Nginx are not working--no fixing attempt could be done";
            }
        }
        _ => {}
    }

    Ok((some_ansible_action, extra_info))
}
// =============================================================
/// Downloads the json content from a sublink URL, changes the ports on the fly
/// to the new port, and stores it into the file whose path is given
async fn dl_sublink(link: Url, filename: &PathBuf, newport: u16) -> Result<(), BoxError> {
    // Download the json sublink file as text
    let xclient = Client::builder()
        .user_agent(XCPLANE_AGENT)
        .timeout(Duration::from_secs(SVC_TIMEOUT))
        .build()?;

    let resp = xclient
        .get(link)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let mut json: Value = serde_json::from_str(&resp)?;
    if let Some(inbounds) = json.get_mut("inbounds").and_then(|v| v.as_array_mut()) {
        for entry in inbounds {
            let protocol = entry.get("protocol").and_then(|v| v.as_str());
            // Change socks/mixed port
            if protocol == Some("socks") || protocol == Some("mixed") {
                if let Some(port) = entry.get_mut("port") {
                    *port = Value::from(newport);
                }
            }
            // For http add newport to its original value
            else if protocol == Some("http") {
                if let Some(port) = entry.get_mut("port").and_then(|v| v.as_u64()) {
                    entry["port"] = Value::from(port + newport as u64);
                }
            }
        }
    }
    let newjson = serde_json::to_string_pretty(&json)?;

    fs::write(filename, newjson).await?;

    Ok(())
}
// =============================================================
/// Gets SSH response from either a TcpStream or a Socks5stream<TcpStream>
async fn check_ssh_stream<S>(stream: &mut S, duration: Duration) -> Result<(), BoxError>
where
    S: AsyncStream + Unpin + Send,
{
    let mut buf = [0u8; 256];
    let n = timeout(duration, stream.read(&mut buf)).await??;
    if n == 0 {
        return Err(format!("Connection closed before SSH banner.").into());
    }
    let banner = std::str::from_utf8(&buf[..n])?;
    if banner.starts_with("SSH-") {
        return Ok(());
    } else {
        return Err(format!("Not an SSH server.").into());
    }
}
// =============================================================
/// Checks if the input stream (either a TcpStream or a Socks5stream<TcpStream>)
/// is proper HTTPS or not. Since we thoroughly check Xray services later, a
/// lower-level approach has been adopted for checking (not using reqwest even
/// though it is faster and simpler) to make our Nginx checking more thorough.
async fn check_tls_stream<S>(
    stream: &mut S,
    server_info: Arc<SvcInfo>,
    time_out: Duration,
) -> Result<(), BoxError>
where
    S: AsyncStream + Unpin + Send,
{
    let ui_domain = server_info.server.uihostname();
    let health_path = &server_info.server.secrets.health_path;

    let mut root_store = rustls::RootCertStore::empty();
    // Load the certs from the Mozilla Root Program
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(config));

    let server_name = ServerName::try_from(ui_domain.to_owned())?;

    let mut handshake = timeout(time_out, connector.connect(server_name, stream)).await??;

    // Request only a Head (GET without body) response followed by closing the
    // connection so we can read it and finish quickly. We use the HTTP standard
    // i.e. double /r/n to indicate the end of Head.
    let req = format!(
        "HEAD /{} HTTP/1.1\r\nHost: {}\r\nUser-Agent: {}\r\nAccept: */*\r\nConnection: close\r\n\r\n",
        health_path, ui_domain, XCPLANE_AGENT
    );

    timeout(time_out, handshake.write_all(req.as_bytes())).await??;
    timeout(time_out, handshake.flush()).await??;

    let mut buf = Vec::with_capacity(1024);
    let mut tmp = [0u8; 512];

    // Read it until end of headers while capping bytes to avoid hanging.  Fill
    // buf with tmp and break as soon as we get double \r\n by looking at the
    // sliding window (same way as Nginx does).
    loop {
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 16 * 1024 {
            return Err("Handshake with the server returned too large headers".into());
        }
        let n = timeout(time_out, handshake.read(&mut tmp)).await??;
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }

    // First, check the status line
    if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
        let status_line = String::from_utf8_lossy(&buf[..pos]);

        if status_line.contains("200 OK") {
            // Now look for the verification header, too
            let full_headers = String::from_utf8_lossy(&buf);

            if full_headers
                .to_lowercase()
                .contains("x-health-verify: my-origin-server")
            {
                return Ok(());
            }

            return Err("Status 200 received, but the secret 'X-Health-Verify' \
			header is missing (possible cache hit or wrong origin)"
                .into());
        }

        // Cloudflare-specific errors (520-527)
        let cf_errors = ["520", "521", "522", "523", "524", "525", "526", "527"];
        if cf_errors.iter().any(|&code| status_line.contains(code)) {
            return Err(format!(
                "Cloudflare edge is up, but Nginx is having issues: {}",
                status_line
            )
            .into());
        }

        return Err(format!("HTTP Error: {}", status_line).into());
    }

    Err("No valid HTTP status line found in TLS response".into())
}
// =============================================================
/// TCP connector for checking connections. It is used to connect to domains or
/// direct IPs.
async fn create_tcp_stream(
    addr: &str,
    port: u16,
    socks5_address: Option<&str>,
    time_out: Duration,
) -> Result<Box<dyn AsyncStream + Unpin + Send>, BoxError> {
    // Caller defines input types, callee defines output shapes; can't return a
    // generic from a function easily and have to wrap it in a smart pointer

    if let Some(socks_addr) = socks5_address {
        let conn = timeout(
            time_out,
            Socks5Stream::connect(socks_addr, format!("{}:{}", addr, port)),
        )
        .await??;
        Ok(Box::new(conn))
    } else {
        let conn = timeout(time_out, TcpStream::connect(format!("{}:{}", addr, port))).await??;
        Ok(Box::new(conn))
    }
}
// =============================================================
/// Checks if the port at the IP is serving SSH connections or not
pub async fn test_ssh_conn(ip: &str, port: u16) -> Result<(), BoxError> {
    let timeout = Duration::from_secs(SVC_TIMEOUT);
    let mut stream = create_tcp_stream(ip, port, None, timeout).await?;
    check_ssh_stream(&mut stream, timeout).await?;

    Ok(())
}
// =============================================================
/// Checks if the health of a sibling service in the action_map is Ok
fn sibling_health_check(
    svc_info: &SvcInfo,
    action_map: &DashMap<TaskEntry, DashAction>,
    sibling: SvcKind,
) -> bool {
    action_map
        .iter()
        .find(|entry| match sibling {
            SvcKind::Ssh => matches!(
                    entry.key(),
                    TaskEntry::SshMonitor(svc) if svc.server == svc_info.server
            ),

            SvcKind::Nginx => matches!(
                entry.key(),
                TaskEntry::NginxMonitor(svc) if svc.server == svc_info.server
            ),

	    // If the health check of any Xray sibling is requested, we reach
	    // out to the supervisor.
	    // This match arm is not utilized in the program.
            SvcKind::Xray(_) => matches!(
                entry.key(),
                TaskEntry::XrayMonitor(svc) if svc.server == svc_info.server && svc.kind == SvcKind::super_xray()
            ),
        })
        .is_some_and(|entry| entry.value().svc_health == SvcHealth::Ok)
}
// =============================================================
fn super_sibling_fixtry(
    svc_info: &SvcInfo,
    action_map: &DashMap<TaskEntry, DashAction>,
) -> Result<u8, BoxError> {
    //let mut res = Err("Super xray sibling was not found in action_map.".into());

    let fix_try = action_map
        .iter()
        .find(|entry| {
            matches!(
		entry.key(), TaskEntry::XrayMonitor(svc)
		    if svc.server == svc_info.server && svc.kind == SvcKind::super_xray())
        })
        .and_then(|entry| Some(entry.value().fix_try))
        .ok_or("Couldn't extract super xray fix_try from action_map")?;

    Ok(fix_try)
}
