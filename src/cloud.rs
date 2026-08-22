// SPDX-License-Identifier: GPL-3.0-or-later

pub mod reconcile;
pub mod server;
pub mod service;

use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::{collections::HashSet, sync::Arc};
use tokio::fs;
use tokio::sync::{Mutex, mpsc::Sender};
use tokio::task::JoinHandle;
use tokio_rusqlite::Connection as SqlConn;
use tracing::{Instrument, Span, info_span};
use tracing::{error, info, instrument};
use url::Host;

use crate::ansible::full_setup;
use crate::cli::ExpandOpts;
use crate::constants::{
    CLOUD_CONFIG, MIN_MON_INTERVAL, NGINX_PORT, SSH_PORT, SUB_PORT, UI_PORT, UNBOUND_PORT,
};
use crate::daemon::{DaemonReply, DaemonRequest};
use crate::types::{
    BoxError, Cloud, CloudSettings, Inbound, KetServer, ServerState, Slugged, SvcKind, TaskEntry,
    TaskHandle, TaskMap, WorkSpace, is_proper, is_proper_subdomain,
};

impl TaskEntry {
    pub fn server(&self) -> Option<&str> {
        match self {
            TaskEntry::SshMonitor(svc)
            | TaskEntry::NginxMonitor(svc)
            | TaskEntry::XrayMonitor(svc) => Some(&svc.server.name),

            TaskEntry::FullSetup(server)
            | TaskEntry::XuiDbPruner(server)
            | TaskEntry::AcmeChecker(server) => Some(&server.name),

            TaskEntry::SocketListener
            | TaskEntry::SignalWatcher
            | TaskEntry::Aggregator
            | TaskEntry::GeoipUpdater => None,
        }
    }

    pub fn service(&self) -> Option<String> {
        match self {
            TaskEntry::SshMonitor(svc)
            | TaskEntry::NginxMonitor(svc)
            | TaskEntry::XrayMonitor(svc) => Some(svc.kind.nice_display()),

            TaskEntry::FullSetup(_)
            | TaskEntry::XuiDbPruner(_)
            | TaskEntry::AcmeChecker(_)
            | TaskEntry::SocketListener
            | TaskEntry::SignalWatcher
            | TaskEntry::Aggregator
            | TaskEntry::GeoipUpdater => None,
        }
    }

    pub fn span(&self) -> Span {
        match self {
            TaskEntry::SocketListener
            | TaskEntry::SignalWatcher
            | TaskEntry::Aggregator
            | TaskEntry::GeoipUpdater => info_span!("monitor", task = %self),

            TaskEntry::FullSetup(server)
            | TaskEntry::XuiDbPruner(server)
            | TaskEntry::AcmeChecker(server) => info_span!(
                "monitor",
                task = %self,
                server = server.name
            ),

            TaskEntry::SshMonitor(svc) | TaskEntry::NginxMonitor(svc) => info_span!(
                    "monitor",
                    task = %self,
                    server = svc.server.name,
                    service = svc.kind.display()
            ),

            TaskEntry::XrayMonitor(svc) => {
                let inbound = if let SvcKind::Xray(inbound) = &svc.kind {
                    inbound.as_str()
                } else {
                    "-"
                };

                info_span!(
                    "monitor",
                    task = %self,
                    server = svc.server.name,
                    service = svc.kind.display(),
                    inbound = inbound
                )
            }
        }
    }

    pub fn nice_display(&self) -> String {
        match (self.server(), self.service()) {
            (None, _) => self.to_string(),
            (Some(server), None) => format!("{}({})", self.to_string(), server),
            (Some(server), Some(service)) => {
                format!("{}({}({}))", self.to_string(), server, service)
            }
        }
    }
}

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

/// Spawns an ordinary task concurrently
pub fn spawn_task<F, T>(future: F, entry: TaskEntry) -> JoinHandle<Result<T, BoxError>>
where
    F: Future<Output = Result<T, BoxError>> + Send + 'static,
    T: Send + 'static,
{
    let task_span = entry.span();
    info!(parent: &task_span, "starting");

    tokio::spawn(
        async move { future.await.inspect_err(|e| error!(error = e, "crashed")) }
            .instrument(task_span),
    )
}
// =============================================================
/// A function to parse cloud configuration from CLOUD_CONFIG and build the
/// cloud
#[instrument(name = "parse_cloud", skip_all)]
pub async fn parse_cloud_config(workspace: &WorkSpace) -> Result<Cloud, BoxError> {
    let cloud_config = fs::read_to_string(workspace.dirs.config_dir.join(CLOUD_CONFIG))
        .await
        .inspect_err(|_| error!("failed to read the cloud config file"))?;

    // A local struct for successfully parsing the config file first, as directly
    // reading the servers as Vec<Arc<>> is unsupported.
    #[derive(Debug, Deserialize)]
    struct TCloud {
        servers: Vec<KetServer>,
        settings: CloudSettings,
    }

    let mut cloud: TCloud = toml::from_str(&cloud_config)
        .inspect_err(|_| error!("failed to parse the cloud config file"))?;

    let mut cloud_inbound_names = HashSet::<&str>::new();
    let mut cloud_inbound_ports = HashSet::<u16>::new();
    let mut cloud_inbound_nps = HashSet::<(&str, u16)>::new();

    if cloud.settings.inbound_set.is_empty() {
        return Err("[ Cloud Config ] The cloud inbound-set cannot be empty.".into());
    }

    for inb in &cloud.settings.inbound_set {
        if !is_proper(&inb.name) {
            return Err(format!(
                "[ Cloud Config ] The inbound-set name '{}' is not a proper name. \
		 A valid name can be '{}'",
                inb.name,
                inb.name.slug()
            )
            .into());
        }

        if !cloud_inbound_names.insert(&inb.name) {
            return Err("[ Cloud Config ] The cloud inbound-set names are not unique.".into());
        }

        if !cloud_inbound_ports.insert(inb.port) {
            return Err("[ Cloud Config ] The cloud inbound-set ports are not unique.".into());
        }

        cloud_inbound_nps.insert((&inb.name, inb.port));
    }

    let super_name = SvcKind::super_name();
    // Add the super_name for later comparison with each server's inbounds
    if !cloud_inbound_names.insert(&super_name) {
        return Err(format!(
            "[ Cloud Config ] The cloud inbound-set names have '{}' which is a reserved keyword.",
            super_name
        )
        .into());
    }
    // We should check that the cloud inbound-set ports don't have any conflict
    // with the other defined ports: ssh_port, nginx_port, ui_port, sub_port,
    // unbound_port
    for port in [SSH_PORT, NGINX_PORT, UI_PORT, SUB_PORT, UNBOUND_PORT] {
        if !cloud_inbound_ports.insert(port) {
            return Err(format!(
                "[ Cloud Config ] {} is a reserved port and should not be \
		 duplicated or used for the cloud inbound-set ports.",
                port
            )
            .into());
        }
    }

    let mut names_set = HashSet::<&str>::new();
    let mut domains_set = HashSet::<&Host>::new();
    let mut ips_set = HashSet::<&IpAddr>::new();

    // We enforce uniqueness on names, IPs and domains
    for s in &mut cloud.servers {
        // Construct AnsibleConn for this server
        s.build_inventory();

        // If a server's inbounds are empty, we take the cloud's inbound-set
        if s.inbounds.is_empty() {
            s.inbounds = cloud.settings.inbound_set.clone();
        }

        // Generating secrets must be done after determining the server's inbounds
        s.fill_secrets();

        if !is_proper(&s.name) {
            return Err(format!(
                "[ Cloud Config ] The server name '{}' is not a proper name. \
		 A valid name can be '{}'",
                s.name,
                s.name.slug()
            )
            .into());
        }

        if !names_set.insert(&s.name) {
            return Err("[ Cloud Config ] Server names are not unique.".into());
        }

        if !domains_set.insert(&s.domain) {
            return Err("[ Cloud Config ] Domain names are not unique.".into());
        }

        if !is_global(&s.ip) {
            return Err(format!("[ Cloud Config ] Server {} has a non-global IP.", s.name).into());
        }

        if !ips_set.insert(&s.ip) {
            return Err("[ Cloud Config ] Servers IPs are not unique.".into());
        }

        // The subdomains for DNS, UI and subscription must be unique and proper
        // subdomains.
        let mut subdomains = HashSet::<&str>::new();
        for sd in [&s.sub_subdomain, &s.ui_subdomain, &s.dns_subdomain] {
            if !is_proper_subdomain(&sd) {
                return Err(format!(
                    "'{}' is not a proper subdomain. A subdomain must be \
		     lowercase and start and end with an ASCII alphanumeric character, \
		     and only hyphen is allowed in the middle.",
                    sd
                )
                .into());
            }

            if !subdomains.insert(sd) {
                return Err(
                    format!("'{}' is not a unique subdomain in server '{}'.", sd, s.name).into(),
                );
            }
        }

        /*
        We are strict about the user defined inbounds for each server, too.
        Therefore, if inbounds for a server have been set, their names & ports
        must be unique, and no inbound should be named "super" as it is a
        keyword reserved for the supervisor Xray.
         */
        if !s.inbounds.is_empty() {
            let mut server_inbound_names = HashSet::<&str>::new();
            let mut server_inbound_ports = HashSet::<u16>::new();
            let mut server_inbound_nps = HashSet::<(&str, u16)>::new();

            server_inbound_names.insert(&super_name);

            for inbound in &s.inbounds {
                if !server_inbound_names.insert(&inbound.name) {
                    return Err(format!(
                        "[ Cloud Config ] The inbound names for the server '{}' are not unique, \
			 or one is named '{}' which is a reserved keyword.",
                        s.name, super_name
                    )
                    .into());
                }

                if !server_inbound_ports.insert(inbound.port) {
                    return Err(format!(
                        "[ Cloud Config ] The inbound ports for the server '{}' are not unique.",
                        s.name
                    )
                    .into());
                }

                server_inbound_nps.insert((&inbound.name, inbound.port));
            }

            // Checking for the port conflict
            for port in [SSH_PORT, NGINX_PORT, UI_PORT, SUB_PORT, UNBOUND_PORT] {
                if !server_inbound_ports.insert(port) {
                    return Err(format!(
                        "[ Cloud Config ] Server '{}' cannot use {} as an inbound port \
			 because it is a reserved port and should not be duplicated \
			 or used anywhere else.",
                        s.name, port
                    )
                    .into());
                }
            }

            // The inbound (name, port) for each server must also be a subset of
            // the cloud inbound-set (name, port). We enforce this to save
            // ourselves from headache, in case the user Remaps to an empty
            // inbound for a server and than comes back.
            if !server_inbound_nps.is_subset(&cloud_inbound_nps) {
                return Err(format!(
                    "[ Cloud Config ] The inbounds of server '{}' \
		     must be a sub-group of the cloud inbound-set.",
                    s.name
                )
                .into());
            }
        }

        // Add the super inbound for each server too
        s.inbounds.push(Inbound::super_inbound());

        s.xui = None.into();
        s.login_gate = Arc::new(Mutex::new(()));
    }

    let arc_servers = cloud.servers.into_iter().map(|s| Arc::new(s)).collect();

    let settings = CloudSettings {
        inbound_set: cloud.settings.inbound_set,
        auto_fix: cloud.settings.auto_fix,

        // A too low value can trigger needless fix actions while the problem
        // might only be intermittent.
        fix_threshold: cloud.settings.fix_threshold,

        // Faster than the minimum for service monitoring interval is
        // meaningless and can cause problems.
        monitor_interval: cloud.settings.monitor_interval.max(MIN_MON_INTERVAL),
    };

    let the_cloud = Cloud {
        servers: arc_servers,
        settings,
    };

    info!("parsed cloud config");
    Ok(the_cloud)
}
// =============================================================
/// Expands the cloud by doing a full setup on an enabled Offgrid server
pub async fn expand_cloud(
    cloud: &Cloud,
    workspace: Arc<WorkSpace>,
    expand_opts: ExpandOpts,
    taskmap: &mut TaskMap,
    sql_conn: Arc<SqlConn>,
    tx: Sender<DaemonRequest>,
) -> Result<DaemonReply, BoxError> {
    // We need to do the full setup task in a loop-resilient structure, as the
    // 'expand' command may be invoked several times repeatedly.

    let random_server = cloud
        .servers
        .iter()
        .find(|s| {
            s.state.load() == ServerState::Offgrid && s.enabled && s.secrets.xui_token.is_none()
        })
        .cloned();

    // Does a server with the given name even exist in the cloud?
    let given_server = if let Some(ref name_given) = expand_opts.server {
        cloud
            .servers
            .iter()
            .find(|server| &server.name == name_given)
            .cloned()
    } else {
        None
    };

    let final_server = match (expand_opts.server, given_server, random_server) {
        (Some(name), Some(server), _) => {
            // More checking is needed for the input server
            match (
                server.state.load(),
                server.enabled,
                server.secrets.xui_token.is_none(),
            ) {
                (_, false, _) => return Err(format!("Server '{}' is not enabled.", name).into()),
                (ServerState::Offgrid, true, true) => server,
                (ServerState::Production, _, _) => {
                    return Err(format!("Server '{}' is already a production unit.", name).into());
                }
                (_, _, false) => {
                    /*
                    Unless a Reload is done (manually or by shutting down the
                    daemon and restarting) repeating Remap from Production to
                    Offgrid and vice versa will not change the secrets. When a
                    Remap to Offgrid is performed, it must be followed by a
                    Reload to refresh the secrets (Reload takes the new spec for
                    Offgrid servers).
                     */
                    return Err(format!(
                        "Server '{}' has been a production unit before. \
			 Do a Reload first, to refresh the secrets.",
                        name
                    )
                    .into());
                }
            }
        }

        (Some(name), None, _) => {
            return Err(
                format!("A server with name '{}' does not exist in the cloud.", name).into(),
            );
        }

        (None, _, Some(server)) => server,
        (None, _, None) => {
            return Err(
                "No fresh, enabled and offgrid server is available for setup. Add a new \
		 server to the cloud config and reload the daemon."
                    .into(),
            );
        }
    };

    // We check if we aren't already doing a full setup, and if we are, then we
    // need to further check whether it is finished or not.
    let mut is_setup_running = true;
    let entry_opt = taskmap
        .tasks
        .keys()
        .find(|key| matches!(key, TaskEntry::FullSetup(_)))
        .cloned();

    if let Some(ref entry @ TaskEntry::FullSetup(ref server)) = entry_opt {
        if let Some(TaskHandle::Detached(jh)) = taskmap.tasks.get(entry) {
            if jh.is_finished() {
                // A prior full setup has finished (either successfully or not)
                taskmap.tasks.remove(&TaskEntry::FullSetup(server.clone()));
                is_setup_running = false;
            } else {
                let res = format!(
                    "Expansion in progress: full setup is already running for server '{}'",
                    server.name
                );
                info!("{}", res);
                return Ok(DaemonReply::Message(res));
            }
        }
    } else {
        is_setup_running = false;
    }

    // Do the setup when either no setup has been done, or one has been
    // attempted and has finished/failed.
    if !is_setup_running {
        // In the registry we store it with the setup_prefix
        let task_entry = TaskEntry::FullSetup(final_server.clone());

        let jh = spawn_task(
            full_setup(sql_conn, Some(tx), final_server.clone(), workspace),
            task_entry.clone(),
        );
        // Store the server setup joinhandle in the taskmap, too
        taskmap
            .tasks
            .entry(task_entry)
            .or_insert(TaskHandle::Detached(jh));
    }

    Ok(DaemonReply::Message(format!(
        "Server '{}' is being provisioned to become Production.",
        final_server.name
    )))
}
// =============================================================
/// Performs a sanity check whether an IP is suitable for a server address or
/// not. Note that this function neither is exhaustive, nor it can determine
/// whether an arbitrary public IP (e.g. Cloudflare's) is actually usable as a
/// server address. The user is still responsible for verifying the address
/// assigned/given by their provider.
pub fn is_global(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_global_v4(ip),
        IpAddr::V6(ip) => is_global_v6(ip),
    }
}

// =============================================================
fn is_global_v4(ip: &Ipv4Addr) -> bool {
    if ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_unspecified()
        || ip.is_broadcast()
    {
        return false;
    }

    let [a, b, ..] = ip.octets();

    // CGNAT: 100.64.0.0/10
    if a == 100 && (64..=127).contains(&b) {
        return false;
    }

    // Documentation: 192.0.2.0/24, 198.51.100.0/24, 203.0.113.0/24
    if (a, b) == (192, 0) || (a, b) == (198, 51) || (a, b) == (203, 0) {
        return false;
    }

    // Benchmarking: 198.18.0.0/15
    if a == 198 && (18..=19).contains(&b) {
        return false;
    }

    // Reserved: 240.0.0.0/4
    if a >= 240 {
        return false;
    }

    true
}
// =============================================================
fn is_global_v6(ip: &Ipv6Addr) -> bool {
    // Unspecified, loopback, link-local, multicast and ULA
    if ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_unicast_link_local()
        || ip.is_multicast()
        || ip.is_unique_local()
    {
        return false;
    }

    let segments = ip.segments();
    // Checking for 2001:2::/48 (Benchmarking), 2001:10::/28 (ORCHID), 2001::/32
    // (Teredo)
    if segments[0] == 0x2001 {
        if segments[1] == 0x0002 {
            return false;
        }
        if segments[1] >= 0x0010 && segments[1] <= 0x001f {
            return false;
        }
        if segments[1] == 0x0000 && segments[2] == 0x0000 {
            return false;
        }
    }

    // Documentation
    let is_doc = (segments[0] == 0x2001 && segments[1] == 0x0db8)
        || (segments[0] == 0x3fff && (segments[1] & 0xf000) == 0);
    if is_doc {
        return false;
    };

    (segments[0] & 0xe000) == 0x2000
}
// =============================================================
