// SPDX-License-Identifier: GPL-3.0-or-later

use arc_swap::ArcSwapOption;
use reqwest::Client;
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    net::IpAddr,
    sync::Arc,
};
use strum_macros::AsRefStr;
use tokio::fs;
use tokio::task::JoinSet;
use tracing::{Instrument, Span, info, instrument, warn};
use url::Host;

use crate::{
    ansible::{AnsibleAction, AnsibleConn, AnsibleOutput, AnsibleRun},
    cli::RebaseOpts,
    cloudflare::prepare_cloudflare_state,
    types::{
        BoxError, Cloud, Inbound, InboundKind, KetServer, Secrets, ServerState, SvcKind, WorkSpace,
    },
};

/// Cloud reconciliation modes which imply how the new declarative model in the
/// cloud config file should be applied. The meanings match the variants used in
/// [`create::daemon::DaemonComm`] & [`crate::cli::CliComm`].
#[derive(Clone, PartialEq, Debug, AsRefStr)]
pub enum ReconMode {
    Reload,
    Remap,
    Rebase,
}

/// A struct carrying information in the joinset of Rebase operations
struct JoinSetTask {
    server: Arc<KetServer>,
    task: Result<AnsibleOutput, BoxError>,
}

/// Holds information about added or removed inbounds for a server in the new
/// cloud, which is used in Rebase operations
#[derive(Clone, Debug, Serialize)]
pub struct RebaseData {
    pub added_inbounds: Vec<Inbound>,
    pub removed_inbounds: Vec<Inbound>,
    pub rebase_domain: RebaseDomain,
}

/// Holds server's domain info when its domain or a subdomain of it is updated
/// in a Rebase operation. This data is used for deleting old DNS records, and
/// if a field is recorded as None, it means no change for that subdomain has
/// occurred. This way AnsibleAction::CloudflareDel knows what to delete.
#[derive(Clone, Debug, Serialize)]
pub struct RebaseDomain {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<String>,
}

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

/// Reconciles with the declarative model defined in the cloud config file
/// according to the following:
///
/// - The config file builds the new cloud, and the cloud loaded from DB or from
///   the prior run (DaemonNext) is the old one.
///
/// - Cloud settings such as auto-fix, inbound-set, etc. are always taken from
///   the new cloud.
///
/// - If the old cloud is empty, everything is taken from the new cloud.
///
/// - If a server exists in the new cloud but is missing from the old one, we
///   add it.
///
/// - If a server of old cloud has been deleted from the new model, we delete
///   it, too. If its data is needed for further work, it is better just to keep
///   it in the cloud config but in a disabled state (enabled = false) and then
///   do a *Reload* or *Restart*. A disabled server either Offgrid or Production
///   is always excluded from management and monitoring.
///
/// - If a change is requested for the same server, we can have 3 reconciliation
///   modes: **Reload, Remap, Rebase.**
///   + Enabling or disabling a server works in any mode (enabled = true/false).
///   + A Reload is the default mode during startup which changes the
///     server specs only if it is Offgrid. **A Reload will not change a Production
///     server**. Therefore, once a full setup is done on an Offgrid server and
///     it joins the production cloud, doing a Restart or Reload will not change
///     any of its parameters.
///   + A Remap is forceful change of the server specs. It's suited for the
///     times when the remote server has been altered manually and we need a
///     reflection of those changes in the monitored cloud here.
///   + A Rebase is effectively a Remap + forceful reconciliation of the remote
///     server. If the sever is marked as Offgrid, no remote actions will be done,
///     however, for enabled, provisioned servers (Production state), xcplane will
///     do a reconfiguration to reconcile the remote server with the requested
///     specs. **A Rebase may cause data change/loss on the server**, and the
///     following alterations are supported:
///     * A change in IP (no change in data occurs in the server itself)
///     * A change in domain
///     * A change in SSH port
///     * Changing any subdomain
///     * Changing Cloudflare proxy state
///     * Changing the countries for direct outbound block
///     * For inbounds which are immutable objects, modifying an existing one is
///       not supported and will return an error. However, new inbounds can freely
///       be added to a server, and removing them are allowed via utilizing the
///       forced flag (-f).
///
///   Note that the default mode is always Reload, and performing Remap or Rebase
///   requires an interaction with the running daemon.
///   Another important point is that once a server becomes Production, its
///   secrets such as usernames, passwords, DoH endpoint and subscription paths
///   will remain the same for its entire lifetime, and even Remap or Rebase
///   will not change them. During a Rebase, only the HashMap of mother sub-IDs
///   will be updated to include the added inbounds. The only exception is the
///   Nginx health path which is a derived value and is computed
///   deterministically from the server's domain and its subdomains.
#[instrument(name = "reconcile", skip_all, fields(mode = mode.as_ref()))]
pub async fn reconcile_cloud(
    old: &Cloud,
    new: &Cloud,
    mode: &ReconMode,
    workspace: Arc<WorkSpace>,
    cf_client: &Client,
    cf_list: &mut Option<String>,
    rebase_data_opt: Option<HashMap<String, RebaseData>>,
) -> Result<Cloud, BoxError> {
    if old.servers.is_empty() {
        // If there is no old cloud, we determine the Cloudflare states and
        // return the new cloud directly
        prepare_cloudflare_state(&new, cf_client, cf_list).await?;
        return Ok(new.clone());
    }

    // Domains and IPs must be unique in the merged cloud
    let mut domains_set = HashSet::<Host>::new();
    let mut ips_set = HashSet::<IpAddr>::new();
    old.servers.iter().for_each(|s| {
        domains_set.insert(s.domain.clone());
        ips_set.insert(s.ip.clone());
    });

    // New cloud inbound-set for later comparison is needed. No need to check or
    // add super inbound as it has already been covered when parsing the cloud
    // config.
    let mut new_cloud_inbounds = HashSet::<(&str, u16)>::new();

    for inbound in &new.settings.inbound_set {
        new_cloud_inbounds.insert((&inbound.name, inbound.port));
    }

    let old_map: HashMap<&str, Arc<KetServer>> = old
        .servers
        .iter()
        .map(|s| (s.name.as_str(), s.clone()))
        .collect();

    let new_map: HashMap<&str, Arc<KetServer>> = new
        .servers
        .iter()
        .map(|s| (s.name.as_str(), s.clone()))
        .collect();

    // Combine both maps
    let all_names: HashSet<&str> = old_map.keys().chain(new_map.keys()).copied().collect();

    let all_servers: Vec<(Option<Arc<KetServer>>, Option<Arc<KetServer>>)> = all_names
        .into_iter()
        .map(|name| (old_map.get(name).cloned(), new_map.get(name).cloned()))
        .collect();

    // Collect the servers that will need Rebase actions
    let mut servers_to_rebase = vec![];

    /*
    Since we take global cloud options from the new cloud, inbound checking has
    already been enforced when parsing the config file. However, in Reload mode,
    if a there is a production server in the existing cloud, the new cloud
    inbound-set must be exhaustive enough to contain the existing server's
    inbounds.

    Also when performing a Rebase, we apply every change since the check for
    command line arguments has already been done by the daemon in
    check_and_backup function.
     */
    let mut merged_servers = vec![];

    // First, we perform deletion round explicitly
    for (old_opt, new_opt) in &all_servers {
        match (old_opt, new_opt) {
            (Some(arc_server), None) => {
                domains_set.remove(&arc_server.domain);
                ips_set.remove(&arc_server.ip);

                // Removing the server directory from data dir
                let server_dir = workspace.dirs.data_dir.join(&arc_server.name);
                if server_dir.is_dir() {
                    fs::remove_dir_all(server_dir).await?;
                }

                warn!(server = arc_server.name, "deleted");
            }

            _ => {}
        }
    }

    // Then check for addition/modification
    for (old_opt, new_opt) in all_servers {
        match (old_opt, new_opt) {
            // New has something Old doesn't
            (None, Some(arc_server)) => {
                // Uniqueness check on domains & IPs
                if !domains_set.insert(arc_server.domain.clone()) {
                    return Err(format!(
                        "[ {:?} ] The new server '{}' has the same \
			 domain used in the current cloud.",
                        mode, arc_server.name
                    )
                    .into());
                }

                if !ips_set.insert(arc_server.ip.clone()) {
                    return Err(format!(
                        "[ {:?} ] The new server '{}' \
			 uses an existing IP in the cloud.",
                        mode, arc_server.name
                    )
                    .into());
                }

                warn!(server = arc_server.name, "added");

                merged_servers.push(arc_server);
            }

            // Update the old server according to the reconciliation mode
            (Some(old_arc_server), Some(new_arc_server)) => {
                let updated_server = match mode {
                    ReconMode::Reload => {
                        reload_merge(old_arc_server, new_arc_server, &mut new_cloud_inbounds)?
                    }

                    ReconMode::Remap => remap_merge(old_arc_server, new_arc_server),

                    ReconMode::Rebase => {
                        let arc_merged_server =
                            rebase_merge(old_arc_server.clone(), new_arc_server.clone())?;
                        // Scheduling remote reconciliation
                        if new_arc_server.state.load() == ServerState::Production {
                            servers_to_rebase
                                .push((old_arc_server.clone(), arc_merged_server.clone()));
                        }

                        arc_merged_server
                    }
                };

                merged_servers.push(updated_server);
            }

            // Deletion has already been covered
            (Some(_arc_server), None) => {}

            // This should be unreachable
            (None, None) => return Err("Couldn't merge new & old clouds.".into()),
        }
    }

    let final_cloud = Cloud {
        servers: merged_servers,
        settings: new.settings.clone(),
    };

    // Before returning the cloud, first we need to prepare the Cloudflare state
    // for it, and second perform the Rebase actions if there are any. Deploying
    // Rebase depends on a ready Cloudflare state of servers.
    prepare_cloudflare_state(&final_cloud, cf_client, cf_list).await?;

    if mode == &ReconMode::Rebase {
        let rebase_data_hm = rebase_data_opt.ok_or("RebaseData is empty in a Rebase action.")?;
        deploy_rebase(workspace, servers_to_rebase, rebase_data_hm).await?
    }

    // If everything went well, return the reconciled cloud
    Ok(final_cloud)
}
// =============================================================
/// Performs the necessary reconciliation actions on the remote server for a
/// Rebase
async fn deploy_rebase(
    workspace: Arc<WorkSpace>,
    recon_pairs: Vec<(Arc<KetServer>, Arc<KetServer>)>,
    rebase_data_hm: HashMap<String, RebaseData>,
) -> Result<(), BoxError> {
    let mut rebase_set = JoinSet::new();

    for (old_server, new_server) in recon_pairs {
        let mut ansible_actions = Vec::new();

        if old_server.ip.to_string() != new_server.ip.to_string() {
            ansible_actions.push(AnsibleAction::Cloudflare);
        }

        if old_server.domain.to_string() != new_server.domain.to_string() {
            ansible_actions.append(&mut vec![
                AnsibleAction::DnsReset,
                AnsibleAction::CloudflareDel,
                AnsibleAction::Cloudflare,
                AnsibleAction::Acme,
                AnsibleAction::Nginx,
                AnsibleAction::DoH,
            ]);
        }

        if old_server.dns_subdomain != new_server.dns_subdomain {
            ansible_actions.append(&mut vec![
                AnsibleAction::DnsReset,
                AnsibleAction::CloudflareDel,
                AnsibleAction::Cloudflare,
                AnsibleAction::Nginx,
                AnsibleAction::DoH,
            ]);
        }

        if old_server.ui_subdomain != new_server.ui_subdomain {
            ansible_actions.append(&mut vec![
                AnsibleAction::CloudflareDel,
                AnsibleAction::Cloudflare,
                AnsibleAction::Nginx,
            ]);
        }

        if old_server.sub_subdomain != new_server.sub_subdomain {
            ansible_actions.append(&mut vec![
                AnsibleAction::CloudflareDel,
                AnsibleAction::Cloudflare,
                AnsibleAction::Nginx,
                AnsibleAction::XuiAuth,
                AnsibleAction::PanelSettings,
            ]);
        }

        if old_server.ssh_port != new_server.ssh_port {
            ansible_actions.append(&mut vec![
                AnsibleAction::PortsCheck,
                AnsibleAction::SSH,
                AnsibleAction::Fail2ban,
            ]);
        }

        let old_cfstate = old_server.cfstate.load();
        let old_cfstate = old_cfstate
            .as_ref()
            .ok_or("Couldn't take a reference to the CFState struct.")?;
        let new_cfstate = new_server.cfstate.load();
        let new_cfstate = new_cfstate
            .as_ref()
            .ok_or("Couldn't take a reference to the CFState struct.")?;

        if old_server.cloudflare_proxied != new_server.cloudflare_proxied
            || old_cfstate.cloudflare_only != new_cfstate.cloudflare_only
        {
            ansible_actions.append(&mut vec![
                AnsibleAction::Cloudflare,
                AnsibleAction::Nginx,
                AnsibleAction::Fail2ban,
                AnsibleAction::Firewall,
            ]);
        }

        let old_outbound_block = old_server.outbound_block.iter().collect::<HashSet<_>>();
        let new_oubound_block = new_server.outbound_block.iter().collect::<HashSet<_>>();

        if old_outbound_block != new_oubound_block {
            ansible_actions.append(&mut vec![AnsibleAction::OutblockedUpdate]);
        }

        // Adding/Removing inbounds will be completely applied as the force flag
        // check has already been done in the prior cycle of daemon in
        // check_and_backup. Note that AnsibleAction::DelInbound has a higher
        // priority than AddInbound.
        let rebase_data = rebase_data_hm.get(&old_server.name);

        if let Some(rebase) = rebase_data {
            if !rebase.removed_inbounds.is_empty() {
                // Firewall enforcement is already applied in add/del inbound tasks
                ansible_actions
                    .append(&mut vec![AnsibleAction::XuiAuth, AnsibleAction::DelInbound]);
            }

            if !rebase.added_inbounds.is_empty() {
                ansible_actions.append(&mut vec![
                    AnsibleAction::PortsCheck,
                    AnsibleAction::XuiAuth,
                    AnsibleAction::AddInbound,
                ]);
            }
        }

        // Manually deduplicating and sorting for the sake of logging
        ansible_actions.sort();
        ansible_actions.dedup();

        let current_span = Span::current();

        if !ansible_actions.is_empty() {
            warn!(server = old_server.name, operations = ?ansible_actions, "applying operations");

            let ansible_run = AnsibleRun {
                workspace: workspace.clone(),
                server: new_server.clone(),
                actions: ansible_actions,
                stream_it: true, // The wait is substantial
                rebase_data: rebase_data.cloned(),
            };

            let rebase_task = ansible_run.run();

            rebase_set.spawn(
                async move {
                    JoinSetTask {
                        server: new_server,
                        task: rebase_task.await,
                    }
                }
                .instrument(current_span),
            );
        } else {
            if new_server.enabled {
                info!(
                    server = old_server.name,
                    reason = "no change in primary parameters",
                    "skipping operations"
                );
            } else {
                info!(
                    server = old_server.name,
                    reason = "server is disabled--no update in primary parameters",
                    "skipping operations"
                );
            }
        }
    }

    while let Some(res) = rebase_set.join_next().await {
        let joinset_task = res?;

        if !joinset_task.task?.output.status.success() {
            return Err(format!(
                "Rebase failed because remote action on server '{}' encountered an error.",
                joinset_task.server.name
            )
            .into());
        }

        // We always update SSH port in AnsibleConn for the server after
        // reconciliation
        let old_conn = joinset_task
            .server
            .ansible_conn
            .load_full()
            .ok_or("Couldn't unwrap AnsibleConn in deploy_rebase.")?;

        let updated_conn = AnsibleConn {
            ansible_host: old_conn.ansible_host.clone(),
            ansible_port: Some(joinset_task.server.ssh_port),
            ansible_user: old_conn.ansible_user,
            ansible_connection: None,
        };

        joinset_task
            .server
            .ansible_conn
            .store(Some(Arc::new(updated_conn)));

        info!(server = joinset_task.server.name, "completed successfully");
    }

    Ok(())
}
// =============================================================
/// Compares old cloud with the new model and creates a HashMap of RebaseData
pub fn create_rebase_data(
    curr_cloud: &Cloud,
    new_cloud: &Cloud,
    rebase_opts: &RebaseOpts,
) -> Result<HashMap<String, RebaseData>, BoxError> {
    /*
    In a Rebase, we record old subdomains for deletion from Cloudflare later.

    Also, we must check whether the new cloud has changed any of its servers'
    inbounds or not.

    - Inbounds are considered immutable objects and existing ones must not be
      modified in any case, otherwise we will not proceed.

    - If the change includes adding an inbound, we will proceed with any
      RebaseOpts.

    - If the change involves removing an inbound in any server, the
      Rebase must be invoked with forced option, otherwise we refuse to
      proceed, and we will send a note back that a forced Rebase is
      needed.
     */
    let mut rebase_data_hashmap = HashMap::<String, RebaseData>::new();
    let mut added_inbounds = Vec::<Inbound>::new();
    let mut removed_inbounds = Vec::<Inbound>::new();

    // Storing old DNS, UI & sub subdomains
    let mut rebase_domains_opt = None;

    let mut curr_servers_hm = HashMap::<&str, &KetServer>::new();
    for server in &curr_cloud.servers {
        curr_servers_hm.entry(&server.name).or_insert(&server);
    }

    let mut new_servers_hm = HashMap::<&str, &KetServer>::new();
    for server in &new_cloud.servers {
        new_servers_hm.entry(&server.name).or_insert(&server);
    }

    for (new_name, new_server) in new_servers_hm {
        if let Some(curr_server) = curr_servers_hm.get(new_name) {
            // It means curr_server == new_server, and we identify
            // shared inbounds in the same way for each server
            let mut curr_inbounds_hm = HashMap::<&str, &Inbound>::new();
            for inbound in &curr_server.inbounds {
                curr_inbounds_hm.entry(&inbound.name).or_insert(&inbound);
            }

            let mut new_inbounds_hm = HashMap::<&str, &Inbound>::new();
            for inbound in &new_server.inbounds {
                new_inbounds_hm.entry(&inbound.name).or_insert(&inbound);
            }

            let all_inbound_names = new_inbounds_hm
                .keys()
                .chain(curr_inbounds_hm.keys())
                .copied() // converts && to &
                .collect::<HashSet<&str>>();

            let all_inbounds = all_inbound_names
                .into_iter()
                .map(|name| {
                    (
                        curr_inbounds_hm.get(name).cloned(), // converts Option<&&> to Option<&>
                        new_inbounds_hm.get(name).cloned(),
                    )
                })
                .collect::<Vec<(Option<&Inbound>, Option<&Inbound>)>>();

            added_inbounds = Vec::<Inbound>::new();
            removed_inbounds = Vec::<Inbound>::new();

            for (curr_opt, new_opt) in all_inbounds {
                match (curr_opt, new_opt) {
                    (Some(curr_inb), Some(new_inb)) if *curr_inb == *new_inb => {
                        /*
                        Inbound is unchanged, however, if it is of TLS security
                        kind, neither the server domain nor its sub-subdomain
                        are to change. This is because TLS inbounds depend on
                        current subhostname, and change of subhostname violates
                        their immutability.
                         */
                        match curr_inb.kind {
                            InboundKind::VlessGrpcTls
                            | InboundKind::VlessWsTls
                            | InboundKind::VlessXhttpTls => {
                                if curr_server.subhostname() != new_server.subhostname() {
                                    let errmsg = format!(
                                        "Inbound '{}' in server '{}' is of TLS kind. Servers with TLS \
					 inbounds cannot change their domain or subscription subdomain, \
					 because their TLS inbounds will have to change, too.",
                                        curr_inb.name, curr_server.name
                                    );
                                    return Err(errmsg.into());
                                }
                            }

                            _ => {}
                        }
                    }

                    (Some(_curr_inb), Some(new_inb)) => {
                        // The same inbound was modified while such
                        // operation is not allowed, because inbounds
                        // are defined to be immutable.
                        let errmsg = format!(
                            "Inbounds are immutable, but server '{}' has modified its '{}' \
			     inbound in the new cloud. Rebase will not be performed.",
                            new_name, new_inb.name
                        );

                        return Err(errmsg.into());
                    }

                    (Some(curr_inb), None) => {
                        // Inbound was deleted
                        match rebase_opts {
                            RebaseOpts { forced: true } => {
                                removed_inbounds.push(curr_inb.to_owned());
                            }
                            RebaseOpts { forced: false } => {
                                let errmsg = format!(
                                    "Inbound deletion detected for server '{}'. Rebase must \
				     be invoked with forced (-f) flag if you really intend \
				     to delete an inbound.",
                                    new_name
                                );

                                return Err(errmsg.into());
                            }
                        }
                    }

                    (None, Some(new_inb)) => {
                        // Inbound was added
                        added_inbounds.push(new_inb.to_owned());
                    }

                    (None, None) => {
                        // Should be unreachable
                    }
                }
            }

            /*
            Checking if the domain or a subdomain has changed:
            - if a subdomain has been changed, that subdomain will be recorded
            - if the domain has been altered, all old subdomains will be
              recorded for deletion
             */

            let mut dns_rebase = None;
            let mut ui_rebase = None;
            let mut sub_rebase = None;

            if new_server.dns_subdomain != curr_server.dns_subdomain {
                dns_rebase = Some(curr_server.dnshostname());
            }

            if new_server.ui_subdomain != curr_server.ui_subdomain {
                ui_rebase = Some(curr_server.uihostname());
            }

            if new_server.sub_subdomain != curr_server.sub_subdomain {
                sub_rebase = Some(curr_server.subhostname());
            }

            if new_server.domain != curr_server.domain {
                dns_rebase = Some(curr_server.dnshostname());
                ui_rebase = Some(curr_server.uihostname());
                sub_rebase = Some(curr_server.subhostname());
            }

            // zone is not needed when no change has occurred at all
            let zone_rebase = if dns_rebase.is_none() && ui_rebase.is_none() && sub_rebase.is_none()
            {
                None
            } else {
                let cfstate = curr_server
                    .cfstate
                    .load_full()
                    .ok_or("Couldn't get CFState in create_rebase_data")?;

                Some(cfstate.zone_name.clone())
            };

            rebase_domains_opt = Some(RebaseDomain {
                zone: zone_rebase,
                dns: dns_rebase,
                ui: ui_rebase,
                sub: sub_rebase,
            });
        };

        let rebase_data = RebaseData {
            removed_inbounds: removed_inbounds.clone(),
            added_inbounds: added_inbounds.clone(),
            rebase_domain: rebase_domains_opt
                .clone()
                .ok_or("rebase_domain extraction must not fail.")?,
        };

        rebase_data_hashmap
            .entry(new_name.to_owned())
            .or_insert(rebase_data);
    }

    Ok(rebase_data_hashmap)
}
// =============================================================
fn reload_merge(
    old: Arc<KetServer>,
    new: Arc<KetServer>,
    new_cloud_inbounds: &mut HashSet<(&str, u16)>,
) -> Result<Arc<KetServer>, BoxError> {
    // Changing a server in Reload is done only when the state remains Offgrid
    let super_name = SvcKind::super_name();

    match (&old.state.load(), &new.state.load()) {
        (&ServerState::Offgrid, &ServerState::Offgrid) => Ok(new.clone()),

        // Extra check needed: the new cloud inbound-set must contain all the
        // existing server's inbounds
        (_, _) => {
            let mut server_inbounds = HashSet::<(&str, u16)>::new();

            for inbound in &old.inbounds {
                // Excluding the added super inbound to each server
                if &inbound.name != &super_name {
                    server_inbounds.insert((&inbound.name, inbound.port));
                }
            }

            if !server_inbounds.is_subset(&new_cloud_inbounds) {
                return Err(format!(
                    "[ {:?} ] The existing server '{}' has inbound names that \
		     are not defined in the new cloud inbound-set. You must \
		     add the missing ones. The server's inbounds: {:?}",
                    ReconMode::Reload,
                    old.name,
                    server_inbounds
                )
                .into());
            }

            Ok(Arc::new(KetServer {
                enabled: new.enabled,
                ..(*old).clone()
            }))
        }
    }
}
// =============================================================
fn remap_merge(old: Arc<KetServer>, new: Arc<KetServer>) -> Arc<KetServer> {
    /*
    A Remap is a forceful change of specs. It expects the server to be ready
    exactly as described in the config file. Note that in a Remap we exclude the
    secrets because they are internal parameters which are set during a full
    setup, and we keep them for the server's entire lifetime for consistency.
     */
    Arc::new(KetServer {
        secrets: old.secrets.clone(),
        ..(*new).clone()
    })
}
// =============================================================
fn rebase_merge(old: Arc<KetServer>, new: Arc<KetServer>) -> Result<Arc<KetServer>, BoxError> {
    // A Rebase is a forceful change of specs + forceful change in the remote
    // server if the new state is Production.
    let merged_server = match (new.enabled, old.state.load(), new.state.load()) {
        (true, ServerState::Production, ServerState::Production) => Arc::new(KetServer {
            /*
            In a Rebase, health_path is set from the newly defined data, because
            Rebase can reconfigure system services such as Nginx, and
            health_path is derived from (sub)domains while having a direct role
            in monitoring. Also, if a new inbound is added, the reconciled
            cloud's mother_subids will be updated, too. The other secrets are
            taken from the old state.
             */
            secrets: {
                let mut mother_subids = old.secrets.mother_subids.clone();

                for (inb_name, subid) in &new.secrets.mother_subids {
                    mother_subids
                        .entry(inb_name.clone())
                        .or_insert(subid.clone());
                }

                Secrets {
                    health_path: new.secrets.health_path.clone(),
                    mother_subids,
                    ..old.secrets.clone()
                }
            },

            /*
            For a Rebase, the valid Ansible connection data are the 'new' IP
            along with the 'current' port, because the change in IP is
            considered final while the connection port is still set to the
            current ansible_port, and needs reconciliation.
             */
            ansible_conn: {
                let new_ansible_conn = new
                    .ansible_conn
                    .load_full()
                    .ok_or("Couldn't unwrap new AnsibleConn for a Rebase.")?;

                let old_ansible_conn = old
                    .ansible_conn
                    .load_full()
                    .ok_or("Couldn't unwrap old AnsibleConn for a Rebase.")?;

                let rebased_ansible_conn = AnsibleConn {
                    ansible_host: new_ansible_conn.ansible_host.clone(),
                    ..(*old_ansible_conn).clone()
                };

                ArcSwapOption::from_pointee(rebased_ansible_conn)
            },

            /*
            The rest of the fields are taken from the new state. We will init
            both Xui and Cloudflare struct to an empty state from the new
            server. The xui client will be updated in the daemon via performing
            new_client() on each server, and the Cloudflare state will be
            populated later in this function.
             */
            ..(*new).clone()
        }),

        // We can't perform any remote action if the server is disabled.
        // Therefore, the specs shouldn't change either. If the user
        // wants to forcefully change specs, there is Remap (which doesn't
        // perform remote reconciliation).
        (false, ServerState::Production, ServerState::Production) => Arc::new(KetServer {
            enabled: false,
            ..(*old).clone()
        }),

        // This is just a Remap (Offgrid doesn't need remote reconciliation)
        (_, _, ServerState::Offgrid) => remap_merge(old, new),

        // This is an illegal operation, user should first provision the server
        (_, ServerState::Offgrid, ServerState::Production) => {
            return Err(format!(
                "'{}' is not provisioned yet. Rebase to Production is invalid.",
                old.name
            )
            .into());
        }
    };

    Ok(merged_server)
}
// =============================================================
