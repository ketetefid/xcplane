// SPDX-License-Identifier: GPL-3.0-or-later

use dashmap::DashMap;
use directories::ProjectDirs;
use include_dir::{Dir, include_dir};
use semver::Version;
use std::collections::HashMap;
use std::fs::{self, Permissions, set_permissions};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::AtomicU16};
use tokio::sync::mpsc::{self, Sender};
use tokio_rusqlite::Connection as SqlConn;
use tracing::{debug, error, info, instrument, warn};
use users::get_effective_uid;

use super::{DaemonLock, DaemonPrereq, DaemonRequest, DaemonRuntime, SvcComm};
use crate::ansible::AnsibleAction;
use crate::cli::socket_listener;
use crate::cloud::spawn_task;
use crate::cloudflare::{create_cf_client, test_cf_token};
use crate::constants::{
    ANSIBLE_CFG, ANSIBLE_DIR, CF_AUTH, CLOUD_BACKUP_DIR, CLOUD_CONFIG, CLOUD_DB, CLOUD_EXAMPLE,
    CLOUD_EXAMPLE_NAME, PROXY_START_PORT, XRAY_BIN, XRAY_GEOIP, XRAY_GEOSITE, XUI_BACKUP_DIR,
};
use crate::nft::{create_geoip_db, dl_geoip_data};
use crate::types::{
    BoxError, CFAuth, Daemon, DaemonDirs, DashAction, SvcEntry, TaskEntry, TaskMap, WorkSpace,
    is_proper,
};

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

/// Creates and validates the project directory for storing configs and data
pub fn prepare_workspace() -> Result<Arc<WorkSpace>, BoxError> {
    /*
    If the program is invoked by root, then we only work with system-wide
    directories for reading the servers file or storing data:
    - Reading config files from /etc/xcplane/
    - Storing data in /var/lib/xcplane/
    - Storing cache in /var/cache/xcplane/
    - Storing runtime data in /run/xcplane/
    - Storing logs in /var/log/xcplane/

    If a normal user has invoked the program, we will only look for and store
    data in XDG directories:
    - Reading config files from ~/.config/xcplane/
    - Storing data in ~/.local/share/xcplane/
    - Storing cache in ~/.cache/xcplane/
    - Storing runtime states in /run/user/{uid}/xcplane/
    - Storing logs in ~/.local/state/xcplane/
     */
    let uid = get_effective_uid();

    let dirs = if uid == 0 {
        // As root we use FHS directories suitable for running as a daemon
        DaemonDirs {
            config_dir: PathBuf::from("/etc/xcplane"),
            data_dir: PathBuf::from("/var/lib/xcplane"),
            cache_dir: PathBuf::from("/var/cache/xcplane"),
            runtime_dir: PathBuf::from("/run/xcplane"),
            state_dir: PathBuf::from("/var/log/xcplane"),
        }
    } else {
        // As a user we work with XDG directories
        let proj = ProjectDirs::from("io", "github", "xcplane")
            .ok_or("Couldn't determine XDG directories.")?;
        DaemonDirs {
            config_dir: proj.config_dir().to_path_buf(),
            data_dir: proj.data_dir().to_path_buf(),
            cache_dir: proj.cache_dir().to_path_buf(),
            runtime_dir: proj
                .runtime_dir()
                .ok_or("Couldn't find a runtime directory for the user.")?
                .to_path_buf(),
            state_dir: proj
                .state_dir()
                .ok_or("Couldn't find a state directory for the user.")?
                .to_path_buf(),
        }
    };

    // config_dir is the directory where the required files will be placed, and
    // in the first run, if it doesn't exist, we create all the paths in an
    // idempotent way.
    if !dirs.config_dir.exists() {
        for app_path in [
            &dirs.config_dir,
            &dirs.data_dir,
            &dirs.cache_dir,
            &dirs.runtime_dir,
            &dirs.state_dir,
        ] {
            fs::create_dir_all(app_path)?;
        }

        // Set strict permissions for the config, data and logs dir
        set_permissions(&dirs.config_dir, Permissions::from_mode(0o700))?;
        set_permissions(&dirs.data_dir, Permissions::from_mode(0o700))?;
        set_permissions(&dirs.state_dir, Permissions::from_mode(0o700))?;

        eprintln!("=================================================");
        eprintln!("Initialized xcplane workspace.\n");
        eprintln!("  config:  {}", dirs.config_dir.display());
        eprintln!("  data:    {}", dirs.data_dir.display());
        eprintln!("  cache:   {}", dirs.cache_dir.display());
        eprintln!("  runtime: {}", dirs.runtime_dir.display());
        eprintln!("  log:     {}", dirs.state_dir.display());

        let cloud_config_path = &dirs.config_dir.join(CLOUD_CONFIG);
        // We write the example config for reference
        let example_path = &dirs.config_dir.join(CLOUD_EXAMPLE_NAME);
        fs::write(&example_path, CLOUD_EXAMPLE)?;
        eprintln!(
            "\nA cloud config file at '{}' must be supplied.",
            cloud_config_path.display()
        );
        eprintln!(
            "\nFor your reference, an example cloud config has been written to:\n{}\n",
            example_path.display()
        );

        // We write an empty skeleton to the auth file with proper permission so
        // the user can seamlessly edit it
        let cf_config_skel = CFAuth {
            cloudflare_api_token: "".to_string(),
        };

        let cf_config_str = toml::to_string(&cf_config_skel)?;

        let cf_auth_path = &dirs.config_dir.join(CF_AUTH);
        fs::write(cf_auth_path, &cf_config_str)?;
        set_permissions(cf_auth_path, Permissions::from_mode(0o600))?;

        eprintln!(
            "A skeleton for placing the Cloudflare token has also been created in:\n{}\n\n\
	     The token must be of 'account' type, and it must have permission to edit DNS, \
	     and optionally rulesets and lists (for Cloudflare proxy) according to the following:\n\
	     -----------------------------------------\n\
	     | Zone    | DNS                  | Edit |\n\
	     | Account | Account Rulesets     | Edit |\n\
	     | Account | Account Filter Lists | Edit |\n\
	     -----------------------------------------\n",
            cf_auth_path.display()
        );
    }

    // Set the path for the daemon instance so that both the daemon and later
    // commands sent by CLI know where the socket is.
    let daemon = Daemon::new(&dirs.runtime_dir);

    println!("Initialized workspace.\n");

    Ok(Arc::new(WorkSpace { dirs, daemon }))
}
// =============================================================
/// Creates and validates the project directory for storing configs and data
#[instrument(name = "startup", skip_all)]
pub fn validate_workspace(workspace: &WorkSpace) -> Result<(), BoxError> {
    let DaemonDirs {
        config_dir,
        data_dir,
        cache_dir,
        runtime_dir,
        state_dir,
    } = &workspace.dirs;

    let required_files = [config_dir.join(CLOUD_CONFIG), config_dir.join(CF_AUTH)];

    // Config directory is already created by prepare_workspace. We need to
    // check if the required files have been placed or not. Also, we (re)create
    // the other directories if needed.
    if !data_dir.exists() || !cache_dir.exists() || !runtime_dir.exists() || !state_dir.exists() {
        for app_path in [&data_dir, &cache_dir, &runtime_dir, &state_dir] {
            fs::create_dir_all(app_path)?;
        }
    }

    // Stricter permission for config, data & state dirs
    set_permissions(&config_dir, Permissions::from_mode(0o700))?;
    set_permissions(&data_dir, Permissions::from_mode(0o700))?;
    set_permissions(&state_dir, Permissions::from_mode(0o700))?;

    for item in &required_files {
        match (item.exists(), item == &config_dir.join(CF_AUTH)) {
            (false, _) => {
                error!(item = %item.display(), "missing");
                return Err("Supply the missing file.".into());
            }

            (true, true) => {
                // If the auth file exists, we ensure it has proper permissions
                // and contains proper data
                let metadata = item.metadata()?;
                let pmode = metadata.permissions().mode() & 0o777;
                if pmode != 0o600 {
                    set_permissions(item, Permissions::from_mode(0o600))?;
                }
                let cf_config_str = fs::read_to_string(&item)?;
                let cf_config: CFAuth = toml::from_str(&cf_config_str).inspect_err(|e| {
                    error!(path = %item.display(), error = %e,
			   "failed to read CloudFlare auth token from path");
                })?;

                if cf_config.cloudflare_api_token == "" {
                    return Err(
                        format!("The CloudFlare token in {} is empty.", item.display()).into(),
                    );
                }
            }
            _ => {}
        }
    }

    // We let the check run, and print useful info about Ansible state. However,
    // error returning is disabled here.
    let _ = check_ansible();

    Ok(())
}
// =============================================================
/// Initializes the daemon and gets hold of socket and PID files
#[instrument(name = "startup", skip_all)]
pub async fn acquire_lock(workspace: &WorkSpace) -> Result<DaemonLock, BoxError> {
    // The input workspace has a path-defined daemon and we start it early to
    // bind the socket and prevent from a race condition where several daemons
    // can try to start.
    let daemon = &workspace.daemon;
    // Check daemon prerequisites
    daemon.is_unique().await?;

    // If everything is OK, create the PID file
    daemon.create_pid_file()?;
    // And bind the socket
    let listener = daemon.create_socket()?;

    // Daemon channel which is used to communicate with the running daemon
    let (txd, rxd) = mpsc::channel::<DaemonRequest>(10);

    // The socket listener task. The socket commands received from the CLI
    // process are sent to the daemon through the daemon channel.
    let socket_task = TaskEntry::SocketListener;
    let jh = spawn_task(socket_listener(listener, txd.clone()), socket_task);

    info!("initialized daemon");

    Ok(DaemonLock {
        channel: (txd, rxd),
        socket_jh: jh,
    })
}
// =============================================================
/// All the checks are applied here before the daemon runtime state is
/// constructed
#[instrument(name = "startup", skip_all)]
pub async fn check_prereq(workspace: &WorkSpace) -> Result<DaemonPrereq, BoxError> {
    // Initializing an empty Cloudflare list. If any server requests Cloudflare
    // proxy, this list will later be fetched/created by the daemon.
    let cf_list = None;

    // Create a client with a default header containing the token for Cloudflare
    // 'reqwests'
    let cf_auth_path = workspace.dirs.config_dir.join(CF_AUTH);
    let cf_auth_str = fs::read_to_string(cf_auth_path)?;
    let cf_auth = toml::from_str::<CFAuth>(&cf_auth_str)?;
    let cf_client = create_cf_client(&cf_auth.cloudflare_api_token)?;

    // Check the backup path and warn if they can clash with server folders
    for path in [CLOUD_BACKUP_DIR, XUI_BACKUP_DIR] {
        if is_proper(path) {
            warn!(
                "Name the backup folder '{}' \
		 with a dash to ensure compatibility.",
                path
            );
        }
    }
    // And check the validity of the supplied Cloudflare token
    info!("checking validity of the supplied Cloudflare token");
    test_cf_token(&cf_client).await?;

    // Check if Xray binary and Geo files are installed properly
    info!("checking if Xray is properly installed on the controller");
    check_xray_paths()?;

    // Check if Ansible is installed with the minimum required version
    info!("checking if Ansible is installed on the controller");
    check_ansible()?;

    // 'Including' Ansible sources in the binary
    let ansible_source: Dir<'static> = include_dir!("$CARGO_MANIFEST_DIR/ansible");
    // And extracting to <data_dir>/<ANSIBLE_DIR> if it doesn't exist
    let ansible_dest = workspace.dirs.data_dir.join(ANSIBLE_DIR);
    if !&ansible_dest.exists() {
        info!(path = %ansible_dest.display(), "extracting Ansible sources");
        extract_ansible(&ansible_source, &ansible_dest)?;
    }

    // Validate Ansible action names and their Ansible tasks
    info!("validating Ansible files");
    AnsibleAction::validate(&workspace)?;

    // We always write the minimal, custom ansible config which will be used in
    // all of the calls, so that user or system defined configs will not alter
    // the calls' behavior.
    let ansible_cfg = "[defaults]\n\
		       host_key_checking = False\n\
		       retry_files_enabled = False\n\
		       stdout_callback = quiet_success\n\
		       callback_plugins = ./callback_plugins\n\
		       nocows = 1\n\n\
		       [ssh_connection]\n\
		       ssh_args = \
		       -o BatchMode=yes \
		       -o IdentitiesOnly=yes \
		       -o ControlMaster=auto \
		       -o ControlPersist=600s \
		       -o ConnectTimeout=10 \
		       -o ServerAliveInterval=15 \
		       -o ServerAliveCountMax=3 \
		       -o StrictHostKeyChecking=no \
		       -o UserKnownHostsFile=/dev/null";
    let ansible_cfg_path = workspace.dirs.config_dir.join(ANSIBLE_CFG);
    fs::write(ansible_cfg_path, ansible_cfg)?;

    // The path to the DB
    let cloud_db = workspace.dirs.data_dir.join(CLOUD_DB);

    // Set strict permission on cloud database as it contains Production secrets
    if cloud_db.exists() {
        set_permissions(&cloud_db, Permissions::from_mode(0o600))?;
    }

    // Create the async SQL connection for all DB jobs. For signature functions
    // we use Arc as well to make it clearer they all deref to one DB.
    let sqlconn = Arc::new(SqlConn::open(cloud_db).await?);

    // Downloading the GeoIP data and creating its DB
    info!("creating GeoIP database");
    let geoip_data = dl_geoip_data().await?;
    create_geoip_db(&workspace, geoip_data).await?;
    debug!("GeoIP DB was created");

    let prereq = DaemonPrereq {
        sqlconn,
        cf_list,
        cf_client,
    };

    Ok(prereq)
}
// =============================================================
/// Constructs the runtime data for the daemon
#[instrument(name = "startup")]
pub async fn prepare_runtime() -> Result<DaemonRuntime, BoxError> {
    // A task registry hashmap storing spawned tasks' joinhandles
    let taskmap = TaskMap {
        tasks: HashMap::new(),
    };

    // A channel registry for storing the sender side of the channel for each
    // service monitoring task
    let svc_chat = HashMap::<TaskEntry, Sender<SvcComm>>::new();

    /*
    atomic_port:

    A thread-safe, fast, shared counter. Compared with a mutex of a var,
    AtomicU types don't need any lock and are faster in operations.

    The port will start from proxy_start_port and will be assigned to
    each Xray task throughout the program.
     */
    let atomic_port = Arc::new(AtomicU16::new(PROXY_START_PORT));

    /*
    A multiple producer, single consumer channel for sending the service data
    across threads and receiving all of them in one aggregator task.

    - One aggregator task for all services, which writes to DB at once
    - Multiple worker tasks using the tx clones
    - The original (cloned tx, rx) lives forever
    - If a new server is set up, it will have a cloned tx
    - Transmitted info must hold extra details: (number of tasks, SvcEntry struct)
     */
    let (txa, rxa) = mpsc::channel::<(usize, SvcEntry)>(500);

    /*
    action_map:

    A central shared action registry for production servers as a DashMap which is
    suitable for simultaneous async read & write across threads. It is used to
    help services know the health and fix_try values of each other.

    The dashmap key is comprised of a tuple of servername & the service kind.
    And the dashmap value contains the service health, the JoinHandle
    of a spawned task for the service, and the number of fix tries done to cure
    a Sick service. For now, only the joinhandles of fixing actions in case of an
    error are stored.

    While Xray services have their own data for each inbound, the health
    status of Xray is measured as a whole (all Xray variants belong to the
    same Linux daemon). So if all the variants have good health, then the
    supervisor Xray is marked as Ok in the dashmap.

    In the program, action_map stores the joinhandles of spawned Ansible tasks
    stored in the playbook_map.

    The logic for automated fixing is implemented in handle_response &
    super_xray_action functions.
     */
    let action_map = Arc::new(DashMap::<TaskEntry, DashAction>::new());

    let runtime = DaemonRuntime {
        taskmap,
        svc_chat,
        action_map,
        atomic_port,
        aggregator_ch: (txa, rxa),
    };

    info!("constructed runtime state");
    Ok(runtime)
}
// =============================================================
/// Checks if a recent version of Ansible is installed
fn check_ansible() -> Result<(), BoxError> {
    match std::process::Command::new("ansible")
        .arg("--version")
        .output()
    {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);

            let first_line = stdout.lines().next();

            // Extracting the version handling both modern and old formats:
            // ansible [core 2.19.2]
            // ansible 2.9.1
            let core_version = first_line
                // Try modern format: split at "core ", take the part after, then stop at ']'
                .and_then(|line| line.split("core ").nth(1))
                .and_then(|part| part.split(']').next())
                .map(|v| v.trim().to_string())
                // If modern format yielded None, try legacy format
                .or_else(|| {
                    first_line?
                        .split_whitespace()
                        .find(|s| s.chars().next().map_or(false, |c| c.is_ascii_digit()))
                        .map(|s| s.trim().to_string())
                })
                .ok_or(
                    "Could not parse version from ansible --version output. \
		     Is Ansible properly installed?",
                )?;

            let min_required = Version::parse("2.19.0")?;

            // Parse the version string from Ansible (e.g., "2.19.2")
            // Note: If Ansible outputs "2.19", we might need to append ".0"
            // to make it compliant with Semantic Versioning
            let parsed_version = match Version::parse(&core_version) {
                Ok(v) => v,
                Err(_) => {
                    let padded = format!("{}.0", &core_version);
                    Version::parse(&padded).unwrap_or(Version::new(0, 0, 0))
                }
            };

            if parsed_version < min_required {
                error!(
                    version = core_version,
                    "Ansible version too low, minimum required is 2.19.0"
                );
                return Err("Ansible must be upgraded to at least 2.19.0.".into());
            }
        }
        Err(_) => {
            error!("Ansible is not installed");

            return Err(
                "Ansible is absent from the system. Install a recent version \
		 (ansible-core >= 2.19) according to the official documentation at:\n\
		 https://docs.ansible.com/projects/ansible/latest/installation_guide/index.html"
                    .into(),
            );
        }
    };

    Ok(())
}
// =============================================================
/// Checks if Xray binary and Geo data files are present on the system
fn check_xray_paths() -> Result<(), BoxError> {
    let mut xray_paths_ok = true;

    for xray_item in [XRAY_BIN, XRAY_GEOIP, XRAY_GEOSITE] {
        let item_path = Path::new(xray_item);
        // We first print all the required files at once, then return an error.
        if xray_item.is_empty() || !item_path.exists() {
            error!(item = %item_path.display(),"missing");
            xray_paths_ok = false;
        }
    }

    if !xray_paths_ok {
        return Err(
            "Xray binary and/or its data files are missing from the system. \
	     Compile or download the file(s) from \
	     https://github.com/XTLS/Xray-core"
                .into(),
        );
    }

    for xray_item in [XRAY_BIN, XRAY_GEOIP, XRAY_GEOSITE] {
        // The files must be accessible, too. The reason we check in this way is
        // that xray can start without its geo files, but xcplane needs them.
        if fs::File::open(xray_item).is_err() {
            return Err(format!("The file '{}' is not accessible.", xray_item).into());
        }
    }

    match std::process::Command::new(XRAY_BIN)
        .arg("--version")
        .output()
    {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let first_line = stdout.lines().next();

            let xray_version_str = first_line
                .and_then(|s| s.split_whitespace().nth(1))
                .and_then(|s| Some(s.trim().to_string()))
                .ok_or("Xray doesn't report its version.")?;

            let xray_version = match Version::parse(&xray_version_str) {
                Ok(v) => v,
                Err(_) => {
                    let padded = format!("{}.0", &xray_version_str);
                    Version::parse(&padded).unwrap_or(Version::new(0, 0, 0))
                }
            };

            if xray_version < Version::parse("26.0.0")? {
                error!(version = xray_version_str, "xray >= 26 is required");
                return Err("Minimum required Xray version is 26.".into());
            }
        }

        Err(_) => return Err("Xray is not installed properly.".into()),
    }

    Ok(())
}
// =============================================================
/// Extracts the embedded Ansible files into the destination directory
fn extract_ansible(source: &Dir<'static>, dest: &Path) -> Result<(), BoxError> {
    for entry in source.entries() {
        match entry {
            include_dir::DirEntry::Dir(subdir) => {
                let newdest = dest.join(subdir.path());
                fs::create_dir_all(&newdest)?;
                // Recursively do it for every subdirectory
                extract_ansible(subdir, dest)?;
            }
            include_dir::DirEntry::File(file) => {
                let path = dest.join(file.path());
                if let Some(parent) = path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(path, file.contents())?;
            }
        }
    }

    Ok(())
}
// =============================================================
