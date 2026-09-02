// SPDX-License-Identifier: GPL-3.0-or-later

use arc_swap::ArcSwapOption;
use chrono::{DateTime, Utc};
use isocountry::CountryCode;
use reqwest::Client;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::IpAddr;
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{ExitStatus, Output};
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock, atomic::AtomicU64};
use strum_macros::{AsRefStr, Display, EnumString};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use url::{Host, Url};

/// An error type to make result propagation in async blocks possible
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

use crate::ansible::{AnsibleConn, AnsibleOutput};
use crate::cloudflare::CFState;
use crate::constants::{
    default_dns_subdomain, default_mon_interval, default_outbound_block, default_ssh_port,
    default_sub_subdomain, default_threshold, default_true, default_ui_subdomain,
};

///////////////////////////////////////////////////////////////////////////////////
///////////////////////////// Daemon related types ////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////
/// Workspace holds information about working directories and daemon files.
pub struct WorkSpace {
    pub dirs: DaemonDirs,
    pub daemon: Daemon,
}

/// A struct to hold information about where to look for config files and where
/// to store data. If a normal user invoked the program, this would be populated
/// with XDG directories, otherwise, FHS directories will be used here.
pub struct DaemonDirs {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub runtime_dir: PathBuf,
    pub state_dir: PathBuf,
}

/// Daemon struct holding PID & socket paths
pub struct Daemon {
    pub pid_file: PathBuf,
    pub sock_file: PathBuf,
}

pub struct TrackedTask {
    pub kind: TaskEntry,
    pub task: TaskHandle,
}

#[derive(Debug, Clone, Eq, Hash, PartialEq, AsRefStr, Display)]
pub enum TaskEntry {
    SshMonitor(Arc<SvcInfo>),
    NginxMonitor(Arc<SvcInfo>),
    XrayMonitor(Arc<SvcInfo>),
    SocketListener,
    SignalWatcher,
    Aggregator,
    FullSetup(Arc<KetServer>),
    DestroyServer(Arc<KetServer>),
    XuiDbPruner(Arc<KetServer>),
    AcmeChecker(Arc<KetServer>),
    GeoipUpdater,
}

pub enum TaskHandle {
    Detached(JoinHandle<Result<(), BoxError>>),
    Outputter(JoinHandle<Result<Output, BoxError>>),
}

impl TaskHandle {
    pub fn is_finished(&self) -> bool {
        match self {
            TaskHandle::Detached(jh) => jh.is_finished(),
            TaskHandle::Outputter(jh) => jh.is_finished(),
        }
    }

    pub fn abort(&self) {
        match self {
            TaskHandle::Detached(jh) => jh.abort(),
            TaskHandle::Outputter(jh) => jh.abort(),
        }
    }
}
/// A task registry HashMap storing task entries and their handles
pub struct TaskMap {
    pub tasks: HashMap<TaskEntry, TaskHandle>,
}

///////////////////////////////////////////////////////////////////////////////////
///////////////////////////// Server related types ////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////
/// The cloud that xcplane manages and monitors
#[derive(Debug, Clone, Serialize)]
pub struct Cloud {
    pub servers: Vec<Arc<KetServer>>,
    pub settings: CloudSettings,
}

/// Global options of the cloud
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct CloudSettings {
    /// The set of inbounds that will be used as a reference from which each
    /// server must choose its own ones. If a server is defined with empty
    /// inbounds, this set of inbounds will automatically be picked.
    pub inbound_set: Vec<Inbound>,

    /// Whether the cloud should apply automated fix actions or not. (default:
    /// true)
    #[serde(default = "default_true")]
    pub auto_fix: bool,

    /// How many consecutive failures a service should encounter until automated
    /// fix actions start to be applied. (default: FIX_THRESHOLD)
    #[serde(default = "default_threshold")]
    pub fix_threshold: u64,

    /// How many seconds should pass between monitoring attempts. (default:
    /// SVC_MON_INTERVAL)
    #[serde(default = "default_mon_interval")]
    pub monitor_interval: u64,
}

/// The struct to hold the information about a server
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct KetServer {
    /// An arbitrary, unique name for the server. Servers become distinguishable
    /// by their names.
    pub name: String,

    /// The server's public IPv4 or IPv6 address
    pub ip: IpAddr,

    /// The base domain that will be used for services on this server
    #[serde(deserialize_with = "KetServer::deserialize_host")]
    #[serde(serialize_with = "KetServer::serialize_host")]
    pub domain: Host,

    /// The two-letter country code where the server resides
    #[serde(default = "KetServer::default_country_code")]
    pub region: CountryCode,

    /// Is the server in the state of 'Offgrid' and has not joined the cloud? Or
    /// it has been provisioned and has become 'Production'? Refer to [`ServerState`]
    #[serde(default)]
    pub state: AtomicServerState,

    /// The monthly traffic quota measured in TeraBytes. 0 means unlimited.
    #[serde(default)]
    pub quota: u16,

    /// Is this server available for management and monitoring?
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// The 'intended' SSH port which will be set during a full setup, or later
    /// in a relevant Rebase action.
    #[serde(default = "default_ssh_port")]
    pub ssh_port: u16,

    /// The subdomain at which the DNS-over-HTTPS server will be set up
    #[serde(default = "default_dns_subdomain")]
    pub dns_subdomain: String,

    /// The subdomain at which the UI server will be accessible
    #[serde(default = "default_ui_subdomain")]
    pub ui_subdomain: String,

    /// The subdomain which will serve as the subscription server
    #[serde(default = "default_sub_subdomain")]
    pub sub_subdomain: String,

    /// Whether we should use Cloudflare proxy in DNS and connections (default)
    /// or not. If not, Cloudflare is still used for DNS management, however,
    /// the orange proxy for DNS will not be utilized and network connections
    /// will not go through Cloudflare anymore.
    #[serde(default)]
    pub cloudflare_proxied: bool,

    /// The Xray inbounds this server will have. At least one inbound must be
    /// present either manually or taken from the cloud's inbound-set.
    #[serde(default)]
    pub inbounds: Vec<Inbound>,

    /// The list of countries to which new outbound connections will be
    /// blocked. The OUTPUT connection where it is RELATED or ESTABLISHED will
    /// still be allowed, which means users from these countries can freely
    /// connect to the servers, however, newly initiated connections will not be
    /// allowed. This is used when domestic traffic should not be routed through
    /// the cloud, or very useful in regions whose governments are actively
    /// censoring the information across the internet. (default: CN, IR, RU)
    #[serde(default = "default_outbound_block")]
    pub outbound_block: Vec<CountryCode>,

    /// Holds connection information for performing Ansible actions
    #[serde(skip)]
    pub ansible_conn: ArcSwapOption<AnsibleConn>,

    /// The group of secrets used in this server which includes
    /// username/passwords and random URL paths
    #[serde(skip)]
    pub secrets: Secrets,

    /// The Cloudflare set of data for DNS & connection proxy management
    #[serde(skip)]
    pub cfstate: ArcSwapOption<CFState>,

    /// The struct containing xui panel login info for the server
    #[serde(skip)]
    pub xui: ArcSwapOption<Xui>,

    /// A mutex to prevent simultaneous X-UI panel login actions across
    /// threads. It allows one function or task to acquire the login cookie, and
    /// the others will only use the existing, refreshed one.
    #[serde(skip)]
    pub login_gate: Arc<Mutex<()>>,
}

////////////////////////////////////////////////////////////////////////////////

impl PartialEq for KetServer {
    fn eq(&self, other: &Self) -> bool {
        // Servers are distinguishable by their names
        self.name == other.name
    }
}

impl Eq for KetServer {}

impl Hash for KetServer {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

/// A server's state can have two variants: Production & Offgrid. It shows
/// whether the server is ready to serve in the cloud, or has not been prepared
/// yet.
#[repr(u8)]
#[derive(EnumString, Display, AsRefStr, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ServerState {
    #[default]
    Offgrid = 0,
    Production = 1,
}

/// A mutable struct in Arc that is used to change the server state during runtime
#[derive(Default, Debug)]
pub struct AtomicServerState(AtomicU8);

impl Clone for AtomicServerState {
    fn clone(&self) -> Self {
        Self::new(self.load())
    }
}

impl AtomicServerState {
    pub fn new(state: ServerState) -> Self {
        Self(AtomicU8::new(state as u8))
    }

    pub fn load(&self) -> ServerState {
        match self.0.load(Ordering::Relaxed) {
            0 => ServerState::Offgrid,
            1 => ServerState::Production,
            _ => unreachable!("Invalid ServerState"),
        }
    }

    pub fn store(&self, state: ServerState) {
        self.0.store(state as u8, Ordering::Relaxed);
    }
}

impl<'de> Deserialize<'de> for AtomicServerState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let state = ServerState::deserialize(deserializer)?;
        Ok(Self::new(state))
    }
}

impl Serialize for AtomicServerState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.load().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ServerState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?.trim().to_lowercase();
        match s.as_str() {
            "production" => Ok(ServerState::Production),
            "offgrid" => Ok(ServerState::Offgrid),
            _ => Err(serde::de::Error::custom(format!(
                "Invalid server state: {}",
                s
            ))),
        }
    }
}

impl Serialize for ServerState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Secrets {
    /// The path set on the server used for checking Nginx health. This is
    /// a derived & deterministic value, and will only change if the server's domain
    /// or subdomains change.
    pub health_path: String,

    /// The DoH endpoint on the server. This path is generated randomly during
    /// full setup.
    pub doh_endpoint: String,

    /// 3XUI panel username
    pub xui_username: String,

    /// 3XUI panel password
    pub xui_password: String,

    /// 3XUI API token
    #[serde(skip_serializing_if = "XuiToken::is_none")]
    pub xui_token: XuiToken,

    /// The random path at which 3XUI panel is accessible
    pub xui_webpath: String,

    /// The random path for sublinks in the subscription server
    pub xui_subpath: String,

    /// The random path for json sublinks in the subscription server
    pub xui_jsubpath: String,

    /// Stores the mother inbound subscription ID for each inbound of the server
    pub mother_subids: HashMap<String, String>,
}

/// A struct holding the 3XUI API token which will be initialized during a full
/// setup
#[derive(Debug, Default, Clone)]
pub struct XuiToken(pub OnceLock<String>);

impl Serialize for XuiToken {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0.get() {
            Some(value) => serializer.serialize_str(value),
            None => serializer.serialize_none(),
        }
    }
}

impl XuiToken {
    pub fn is_none(&self) -> bool {
        match self.0.get() {
            None => true,
            _ => false,
        }
    }

    pub fn empty() -> Self {
        Self(OnceLock::new())
    }

    pub fn from(value: String) -> Self {
        Self(OnceLock::from(value))
    }
}

/// A struct holding the information about an Xray inbound. Once a server
/// reaches "Production" state, Ansible will have created the mother inbounds
/// and the corresponding number of clients for it. **An inbound is immutable
/// once created**.
#[derive(Deserialize, Serialize, Clone, Hash, Eq, PartialEq)]
pub struct Inbound {
    /// The unique name/remark for the inbound
    pub name: String,

    /// The port this inbound will serve on
    pub port: u16,

    /// There are 5 inbound kinds to choose from
    #[serde(default)]
    pub kind: InboundKind,

    /// Total number of clients this inbound will serve
    pub total: u16,

    /// Traffic allowance in GB each client will have. A value of 0 indicates
    /// infinite traffic.
    #[serde(default)]
    pub traffic: u32,

    /// After the first use, how many days the client will have until expiry. A
    /// value of 0 indicates no expiry.
    #[serde(default)]
    pub expiry: u16,

    /// A note about this inbound which will be advertised to the client
    #[serde(default)]
    pub comment: String,
}

/// The kind of inbound that will be created on the server. Each variant
/// indicates ProtocolTransmissionSecurity. xcplane supports five deployment
/// profiles that have been selected for their maturity, effective censorship
/// resistance, and long-term maintainability. Advanced Xray transport
/// combinations are intentionally not exposed because the Rust app wants to
/// guarantee the correctness of the profiles it manages.
#[derive(
    EnumString,
    Display,
    AsRefStr,
    Debug,
    Clone,
    PartialEq,
    Default,
    Deserialize,
    Serialize,
    Hash,
    Eq,
)]
pub enum InboundKind {
    #[default]
    VlessXhttpReality, // for direct connection, Cloudflare incompatible
    VlessTcpReality, //  for direct connection, Cloudflare incompatible
    VlessXhttpTls,   // for both direct and Cloudflare proxied connections
    VlessWsTls,      // for both direct and Cloudflare proxied connections
    VlessGrpcTls,    // for both direct and Cloudflare proxied connections in HTTP/2 environments
}

impl InboundKind {
    /// Is the Inbound kind compatible for connection proxy through Cloudflare?
    pub fn cloudflare_compatible(&self) -> bool {
        match self {
            InboundKind::VlessXhttpReality => false,
            InboundKind::VlessTcpReality => false,
            InboundKind::VlessXhttpTls => true,
            InboundKind::VlessWsTls => true,
            InboundKind::VlessGrpcTls => true,
        }
    }
}

impl Inbound {
    /// A function that creates a representative of the supervisor Xray. The
    /// supervisor doesn't have any clients or sublink, and is only used to
    /// track the status of the Xray/x-ui daemon as a whole, and apply fix
    /// actions to it.
    pub fn super_inbound() -> Self {
        Inbound {
            name: SvcKind::super_name(),
            port: 0,
            kind: InboundKind::VlessTcpReality,
            total: 0,
            traffic: 0,
            expiry: 0,
            comment: String::from("The super inbound"),
        }
    }

    pub fn get_jsub(&self, server: &KetServer) -> Result<Url, BoxError> {
        let mother_subid = server.secrets.mother_subids.get(&self.name).ok_or(format!(
            "Mother sub-ID is absent from the secrets of server '{}' for inbound '{}'",
            server.name, self.name
        ))?;

        let mother_jsub_str = format!(
            "https://{}/{}/{}",
            server.subhostname(),
            server.secrets.xui_jsubpath,
            mother_subid
        );

        Ok(Url::parse(&mother_jsub_str)?)
    }
}

/// Holds credentials specifically for the 3x-ui panel login. Once a server
/// reaches the state of "Production", this struct will fully be
/// initialized. While a KetServer itself generates most of the credentials and
/// random paths, xui_token is only created during 3x-ui installation.
#[derive(Debug, Deserialize)]
pub struct Xui {
    /// The full web address to the 3XUI panel
    pub xui_url_global: Url,

    pub xui_username: String,
    pub xui_password: String,
    pub xui_token: String,

    /// A simple client used for header-based authorization
    #[serde(skip)]
    pub lightclient: Client,

    /// A full client that stores cookies and is used for full panel login
    #[serde(skip)]
    pub client: Client,

    /// An atomic counter to effectively track refreshed logins and to only
    /// allow one request to update the cookie in the jar of client
    #[serde(skip)]
    pub cookie_count: AtomicU64,
}

/// A struct holding CloudFlare API token for using later in Ansible calls
#[derive(Debug, Deserialize, Serialize)]
pub struct CFAuth {
    pub cloudflare_api_token: String,
}

impl Clone for KetServer {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            ip: self.ip.clone(),
            domain: self.domain.clone(),
            region: self.region.clone(),
            state: self.state.clone(),
            quota: self.quota,
            enabled: self.enabled,
            ssh_port: self.ssh_port,
            dns_subdomain: self.dns_subdomain.clone(),
            ui_subdomain: self.ui_subdomain.clone(),
            sub_subdomain: self.sub_subdomain.clone(),
            secrets: self.secrets.clone(),
            cloudflare_proxied: self.cloudflare_proxied,
            ansible_conn: ArcSwapOption::from(self.ansible_conn.load_full()),
            inbounds: self.inbounds.clone(),
            outbound_block: self.outbound_block.clone(),
            cfstate: ArcSwapOption::from(self.cfstate.load_full()),
            xui: ArcSwapOption::from(self.xui.load_full()),
            login_gate: self.login_gate.clone(),
        }
    }
}

impl fmt::Display for KetServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, " ----------------- KetServer -----------------")?;
        writeln!(f, "                name: {}", self.name)?;
        writeln!(f, "                  IP: {}", self.ip)?;
        writeln!(f, "              domain: {}", self.domain.to_string())?;
        writeln!(f, "              region: {}", self.region)?;
        writeln!(f, "               state: {}", self.state.load())?;
        writeln!(f, "               quota: {}", self.quota)?;
        writeln!(f, "             enabled: {}", self.enabled)?;
        writeln!(f, "            ssh_port: {}", self.ssh_port)?;
        writeln!(f, "       dns_subdomain: {}", self.dns_subdomain)?;
        writeln!(f, "        ui_subdomain: {}", self.ui_subdomain)?;
        writeln!(f, "       sub_subdomain: {}", self.sub_subdomain)?;
        writeln!(f, "  cloudflare_proxied: {}", self.cloudflare_proxied)?;
        writeln!(f, "      outbound_block: {:?}", self.outbound_block)?;
        let debug = format!("{:#?}", self.inbounds);

        writeln!(f, "            inbounds:")?;
        for line in debug.lines() {
            writeln!(f, "                  {}", line)?;
        }
        writeln!(f, " ---------------------------------------------")
    }
}

impl fmt::Debug for KetServer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self, f)
    }
}

impl fmt::Debug for Inbound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inbound")
            .field("name", &self.name)
            .field("port", &self.port)
            .field("kind", &self.kind)
            .field("total", &self.total)
            .field("traffic", &self.traffic)
            .field("expiry", &self.expiry)
            .field("comment", &self.comment)
            .finish()
    }
}

///////////////////////////////////////////////////////////////////////////////////
//////////////////////////// Service related types ////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////

/// A struct holding both general information & status details about a service
#[derive(Debug, Clone)]
pub struct SvcEntry {
    pub info: Arc<SvcInfo>,
    pub status: SvcStatus,
}

/// An error struct to hold service information when returning from
/// service-checking functions
#[derive(Debug)]
pub struct SvcError {
    pub service: SvcEntry,
    pub errmsg: BoxError,
}

/// A struct to hold details excluding live status about a service on a server
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct SvcInfo {
    /// The KetServer this service is operating on
    pub server: Arc<KetServer>,

    /// The port is either used directly for SSH & Nginx, or is a port at which
    /// Xray testing will be done. We will assign a random port from a pool and
    /// a socks5h service will be created using the mother json sublink of each
    /// inbound through which we will test the Xray service connectivity.
    pub port: u16,

    /// Nginx or SSH or Xray(inbound name)
    pub kind: SvcKind,

    /// The mother json sublink for socks5h creation. This is only used for Xray
    /// checking, and the other services don't have or need a link.
    pub link: Option<Url>,
}

/// The service status struct which shows the health of a service, how many
/// intervals it has failed and since when, and the fix level we have tried.
#[derive(Debug, Clone)]
pub struct SvcStatus {
    /// Sick or Ok or Unknown
    pub health: SvcHealth,

    /// How many intervals the service has failed consequently
    pub failed_count: u64,

    /// The time of the first failure incident stored as seconds which have
    /// elapsed since UNIX_EPOCH
    pub failed_since: u64,

    /// The fix level we have tried and completed for the service if it were
    /// sick. The level of fix is different for each service, and each
    /// level corresponds to a spawned task aiming to try an automated fix.
    ///
    /// 0 means no fix has been "completed" yet; 1 means fix level 1 has been
    /// applied, and so on. The next fix action is performed only if a prior
    /// attempt has been made and has failed.
    ///
    /// - SSH failure needs manual intervention and we can't do anything.
    ///
    /// - For Nginx, at level 1 we have restarted it. If that hasn't worked, at
    ///   level 2 we will have tried to recreate config files.
    ///
    /// - For Xray, level 1 fix means we will try a restart. At Level 2 we
    ///   will restore the latest backed up DB to see if it can come to the
    ///   rescue.
    pub fix_try: u8,
}

/// The enum of service kinds to be monitored
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum SvcKind {
    Ssh,
    Nginx,
    /// The Xray service kind holds an inbound name
    Xray(String),
}

/// The health enum for a service
#[derive(EnumString, Display, AsRefStr, Debug, Clone, PartialEq, Serialize)]
pub enum SvcHealth {
    /// We know the service is running fine
    Ok,

    /// We know the service is not running fine
    Sick,

    /// We exactly don't know the service status or we haven't checked its
    /// health yet
    Unknown,
}

// To be able to print SvcError seamlessly
impl fmt::Display for SvcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match DateTime::<Utc>::from_timestamp(self.service.status.failed_since as i64, 0) {
            Some(since) => match &self.service.info.kind {
                SvcKind::Ssh => write!(
                    f,
                    "❌[ Monitoring ][ {} ][ {} ] ({}:{}) has failed \
		     {} times since {}. Error: {}",
                    self.service.info.kind.nice_display(),
                    self.service.info.server.name,
                    self.service.info.server.ip,
                    self.service.info.port,
                    self.service.status.failed_count,
                    since.format("%Y-%m-%d %H:%M:%S"),
                    self.errmsg
                ),
                SvcKind::Nginx => write!(
                    f,
                    "❌[ Monitoring ][ {} ][ {} ] ({}:{}) has failed \
		     {} times since {}. Error: {}",
                    self.service.info.kind.nice_display(),
                    self.service.info.server.name,
                    self.service.info.server.uihostname(),
                    self.service.info.port,
                    self.service.status.failed_count,
                    since.format("%Y-%m-%d %H:%M:%S"),
                    self.errmsg
                ),
                SvcKind::Xray(inbound) => {
                    let svc_inbound_opt = self
                        .service
                        .info
                        .server
                        .inbounds
                        .iter()
                        .find(|inb| &inb.name == inbound);
                    if let Some(svc_inbound) = svc_inbound_opt {
                        write!(
                            f,
                            "❌[ Monitoring ][ {} ][ {} ] ({}:{}) has failed \
			     {} times since {}. Error: {}",
                            self.service.info.kind.nice_display(),
                            self.service.info.server.name,
                            self.service.info.server.subhostname(),
                            svc_inbound.port,
                            self.service.status.failed_count,
                            since.format("%Y-%m-%d %H:%M:%S"),
                            self.errmsg
                        )
                    } else {
                        write!(f, "The inbound can't be found in the server.")
                    }
                }
            },
            None => write!(
                f,
                "Couldn't convert failed_since to a human-readable format."
            ),
        }
    }
}

// Needed when returning it as an Err variant
impl Error for SvcError {}

// To make {} formatting and to_string work for SvcKind enum
impl fmt::Display for SvcKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SvcKind::Ssh => write!(f, "ssh"),
            SvcKind::Nginx => write!(f, "nginx"),
            SvcKind::Xray(inb) => write!(f, "xray_{}", inb),
        }
    }
}

impl SvcKind {
    /// The supervisor Xray name. It is used as the single source of truth.
    pub fn super_name() -> String {
        String::from("super")
    }

    /// The reserved SvcKind for the supervisor Xray
    pub fn super_xray() -> Self {
        SvcKind::Xray(Self::super_name())
    }

    /// Only variant names
    pub fn display(&self) -> String {
        match self {
            SvcKind::Ssh => String::from("SSH"),
            SvcKind::Nginx => String::from("Nginx"),
            SvcKind::Xray(_) => String::from("Xray"),
        }
    }

    /// A more canonical display
    pub fn nice_display(&self) -> String {
        match self {
            SvcKind::Ssh => String::from("SSH"),
            SvcKind::Nginx => String::from("Nginx"),
            SvcKind::Xray(inb) => format!("Xray({})", inb),
        }
    }
}

impl std::str::FromStr for SvcKind {
    type Err = ();

    /// To safely convert services from string back to enum
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "ssh" {
            return Ok(SvcKind::Ssh);
        }
        if s == "nginx" {
            return Ok(SvcKind::Nginx);
        }

        if let Some(name) = s.strip_prefix("xray_") {
            return Ok(SvcKind::Xray(name.to_string()));
        }

        Err(())
    }
}

/// A struct holding the service status and the fix's joinhandle. It is to be
/// used in the DashMap registry of fix actions as the value.
#[derive(Debug)]
pub struct DashAction {
    pub svc_health: SvcHealth,
    pub fix_try: u8,
    pub fix_action: Option<JoinHandle<Result<AnsibleOutput, BoxError>>>,
}

impl DashAction {
    /// Initialized value for DashAction
    pub fn default() -> Self {
        Self {
            svc_health: SvcHealth::Unknown,
            fix_try: 0,
            fix_action: None,
        }
    }
}

/// Creates a synthetic output to be used when the caller doesn't return
/// Std::process::Output and just ()
pub fn ok_output() -> Output {
    Output {
        status: ExitStatus::from_raw(0),
        stdout: Vec::new(),
        stderr: Vec::new(),
    }
}

// Trait grouping
pub trait AsyncStream: AsyncRead + AsyncWrite {}
impl<T> AsyncStream for T where T: AsyncRead + AsyncWrite {}

/// Checks if the given string for DNS, UI and subscription subdomains meets the
/// criteria: it must be in lowercase; must start and end with an ASCII
/// alphanumeric character, and only hyphen is allowed in the middle. The
/// maximum length is limited to 63, too.
pub fn is_proper_subdomain(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

/// A function to check if a string chosen as an ID or name meets proper naming:
/// it must start & end with an ASCII alphanumeric character, and only
/// underscore is allowed in the middle. A companion slug function has been made
/// to suggest proper naming. Dash/hyphen is not allowed as it will cause
/// problems with Ansible.
pub fn is_proper(name: &str) -> bool {
    !name.is_empty()
        && name.starts_with(|c: char| c.is_ascii_alphanumeric())
        && name.ends_with(|c: char| c.is_ascii_alphanumeric())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A trait for creating pure ASCI alphanumeric string IDs
pub trait Slugged {
    fn slug(&self) -> String;
}

impl Slugged for str {
    /// Suggests a proper ID for the str
    fn slug(&self) -> String {
        let s = self.trim();

        if s.is_empty() {
            return "good_nickname1".to_string();
        }

        let sl = s.len();

        s.char_indices()
            .map(
                // char_indices position is the byte position, and UTF‑8
                // characters can be multiple bytes.
                |(p, c)| match (p == 0, p + c.len_utf8() == sl, c.is_ascii_alphanumeric()) {
                    (_, _, true) => c,
                    (true, _, false) => 'a',
                    (_, true, false) => 'a',
                    _ => '_',
                },
            )
            .collect()
    }
}

impl Slugged for String {
    fn slug(&self) -> String {
        self.as_str().slug()
    }
}
