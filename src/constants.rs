// SPDX-License-Identifier: GPL-3.0-or-later

////////////////// The ports that will be used on each server //////////////////
/// The default SSH port which will be used for server setup and SSH checking
pub const SSH_PORT: u16 = 79;

/// The default https web server port used for Nginx setup and checking. It must
/// not be changed as it is the TLS web port.
pub const NGINX_PORT: u16 = 443;

/// The default internal unbound DoH port. Normally, it's not needed to be
/// changed, as Nginx will reverse-proxy it.
pub const UNBOUND_PORT: u16 = 444;

/// The default internal port for 3x-ui web. Normally, it's not needed to be
/// changed, as Nginx will reverse-proxy it.
pub const UI_PORT: u16 = 400;

/// The default internal port for 3x-ui subscription service. Normally, it's not
/// needed to be changed, as Nginx will reverse-proxy it.
pub const SUB_PORT: u16 = 401;

/// Used for setting defaults in serde
pub const fn default_ssh_port() -> u16 {
    SSH_PORT
}

/// The default Ansible port for unprovisioned (Offgrid) servers which is
/// normally expected to be 22.
pub const ANSIBLE_PORT: u16 = 22;

/// The default list of countries to which new outbound connections will be blocked.
pub fn default_outbound_block() -> Vec<isocountry::CountryCode> {
    vec![
        isocountry::CountryCode::CHN,
        isocountry::CountryCode::IRN,
        isocountry::CountryCode::RUS,
    ]
}
////////////// Subdomains used for DNS, UI and subscription server /////////////
pub const DNS_SUBDOMAIN: &str = "dns";
pub const UI_SUBDOMAIN: &str = "ui";
pub const SUB_SUBDOMAIN: &str = "sub";

pub fn default_dns_subdomain() -> String {
    String::from(DNS_SUBDOMAIN)
}

pub fn default_ui_subdomain() -> String {
    String::from(UI_SUBDOMAIN)
}

pub fn default_sub_subdomain() -> String {
    String::from(SUB_SUBDOMAIN)
}

////////////////////////////////////////////////////////////////////////////////
/// The user agent that will be used in all contacts
pub const XCPLANE_AGENT: &str = concat!("xcplane/", env!("CARGO_PKG_VERSION"));

pub fn default_true() -> bool {
    true
}

/// The remote Ansible user is root
pub const ROOT_USER: &str = "root";

/// The path to Xray binary which will be used in testing inbounds
pub const XRAY_BIN: &str = "/usr/local/bin/xray";

/// The path to GeoIP data file which is a component of Xray program
pub const XRAY_GEOIP: &str = "/usr/local/bin/geoip.dat";

/// The path to GeoSite data file which is a component of Xray program
pub const XRAY_GEOSITE: &str = "/usr/local/bin/geosite.dat";

/// The starting port number which will be assigned for Xray testing
/// upward. These ports just need to be free for localhost and shouldn't be
/// opened to public.
pub const PROXY_START_PORT: u16 = 10_000;

/// The socks address if testing SSH & Nginx services needs to be done through
/// such proxy. This has nothing to do with testing Xray services which will
/// create their own socks5h proxies.
pub const SOCKS_ADDR: &str = "127.0.0.1:10808";

/// An array of text-returning “what is my IPv4” endpoints
pub const IPV4_ENDPOINTS: [&str; 4] = [
    "https://api.ipify.org",
    "https://v4.ident.me",
    "https://ipv4.icanhazip.com",
    "https://whatismyip.akamai.com",
];

/// An array of text-returning “what is my IPv6” endpoints
pub const IPV6_ENDPOINTS: [&str; IPV4_ENDPOINTS.len()] = [
    "https://api6.ipify.org",
    "https://v6.ident.me",
    "https://ipv6.icanhazip.com",
    "https://whatismyip.akamai.com",
];

/// Cloudflare secure ports. If a server's inbound ports are a subset of these,
/// then Cloudflare-only mode will be activated for that server, which means
/// only Cloudflare traffic to the whole server will be allowed.
pub const CLOUDFLARE_SPORTS: [u16; 5] = [2053, 2083, 2087, 2096, 8443];

/// The directory where the Ansible files/tasks/playbooks are stored. We will
/// use program version as a sub-directory for storing versioned, up-to-date
/// files. A dash is used to avoid merging with server folders.
pub const ANSIBLE_DIR: &str = concat!("Ansible-Files/", env!("CARGO_PKG_VERSION"));

/// The name of the master playbook which invokes actions in Ansible
pub const MASTER_PLAYBOOK: &str = "master_playbook.yaml";

// Setting names for Ansible task filenames as easier references
pub const FULL_SETUP: &str = "full_setup.yaml";
pub const BASE_SETUP: &str = "base_setup.yaml";
pub const BASICS: &str = "basics_install.yaml";
pub const DNS_RESET: &str = "dns_reset.yaml";
pub const CF_ZONE: &str = "cf_getzone.yaml";
pub const CLOUDFLARE_DEL: &str = "cloudflare_del.yaml";
pub const CLOUDFLARE: &str = "cloudflare_setup.yaml";
pub const ACME: &str = "acme_get_cert.yaml";
pub const SSH_SETUP: &str = "ssh_setup.yaml";
pub const NGINX_SETUP: &str = "nginx_setup.yaml";
pub const DOH_SETUP: &str = "doh_setup.yaml";
pub const BOOTSTRAP: &str = "bootstrap.yaml";
pub const FAIL2BAN: &str = "fail2ban_setup.yaml";
pub const FIREWALL: &str = "firewall.yaml";
pub const FIREWALL_BOOTSTRAP: &str = "firewall_bootstrap.yaml";
pub const OUTBLOCKED_UPDATE: &str = "outblocked_update.yaml";
pub const XUI_LOGIN: &str = "xui_login.yaml";
pub const XUI_AUTH: &str = "xui_auth.yaml";
pub const PANEL_SETTINGS: &str = "xui_setup_panel.yaml";
pub const ADD_INBOUND: &str = "xui_add_inbound.yaml";
pub const DEL_INBOUND: &str = "xui_del_inbound.yaml";

pub const NGINX_FIX1: &str = "nginx_restart.yaml";
pub const NGINX_FIX2: &str = "nginx_restore_config.yaml";
pub const XRAY_FIX1: &str = "xui_restart.yaml";
pub const XRAY_FIX2: &str = "xui_restore_db.yaml";

/// The name of cloud configuration file
pub const CLOUD_CONFIG: &str = "cloud.toml";

/// The versioned cloud config example which will be written to disk in the
/// first run of the program
pub const CLOUD_EXAMPLE_NAME: &str = concat!("cloud-example-", env!("CARGO_PKG_VERSION"), ".toml");

/// The str which holds a descriptive example of cloud config
pub const CLOUD_EXAMPLE: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/cloud-example.toml"));

/// The name of SQLite DB file which stores all the information about the cloud
pub const CLOUD_DB: &str = "cloud.db";

/// The SQLite db file which stores GeoIP data
pub const GEOIP_DB: &str = "geoip.db";

/// The directory where cloud backups are stored. Intentionally dash is
/// included in the filename to avoid any clash with a server's directory.
pub const CLOUD_BACKUP_DIR: &str = "Cloud-Backups";

/// The directory where x-ui DB backups are stored. Dash is included to avoid
/// any clash with a server's directory.
pub const XUI_BACKUP_DIR: &str = "Xui-Backups";

/// The suffix for the filename which will contain the sublinks of generated
/// clients for each inbound
pub const SUBLINKS_SUFFIX: &str = "_sublinks.txt";

/// The suffix for the filename which will contain the json sublinks of
/// generated clients for each inbound
pub const JSON_SUBLINKS_SUFFIX: &str = "_json_sublinks.txt";

/// The number of x-ui DB backups will be pruned to this value
pub const RETAINED_XUI_DBNUM: usize = 4320; // ~ 3 days for monitoring every minute

/// The name of the file for storing Cloudflare API token
pub const CF_AUTH: &str = "cf-auth.toml";

/// The name of the minimal Ansible config file which will be used for all Ansible calls
pub const ANSIBLE_CFG: &str = "ansible_min.cfg";

/// The interval in seconds for checking service health
pub const SVC_MON_INTERVAL: u64 = 60;

pub const fn default_mon_interval() -> u64 {
    SVC_MON_INTERVAL
}

/// The total timeout in seconds when a service is checked
pub const SVC_TIMEOUT: u64 = 7;

/// The minimum monitoring interval, because a too low value can cause problems
/// in timeouts and channel buffers. 3x of SVC_TIMEOUT is a safe value.
pub const MIN_MON_INTERVAL: u64 = 3 * SVC_TIMEOUT;

/// The socket name used for IPC with the daemon
pub const SOCKET_NAME: &str = "xcplane.sock";

/// The PID name used for IPC with the daemon
pub const PID_NAME: &str = "xcplane.pid";

// exit codes
pub const EXIT_OK: i8 = 0;
pub const EXIT_ERROR: i8 = 1;
pub const EXIT_RESTART: i8 = 20;
pub const EXIT_RELOAD: i8 = 21;
pub const EXIT_REMAP: i8 = 22;
pub const EXIT_REBASE: i8 = 23;

/// How many consecutive monitoring intervals the service must fail to be
/// considered for an automated corrective action
pub const FIX_THRESHOLD: u64 = 10;

pub const fn default_threshold() -> u64 {
    FIX_THRESHOLD
}

/// Set of characters for random string generation for usernames
pub const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz\
			      ABCDEFGHIJKLMNOPQRSTUVWXYZ\
			      0123456789";
