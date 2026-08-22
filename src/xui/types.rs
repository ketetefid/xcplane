// SPDX-License-Identifier: GPL-3.0-or-later

//! API response types for the 3x-ui service

use serde::Deserialize;

pub type MetricsResponse = ApiResponse<ServerMetrics>;
pub type XuiApiInboundsResponse = ApiResponse<Vec<XuiApiInbound>>;

/// The general response of the 3XUI API
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ApiResponse<T> {
    #[serde(default)]
    pub success: bool,

    #[serde(default)]
    pub msg: String,

    #[serde(default)]
    pub obj: T,
}

/// Includes the response type of GET through /panel/api/server/status endpoint
#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ServerMetrics {
    #[serde(default)]
    pub cpu: f64,

    #[serde(default)]
    pub cpu_cores: u32,

    #[serde(default)]
    pub logical_pro: u32,

    #[serde(default)]
    pub cpu_speed_mhz: f64,

    #[serde(default)]
    pub mem: Memory,

    #[serde(default)]
    pub swap: Memory,

    #[serde(default)]
    pub disk: Disk,

    #[serde(default)]
    pub xray: XrayStatus,

    #[serde(default)]
    pub uptime: u64,

    #[serde(default)]
    pub loads: Vec<f64>,

    #[serde(default)]
    pub tcp_count: u32,

    #[serde(default)]
    pub udp_count: u32,

    #[serde(default)]
    pub net_io: NetIO,

    #[serde(default)]
    pub net_traffic: NetTraffic,

    #[serde(default)]
    pub public_ip: PublicIP,

    #[serde(default)]
    pub app_stats: AppStats,
}

#[derive(Debug, Deserialize, Default)]
pub struct Memory {
    #[serde(default)]
    pub current: u64,

    #[serde(default)]
    pub total: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct Disk {
    #[serde(default)]
    pub current: u64,

    #[serde(default)]
    pub total: u64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct XrayStatus {
    #[serde(default)]
    pub state: String,

    #[serde(default)]
    pub error_msg: String,

    #[serde(default)]
    pub version: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct NetIO {
    #[serde(default)]
    pub up: u64,

    #[serde(default)]
    pub down: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct NetTraffic {
    #[serde(default)]
    pub sent: u64,

    #[serde(default)]
    pub recv: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct PublicIP {
    #[serde(default)]
    pub ipv4: String,

    #[serde(default)]
    pub ipv6: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppStats {
    #[serde(default)]
    pub threads: u32,

    #[serde(default)]
    pub mem: u64,

    #[serde(default)]
    pub uptime: u64,
}

///////////////////////////////////////////////////////////////////

/// Represents an inbound structure used in API responses like
/// /panel/api/inbounds/get/<ID> or /panel/api/inbounds/list
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct XuiApiInbound {
    #[serde(default)]
    pub id: u32,

    #[serde(default)]
    pub up: u64,

    #[serde(default)]
    pub down: u64,

    #[serde(default)]
    pub total: u64,

    #[serde(default)]
    pub remark: String,

    #[serde(default)]
    pub sub_sort_index: u32,

    #[serde(default)]
    pub enable: bool,

    #[serde(default)]
    pub expiry_time: u64,

    #[serde(default)]
    pub traffic_reset: String,

    #[serde(default)]
    pub last_traffic_reset_time: u64,

    #[serde(default)]
    pub listen: String,

    #[serde(default)]
    pub port: u16,

    #[serde(default)]
    pub protocol: String,

    #[serde(default)]
    pub tag: String,

    #[serde(default)]
    pub share_addr_strategy: String,

    #[serde(default)]
    pub share_addr: String,

    // Nested structures
    #[serde(default)]
    pub client_stats: Vec<ClientStat>,

    #[serde(default)]
    pub settings: CSettings,

    #[serde(default)]
    pub sniffing: Sniffing,
    /*
    // For different protocols it must be implemented
    #[serde(default)]
    pub stream_settings: StreamSettings,

    #[serde(default)]
    pub xhttp_settings: XHttpSettings,
    */
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClientStat {
    #[serde(default)]
    pub id: u32,

    #[serde(default)]
    pub inbound_id: u32,

    #[serde(default)]
    pub enable: bool,

    #[serde(default)]
    pub email: String,

    #[serde(default)]
    pub uuid: String,

    #[serde(default)]
    pub sub_id: String,

    #[serde(default)]
    pub up: u64,

    #[serde(default)]
    pub down: u64,

    #[serde(default)]
    pub expiry_time: i64,

    #[serde(default)]
    pub total: u64,

    #[serde(default)]
    pub reset: u64,

    #[serde(default)]
    pub last_online: u64,
}

#[derive(Debug, Deserialize, Default)]
pub struct CSettings {
    #[serde(default)]
    pub clients: Vec<ClientSetting>,

    #[serde(default)]
    pub decryption: String,

    #[serde(default)]
    pub encryption: String,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ClientSetting {
    #[serde(default)]
    pub comment: String,

    #[serde(default, rename = "created_at")]
    pub created_at: u64,

    #[serde(default, rename = "updated_at")]
    pub updated_at: u64,

    #[serde(default)]
    pub id: String,

    #[serde(default)]
    pub limit_ip: i64,

    #[serde(default)]
    pub reset: i64,

    #[serde(default)]
    pub security: String,

    #[serde(default)]
    pub sub_id: String,

    #[serde(default)]
    pub tg_id: i64,

    #[serde(default)]
    pub total_gb: u64,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Sniffing {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default)]
    pub dest_override: Vec<String>,

    #[serde(default)]
    pub metadata_only: bool,

    #[serde(default)]
    pub route_only: bool,
}

/////////////////////////////////////////////////////////////

/// A lightweight version of XUI inbound returned in a GET call to
/// /Panel/api/inbounds/options
#[derive(Deserialize)]
pub struct LightXuiApiInbound {
    pub id: u32,
    pub remark: String,
    pub port: u16,
}
