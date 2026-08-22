// SPDX-License-Identifier: GPL-3.0-or-later

use arc_swap::ArcSwapOption;
use chrono::Utc;
use isocountry::CountryCode;
use rusqlite::backup::Backup;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::fs::{Permissions, set_permissions};
use std::iter;
use std::net::IpAddr;
use std::os::unix::fs::PermissionsExt;
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Mutex;
use tokio::time::Duration;
use tokio_rusqlite::{Connection as SqlConn, Error as SqlErr, params, rusqlite};
use toml_edit::{DocumentMut, value as toml_value};
use url::Host;

use crate::ansible::AnsibleConn;
use crate::constants::{
    CLOUD_BACKUP_DIR, CLOUD_CONFIG, CLOUD_DB, FIX_THRESHOLD, ROOT_USER, SVC_MON_INTERVAL,
};
use crate::types::{
    AtomicServerState, BoxError, Cloud, CloudSettings, Inbound, InboundKind, KetServer, Secrets,
    ServerState, SvcEntry, SvcInfo, SvcKind, SvcStatus, WorkSpace, XuiToken,
};

/// A trait to lessen the boilerplate when returning Result from rusqlite
/// closures
pub trait ToSqlError<T> {
    fn to_sql_err(self) -> Result<T, rusqlite::Error>;
}

impl<T, E> ToSqlError<T> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn to_sql_err(self) -> Result<T, rusqlite::Error> {
        self.map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
        })
    }
}

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

/// Sets up the schema for the database
pub async fn create_db(conn: Arc<SqlConn>) -> Result<(), BoxError> {
    // xui_token in servers table is the only DB field that can be null, because
    // it is built only when the server becomes production.
    conn.call(move |conn| {
        // Create the key-value table for storing global cloud options
        // and init with default values
        conn.execute(
            "CREATE TABLE IF NOT EXISTS options (
                 key TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             )",
            [],
        )?;

        let default_options = [
            ("cloud_config", ""),
            ("inbound_set", "[]"),
            ("auto_fix", "true"),
            ("fix_threshold", &FIX_THRESHOLD.to_string()),
            ("monitor_interval", &SVC_MON_INTERVAL.to_string()),
        ];

        let mut stmto = conn.prepare(
            "INSERT INTO options (key, value)
                 VALUES (?1, ?2)
                 ON CONFLICT(key) DO NOTHING",
        )?;

        for (key, value) in default_options {
            stmto.execute(params![key, value])?;
        }

        // Create the server table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS servers (
		 id                 INTEGER PRIMARY KEY,
		 name               TEXT UNIQUE NOT NULL,
		 ip                 TEXT NOT NULL,
                 domain             TEXT NOT NULL,
		 region             TEXT NOT NULL,
                 state              TEXT NOT NULL,
                 quota              INTEGER NOT NULL,
                 enabled            INTEGER NOT NULL CHECK (enabled IN (0,1)),
                 ssh_port           INTEGER NOT NULL,
                 dns_subdomain      TEXT NOT NULL,
                 ui_subdomain       TEXT NOT NULL,
                 sub_subdomain      TEXT NOT NULL,
                 health_path        TEXT NOT NULL,
                 doh_endpoint       TEXT NOT NULL,
                 xui_username       TEXT NOT NULL,
                 xui_password       TEXT NOT NULL,
                 xui_token          TEXT,
                 xui_webpath        TEXT NOT NULL,
                 xui_subpath        TEXT NOT NULL,
                 xui_jsubpath       TEXT NOT NULL,
                 cloudflare_proxied INTEGER NOT NULL CHECK (cloudflare_proxied IN (0,1)),
                 outbound_block     TEXT NOT NULL
	     )",
            [], // empty list of parameters.
        )?;

        // Enforce foreign keys
        conn.execute("PRAGMA foreign_keys = ON;", [])?;

        // Create a group table for service stats where we will store SSH,
        // Nginx, and Xray inbounds for each server as service
        conn.execute(
            "CREATE TABLE IF NOT EXISTS stats (
                 server_name  TEXT NOT NULL,
                 service      TEXT NOT NULL,
                 health       TEXT NOT NULL DEFAULT 'Unknown',
                 failed_count INTEGER NOT NULL DEFAULT 0,
                 failed_since INTEGER NOT NULL DEFAULT 0,
                 fix_try      INTEGER NOT NULL DEFAULT 0,
                 PRIMARY KEY (server_name, service),
                 FOREIGN KEY (server_name) REFERENCES servers(name) ON DELETE CASCADE
             )",
            [],
        )?;

        // Create a group table for inbounds
        conn.execute(
            "CREATE TABLE IF NOT EXISTS inbounds (
                 server_name       TEXT NOT NULL,
                 inbound_group     TEXT NOT NULL,
                 port              INTEGER NOT NULL,
                 kind              TEXT NOT NULL,
                 traffic           INTEGER NOT NULL,
                 expiry            INTEGER NOT NULL,
                 comment           TEXT NOT NULL,
                 mother_subid      TEXT NOT NULL,
                 total_clients     INTEGER NOT NULL,
                 PRIMARY KEY (server_name, inbound_group),
                 FOREIGN KEY (server_name) REFERENCES servers(name) ON DELETE CASCADE
             )",
            [],
        )?;

        // Set the closure return type for compatible error handling
        Ok::<(), SqlErr>(())
    })
    .await?;

    Ok(())
}
// =============================================================
/// Populates or updates the database to match the reconciled cloud
pub async fn update_db(
    workspace: Arc<WorkSpace>,
    conn: Arc<SqlConn>,
    cloud: Cloud,
) -> Result<(), BoxError> {
    conn
        .call(move |conn| {
	    // Directly store the whole cloud config as is into the options
	    // table
	    let cloud_config_path = workspace.dirs.config_dir.join(CLOUD_CONFIG);
	    let cloud_config = std::fs::read_to_string(&cloud_config_path).to_sql_err()?;

	    // Update the global cloud options
            let mut stmto = conn.prepare(
		"UPDATE options SET value = ?2 WHERE key = ?1"
            )?;

	    let inbound_set_json = serde_json::to_string(&cloud.settings.inbound_set).to_sql_err()?;

	    let cloud_options = [
		("cloud_config", cloud_config),
		("inbound_set", inbound_set_json),
		("auto_fix", cloud.settings.auto_fix.to_string()),
		("fix_threshold", cloud.settings.fix_threshold.to_string()),
		("monitor_interval", cloud.settings.monitor_interval.to_string())
	    ];

	    for (key, value) in cloud_options {
		stmto.execute(params![key, value])?;
	    }

	    // Get all the servers we want to retain
	    let server_names: Vec<String> = cloud.servers.iter().map(|s| s.name.clone()).collect();

            if server_names.is_empty() {
		conn.execute("DELETE FROM servers", params![])?;
            } else {
		let holder_sql = iter::repeat("?")
                .take(server_names.len())
                .collect::<Vec<_>>()
                .join(",");

		conn.execute(&format!(
                    "DELETE FROM servers WHERE name NOT IN ({})",
                    holder_sql
		), rusqlite::params_from_iter(server_names))?;
            }

	    // Insert or update the servers table
            let mut stmts = conn.prepare(
                r#"
                INSERT INTO servers (name, ip, domain, region, state,
                                     quota, enabled, ssh_port, dns_subdomain,
                                     ui_subdomain, sub_subdomain, health_path,
                                     doh_endpoint, xui_username, xui_password,
                                     xui_token, xui_webpath, xui_subpath,
                                     xui_jsubpath, cloudflare_proxied,
                                     outbound_block)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                        ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21)
                ON CONFLICT(name)
                DO UPDATE SET
		   ip = excluded.ip,
                   domain = excluded.domain,
		   region = excluded.region,
		   state = excluded.state,
                   quota = excluded.quota,
                   enabled = excluded.enabled,
                   ssh_port = excluded.ssh_port,
                   dns_subdomain = excluded.dns_subdomain,
                   ui_subdomain = excluded.ui_subdomain,
                   sub_subdomain = excluded.sub_subdomain,
                   health_path = excluded.health_path,
                   doh_endpoint = excluded.doh_endpoint,
                   xui_username = excluded.xui_username,
                   xui_password = excluded.xui_password,
                   xui_token = excluded.xui_token,
                   xui_webpath = excluded.xui_webpath,
                   xui_subpath = excluded.xui_subpath,
                   xui_jsubpath = excluded.xui_jsubpath,
                   cloudflare_proxied = excluded.cloudflare_proxied,
                   outbound_block = excluded.outbound_block
                "#,
            )?;

            // Set the initial health data for the services
            let mut stmts_stats = conn.prepare(
                r#"
                INSERT INTO stats (server_name, service, health, failed_count, failed_since, fix_try)
                VALUES (?1, ?2, 'Unknown', 0, 0, 0)
                ON CONFLICT(server_name, service) DO NOTHING
                "#,
            )?;

            for ketserver in cloud.servers.iter() {
                stmts.execute(params![
                    ketserver.name,
                    ketserver.ip.to_string(),
		    ketserver.domain.to_string(),
                    ketserver.region.alpha2().to_string(),
                    ketserver.state.load().to_string(),
		    ketserver.quota,
		    ketserver.enabled,
		    ketserver.ssh_port,
		    ketserver.dns_subdomain,
		    ketserver.ui_subdomain,
		    ketserver.sub_subdomain,
		    ketserver.secrets.health_path,
		    ketserver.secrets.doh_endpoint,
		    ketserver.secrets.xui_username,
		    ketserver.secrets.xui_password,
		    ketserver.secrets.xui_token.0.get(),
		    ketserver.secrets.xui_webpath,
		    ketserver.secrets.xui_subpath,
		    ketserver.secrets.xui_jsubpath,
		    ketserver.cloudflare_proxied,
		    serde_json::to_string(&ketserver.outbound_block).to_sql_err()?
                ])?;
            }

            let mut stmti = conn.prepare(
                r#"
                 INSERT INTO inbounds (
                 server_name, inbound_group, port, kind, traffic,
                 expiry, comment, mother_subid, total_clients)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(server_name, inbound_group)
                 DO UPDATE SET
                    port = excluded.port,
                    kind = excluded.kind,
                    traffic = excluded.traffic,
                    expiry = excluded.expiry,
                    comment = excluded.comment,
                    mother_subid = excluded.mother_subid,
                    total_clients = excluded.total_clients
                 "#,
            )?;

            for server in cloud.servers.iter() {
		let holder_sql = iter::repeat("?")
		    .take(server.inbounds.len())
		    .collect::<Vec<_>>()
		    .join(",");

		let params = iter::once(server.name.as_str()).chain(server.inbounds.iter().map(|inb| inb.name.as_str()));

		// Retain only the inbounds present in the server
		conn.execute(&format!(
		    "DELETE FROM inbounds WHERE server_name = ? AND inbound_group NOT IN ({})",
		    holder_sql
		), rusqlite::params_from_iter(params))?;

                // Now populate the inbound group
                for inbound in &server.inbounds {
		    let mother_subid = server
			.secrets
			.mother_subids
			.get(&inbound.name)
			.ok_or_else(|| {
			    rusqlite::Error::FromSqlConversionFailure(
				0,
				rusqlite::types::Type::Text,
				format!("Missing mother_subid for inbound {} in server {}",
					inbound.name, server.name).into(),
			    )
			})?;

                    stmti.execute(params![
                        server.name,
                        inbound.name,
			inbound.port,
			inbound.kind.to_string(),
			inbound.traffic,
			inbound.expiry,
			inbound.comment,
			mother_subid,
                        inbound.total
                    ])?;
                }

		// Check stats table to see what services should be retained
		let holder_sql = iter::repeat("?")
		    .take(server.svc_kinds().len())
		    .collect::<Vec<_>>()
		    .join(",");
		let all_svcs = server.svc_kinds_string();
		let params = iter::once(server.name.as_str()).chain(all_svcs.iter().map(|svc_kind| svc_kind.as_str()));

		// Retain only the services that the server has
		conn.execute(&format!(
		    "DELETE FROM stats WHERE server_name = ? AND service NOT IN ({})",
		    holder_sql
		), rusqlite::params_from_iter(params))?;

                // Now populate the services stats for the initial
                // insertion; updating will occur in write_status_data
                for svc in server.svc_kinds_string() {
                    stmts_stats.execute(params![server.name, svc])?;
                }
            }

	    // Set the closure return type for compatible error handling
            Ok::<(), SqlErr>(())
        })
        .await?;

    Ok(())
}
// =============================================================
/// Loads the cloud from DB. The static values are loaded but dynamical states
/// of the servers such as login_gate & xui are initialized empty.
pub async fn load_cloud(conn: Arc<SqlConn>) -> Result<Cloud, BoxError> {
    let cloud = conn
        .call(move |conn| {
            let mut stmti = conn.prepare(
                "SELECT inbound_group, port, kind, total_clients, traffic, expiry,
		 comment, mother_subid FROM inbounds WHERE server_name = ?1",
            )?;

            let mut stmts = conn.prepare(
                "SELECT name, ip, domain, region, state, quota,
                enabled, ssh_port, dns_subdomain, ui_subdomain, sub_subdomain,
                health_path, doh_endpoint, xui_username, xui_password, xui_token,
                xui_webpath, xui_subpath, xui_jsubpath, cloudflare_proxied,
                outbound_block FROM servers",
            )?;

            let rows = stmts.query_map([], |row| {
                let server_name: String = row.get("name")?;
                let ip_str: String = row.get("ip")?;
                let ssh_port = row.get::<_, i64>("ssh_port")? as u16;
                let mut mother_subids = HashMap::<String, String>::new();

                Ok(KetServer {
                    name: server_name.clone(),
                    ip: ip_str.parse::<IpAddr>().to_sql_err()?,
                    domain: {
                        let domain_str: String = row.get("domain")?;
                        Host::parse(&domain_str).to_sql_err()?
                    },
                    region: {
                        let region_str: String = row.get("region")?;
                        CountryCode::for_alpha2(&region_str).to_sql_err()?
                    },
                    state: {
                        let state_str: String = row.get("state")?;
                        let state = ServerState::from_str(&state_str).to_sql_err()?;
                        AtomicServerState::new(state)
                    },
                    quota: row.get::<_, i64>("quota")? as u16,
                    enabled: row.get::<_, i64>("enabled")? == 1,
                    ssh_port: ssh_port,
                    dns_subdomain: row.get("dns_subdomain")?,
                    ui_subdomain: row.get("ui_subdomain")?,
                    sub_subdomain: row.get("sub_subdomain")?,
                    cloudflare_proxied: row.get::<_, i64>("cloudflare_proxied")? == 1,
                    outbound_block: {
                        let outbound_block_str: String = row.get("outbound_block")?;
                        serde_json::from_str(&outbound_block_str).to_sql_err()?
                    },
                    ansible_conn: {
                        let ansible_conn = AnsibleConn {
                            ansible_host: Some(ip_str),
                            ansible_port: Some(ssh_port),
                            ansible_user: Some(ROOT_USER),
                            ansible_connection: None,
                        };

                        ArcSwapOption::from_pointee(ansible_conn)
                    },

                    inbounds: {
                        let inb_rows = stmti.query_map(params![server_name], |irow| {
                            let name: String = irow.get("inbound_group")?;
                            let port = irow.get::<_, i64>("port")? as u16;
                            let kind = {
                                let kind_str: String = irow.get("kind")?;
                                InboundKind::from_str(&kind_str).to_sql_err()?
                            };
                            let total = irow.get::<_, i64>("total_clients")? as u16;
                            let traffic = irow.get::<_, i64>("traffic")? as u32;
                            let expiry = irow.get::<_, i64>("expiry")? as u16;
                            let comment: String = irow.get("comment")?;

                            let mother_subid: String = irow.get("mother_subid")?;
                            mother_subids.entry(name.clone()).or_insert(mother_subid);

                            Ok(Inbound {
                                name,
                                port,
                                kind,
                                total,
                                traffic,
                                expiry,
                                comment,
                            })
                        })?;
                        let mut inbounds = Vec::new();
                        for inb in inb_rows {
                            inbounds.push(inb?);
                        }

                        inbounds
                    },

                    // While most of the secrets reside in the server table,
                    // subscription IDs naturally are stored in the inbounds
                    // table, so secrets field is computed after getting inbounds.
                    secrets: Secrets {
                        health_path: row.get("health_path")?,
                        doh_endpoint: row.get("doh_endpoint")?,
                        xui_username: row.get("xui_username")?,
                        xui_password: row.get("xui_password")?,
                        xui_token: {
                            let token: Option<String> = row.get("xui_token")?;
                            match token {
                                None => XuiToken::empty(),
                                Some(value) => XuiToken::from(value),
                            }
                        },
                        xui_webpath: row.get("xui_webpath")?,
                        xui_subpath: row.get("xui_subpath")?,
                        xui_jsubpath: row.get("xui_jsubpath")?,
                        mother_subids,
                    },

                    // Empty runtime states
                    cfstate: ArcSwapOption::empty(),
                    xui: ArcSwapOption::empty(),
                    login_gate: Arc::new(Mutex::new(())),
                })
            })?;
            let mut servers = Vec::new();
            for server in rows {
                servers.push(Arc::new(server?));
            }

            // Get the global options
            let inbound_set = get_option_json(conn, "inbound_set")?;
            let auto_fix = get_option_parse(conn, "auto_fix")?;
            let fix_threshold = get_option_parse(conn, "fix_threshold")?;
            let monitor_interval = get_option_parse(conn, "monitor_interval")?;

            let settings = CloudSettings {
                inbound_set,
                auto_fix,
                fix_threshold,
                monitor_interval,
            };

            let cloud = Cloud { servers, settings };

            Ok::<Cloud, SqlErr>(cloud)
        })
        .await?;

    Ok(cloud)
}
// =============================================================
/// When a server is provisioned, this function changes the state of the server
/// in the DB, and also in the cloud config file.
pub async fn mark_server_production(
    workspace: Arc<WorkSpace>,
    conn: Arc<SqlConn>,
    server: Arc<KetServer>,
    xui_token: String,
) -> Result<(), BoxError> {
    // Changing the runtime state (which will be transferred with DaemonNext)
    server.state.store(ServerState::Production);
    server.secrets.xui_token.0.set(xui_token.clone())?;

    let ansible_conn = server
        .ansible_conn
        .load_full()
        .ok_or("Couldn't get AnsibleConn in mark_server_production.")?;

    let updated_conn = AnsibleConn {
        ansible_host: ansible_conn.ansible_host.clone(),
        ansible_port: Some(server.ssh_port),
        ansible_user: ansible_conn.ansible_user,
        ansible_connection: None,
    };
    server.ansible_conn.store(Some(Arc::new(updated_conn)));

    let the_server = server.clone();

    // The DB remains the source of truth
    conn.call(move |conn| {
        conn.execute(
            "UPDATE servers SET state = ?2, xui_token = ?3 WHERE name = ?1",
            params![server.name, ServerState::Production.to_string(), xui_token],
        )?;

        Ok::<(), SqlErr>(())
    })
    .await?;

    // We update the cloud config file as well to mark the server as
    // Production. While this is not necessary for xcplane to work as intended
    // (DB is the single source of truth), it helps in averting accidental,
    // unwanted Remap or Rebase to Offgrid.
    let config_path = workspace.dirs.config_dir.join(CLOUD_CONFIG);
    let cloud_config = fs::read_to_string(&config_path).await?;

    let mut cloud = cloud_config.parse::<DocumentMut>()?;

    let servers = cloud["servers"]
        .as_array_of_tables_mut()
        .ok_or("servers must be an array of tables in cloud config.")?;

    if let Some(server) = servers
        .iter_mut()
        .find(|s| s["name"].as_str() == Some(the_server.name.as_str()))
    {
        server["state"] = toml_value("Production");
    }

    fs::write(&config_path, cloud.to_string()).await?;

    Ok(())
}
// =============================================================
/// Gets service stats SvcStatus from DB
pub async fn read_status_data(
    conn: Arc<SqlConn>,
    service_info: Arc<SvcInfo>,
) -> Result<SvcStatus, BoxError> {
    let svc_name = service_info.kind.to_string();
    let server_name = service_info.server.name.clone();
    let (failed_count, failed_since, fix_try, health): (u64, u64, u8, String) = conn
        .call(move |conn| {
            let stmt = conn.query_row(
                r#"SELECT failed_count, failed_since, fix_try,
                health FROM stats WHERE server_name=?1 AND service=?2"#,
                params![server_name, svc_name],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            Ok::<(u64, u64, u8, String), SqlErr>(stmt)
        })
        .await?;

    let svc_status = SvcStatus {
        health: health.parse()?,
        failed_count,
        failed_since,
        fix_try,
    };

    Ok(svc_status)
}
// =============================================================
/// Write service status data SvcStatus to DB
pub async fn write_status_data(conn: Arc<SqlConn>, stats: Vec<SvcEntry>) -> Result<(), BoxError> {
    conn.call(move |conn| {
        let mut stmt = conn.prepare(
            r#"UPDATE stats SET failed_count=?3, failed_since=?4,
                fix_try=?5, health=?6 WHERE server_name=?1 AND service=?2"#,
        )?;
        for stat in stats {
            stmt.execute(params![
                stat.info.server.name,
                stat.info.kind.to_string(),
                stat.status.failed_count,
                stat.status.failed_since,
                stat.status.fix_try,
                stat.status.health.to_string()
            ])?;
        }
        Ok::<(), SqlErr>(())
    })
    .await?;

    Ok(())
}
// =============================================================
/// Creates a backup of the current running cloud in the forms of DB and toml config
pub async fn create_backup(
    cloud: &Cloud,
    conn: Arc<SqlConn>,
    workspace: Arc<WorkSpace>,
    action: String,
) -> Result<(), BoxError> {
    let the_action = action.clone();
    let the_workspace = workspace.clone();
    conn.call(move |conn| {
        let db_backup_name = format!(
            "cloud-{}-{}.db",
            action,
            Utc::now().format("%Y-%m-%d_%H-%M-%S")
        );
        let backup_path = workspace
            .dirs
            .data_dir
            .join(CLOUD_BACKUP_DIR)
            .join(db_backup_name);

        // Creating backup dir if it doesn't exist
        if let Some(parent) = backup_path.parent() {
            std::fs::create_dir_all(&parent).to_sql_err()?;

            // Setting strict permission on the backup dir is needed
            set_permissions(&parent, Permissions::from_mode(0o700)).to_sql_err()?;
        }

        let mut backup_conn = rusqlite::Connection::open(&backup_path)?;
        let backup = Backup::new(conn, &mut backup_conn)?;
        backup.run_to_completion(1000, Duration::ZERO, None)?;

        // Set 600 on the file, too
        set_permissions(&backup_path, Permissions::from_mode(0o600)).to_sql_err()?;
        Ok::<_, SqlErr>(())
    })
    .await?;

    let cloud_str = toml::to_string(cloud)?;

    // Deleting super inbounds before writing to file
    let mut cloud_toml = cloud_str.parse::<DocumentMut>()?;

    let servers = cloud_toml["servers"]
        .as_array_of_tables_mut()
        .ok_or("servers must be an array of tables in cloud config.")?;

    let super_name = SvcKind::super_name();
    for server in servers.iter_mut() {
        if let Some(inbounds) = server["inbounds"].as_array_of_tables_mut() {
            inbounds.retain(|inb| inb["name"].as_str() != Some(&super_name));
        }
    }

    let toml_backup_name = format!(
        "cloud-{}-{}.toml",
        the_action,
        Utc::now().format("%Y-%m-%d_%H-%M-%S")
    );

    // The cloud-*.toml does not contain secrets and doesn't need to have very
    // strict permissions.
    let backup_path = the_workspace
        .dirs
        .data_dir
        .join(CLOUD_BACKUP_DIR)
        .join(toml_backup_name);

    fs::write(backup_path, cloud_toml.to_string()).await?;

    Ok(())
}
// =============================================================
/// Restores cloud config from DB into config directory
pub async fn restore_config(workspace: &WorkSpace) -> Result<(), BoxError> {
    // Since this function is called as a standalone command, we open the DB directly
    let db_path = workspace.dirs.data_dir.join(CLOUD_DB);
    let conn = SqlConn::open(db_path).await?;

    let cloud_config = conn
        .call(move |conn| {
            let cloud_config: String = get_option_parse(conn, "cloud_config")?;

            Ok::<_, SqlErr>(cloud_config)
        })
        .await?;

    if cloud_config.trim().is_empty() {
        return Err("No cloud config has been backed up to DB yet.".into());
    }

    let cloud_config_path = workspace.dirs.config_dir.join(CLOUD_CONFIG);
    fs::write(&cloud_config_path, cloud_config).await?;

    println!(
        "The last reconciled cloud was restored to '{}'",
        cloud_config_path.display()
    );

    Ok(())
}
// =============================================================
/// Gets a json 'value' from 'options' table in the DB
fn get_option_json<T>(conn: &rusqlite::Connection, key: &str) -> rusqlite::Result<T>
where
    T: DeserializeOwned,
{
    let stmto_sql = "SELECT value FROM options WHERE key = ?1";
    conn.query_row(stmto_sql, params![key], |r| {
        let value_json_str: String = r.get(0)?;
        let value = serde_json::from_str::<T>(&value_json_str).to_sql_err()?;

        Ok(value)
    })
}
// =============================================================
/// Gets a parsed 'value' from 'options' table in the DB
fn get_option_parse<T>(conn: &rusqlite::Connection, key: &str) -> rusqlite::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static, // awesome compiler suggestions
{
    let stmto_sql = "SELECT value FROM options WHERE key = ?1";
    conn.query_row(stmto_sql, params![key], |r| {
        let value_str: String = r.get(0)?;
        let value = value_str.parse::<T>().to_sql_err()?;

        Ok(value)
    })
}
// =============================================================
