// SPDX-License-Identifier: GPL-3.0-or-later

use chrono::Utc;
use futures_util::TryStreamExt;
use reqwest::{self, Client, Response, StatusCode, cookie::Jar, header as Header};
use std::fs::{Permissions, set_permissions};
use std::os::unix::fs::PermissionsExt;
use std::process::Output;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use tokio::time::Duration;
use tokio::{fs, io};
use tokio_util::io::StreamReader;
use tracing::{debug, info, instrument};
use url::Url;

use super::types::{ApiResponse, MetricsResponse, XuiApiInboundsResponse};
use crate::constants::{XCPLANE_AGENT, XUI_BACKUP_DIR};
use crate::types::{BoxError, KetServer, WorkSpace, Xui, ok_output};

/// There are two ways to perform an API call to 3XUI: using the server's light
/// client for header-based authorization utilizing xui_token, and using the
/// full client which posts to the login page (CSRF + Cookie) for authorization.
#[derive(Debug)]
enum AuthMethod {
    Token,
    Cookie,
}

impl KetServer {
    // =============================================================
    /// Loads the secrets from a production server and prepares two clients for
    /// x-ui API requests: a lightweight one for header-based authorization and
    /// a cookie-based one for use in panel logins
    #[instrument(skip(self), fields(server=%self.name))]
    pub async fn new_xui_client(&self) -> Result<(), BoxError> {
        // Get the xui_token and build the light client
        let xui_token = self.secrets.xui_token.0.get();
        let xui_token = xui_token.ok_or(format!(
            "'xui_token' is absent from secrets container of production server '{}'",
            self.name
        ))?;
        let mut headers = Header::HeaderMap::new();
        let mut auth_value = Header::HeaderValue::from_str(&format!("Bearer {}", xui_token))?;
        auth_value.set_sensitive(true);
        headers.insert(Header::AUTHORIZATION, auth_value);

        let lightclient = Client::builder()
            .user_agent(XCPLANE_AGENT)
            .default_headers(headers)
            .timeout(Duration::from_secs(5))
            .build()?;

        // Build the cookie-bearing client
        let jar = Arc::new(Jar::default());
        let client = Client::builder()
            .user_agent(XCPLANE_AGENT)
            .cookie_provider(jar)
            .timeout(Duration::from_secs(5))
            .build()?;

        let xui_url_global = Url::parse(&format!(
            "https://{}/{}/",
            self.uihostname(),
            self.secrets.xui_webpath
        ))?;

        // The start of cookie counting
        let cookie_count = AtomicU64::new(0);

        // Note: the xui struct can also be built by reading and deserializing
        // 'xui_credentials.yaml' in <data dir>/<server name>
        let cred = Xui {
            xui_url_global,
            xui_username: self.secrets.xui_username.clone(),
            xui_password: self.secrets.xui_password.clone(),
            xui_token: xui_token.to_owned(),
            lightclient,
            client,
            cookie_count,
        };

        self.xui.store(Some(Arc::new(cred)));
        debug!("Xui client was created.");

        Ok(())
    }
    // =============================================================
    /// Performs login to the x-ui panel and stores the cookie in the jar inside
    /// the client
    async fn xui_login(&self) -> Result<(), BoxError> {
        let xui_cred = match self.xui.load_full() {
            Some(cred) => cred,
            None => {
                self.new_xui_client().await?;
                self.xui
                    .load_full()
                    .ok_or("Couldn't get the xui value in xui_login.")?
            }
        };

        // We need to make sure the Ansible setup has written the URL with a
        // trailing slash, or joining here will end up in 404 error.
        let login_endpoint = xui_cred.xui_url_global.join("login")?;
        let csrf_endpoint = xui_cred.xui_url_global.join("csrf-token")?;

        // First, GET the base page to store the session cookie
        xui_cred
            .client
            .get(xui_cred.xui_url_global.clone())
            .send()
            .await?
            .error_for_status()?;

        // Then, get the CSRF token using the cookie gotten above
        let csrf_res = xui_cred
            .client
            .get(csrf_endpoint)
            .send()
            .await?
            .error_for_status()?
            .json::<ApiResponse<String>>()
            .await?;

        if !csrf_res.success {
            return Err("Couldn't mint a CSRF token.".into());
        }

        let csrf_token = csrf_res.obj;

        // Now login. The API call needs the credentials in json not 'form'.
        xui_cred
            .client
            .post(login_endpoint)
            .header("X-CSRF-Token", csrf_token)
            .json(&serde_json::json!({
                "username": xui_cred.xui_username,
                "password": xui_cred.xui_password
            }))
            .send()
            .await?
            .error_for_status()?;

        // Update the cookie counter
        xui_cred.cookie_count.fetch_add(1, Relaxed);
        self.xui.store(Some(xui_cred));

        debug!("Xui full client was updated.");

        Ok(())
    }
    // =============================================================
    /// A universal function to GET data from x-ui API using the server's
    /// cookie-bearing client
    async fn xui_get_call_cookie(&self, suburl: &str) -> Result<Response, BoxError> {
        let xui_cred = if let Some(cred) = self.xui.load_full() {
            cred
        } else {
            // To prevent all calls storming for a login we guard the gate
            let _guard = self.login_gate.lock().await;
            // Either we're the one who locked it or someone else did it and we
            // have been waiting for the unlock. Therefore, a reload & recheck
            // is necessary in case of the second pathway.
            if let Some(cred) = self.xui.load_full() {
                cred
            } else {
                // If still None, it means it's our duty to initialize it
                self.xui_login().await?;
                self.xui
                    .load_full()
                    .ok_or("Couldn't get the xui value in xui_get_call_cookie after login.")?
            }
        };

        // Before any retries, get the current cookie counter
        let count_before = xui_cred.cookie_count.load(Relaxed);

        let endpoint = xui_cred.xui_url_global.join(suburl)?;
        let mut resp = xui_cred.client.get(endpoint.clone()).send().await?;

        // x-ui uses 404 for obfuscation
        if resp.status() == StatusCode::FORBIDDEN
            || resp.status() == StatusCode::UNAUTHORIZED
            || resp.status() == StatusCode::NOT_FOUND
        {
            // Before anything, just reload and recheck cookie count. We might
            // have had a stale auth while someone else has already renewed it.
            let xui_cred_now = self
                .xui
                .load_full()
                .ok_or("Couldn't get the xui value in xui_get_call_cookie.")?;

            let count_after = xui_cred_now.cookie_count.load(Relaxed);

            if count_after != count_before {
                // Another call refreshed auth while our request was in flight.
                // Retry immediately with the new session. We expect to succeed.
                resp = xui_cred_now.client.get(endpoint.clone()).send().await?;
            } else {
                // No one has refreshed the auth yet; we get prepared to do it.
                let _guard = self.login_gate.lock().await;

                // Another reload & recheck is necessary as we might be the one
                // who had to wait for the gate unlock, and another task has
                // already refreshed the session.
                let xui_cred_locked = self
                    .xui
                    .load_full()
                    .ok_or("Couldn't get the xui value in xui_get_call_cookie.")?;

                let count_locked = xui_cred_locked.cookie_count.load(Relaxed);

                // It's really our duty now.
                if count_locked == count_before {
                    self.xui_login().await?;
                }
                let xui_cred_fresh = self
                    .xui
                    .load_full()
                    .ok_or("Couldn't get the xui value in xui_get_call_cookie.")?;

                resp = xui_cred_fresh.client.get(endpoint).send().await?;
            }
        }

        Ok(resp)
    }
    // =============================================================
    /// A universal function to GET data from x-ui API using the server's
    /// light client i.e., token-based authorization
    async fn xui_get_call_token(&self, suburl: &str) -> Result<Response, BoxError> {
        let xui_cred = if let Some(cred) = self.xui.load_full() {
            cred
        } else {
            self.new_xui_client().await?;
            self.xui.load_full().ok_or(format!(
                "Couldn't initialize Xui struct for server {} in xui_get_call_token.",
                self.name
            ))?
        };
        let endpoint = xui_cred.xui_url_global.join(suburl)?;
        Ok(xui_cred.lightclient.get(endpoint).send().await?)
    }
    // =============================================================
    /// GETs data from x-ui API using the specified AuthMethod
    #[instrument(skip(self, suburl), fields(server = %self.name, api_path = %suburl))]
    async fn xui_get_call(&self, suburl: &str, method: AuthMethod) -> Result<Response, BoxError> {
        match method {
            AuthMethod::Cookie => Ok(self.xui_get_call_cookie(suburl).await?),
            AuthMethod::Token => Ok(self.xui_get_call_token(suburl).await?),
        }
    }
    // =============================================================
    /// Gets the server stats using x-ui API
    pub async fn xui_call_metrics(&self) -> Result<MetricsResponse, BoxError> {
        let _backup_method = AuthMethod::Cookie;

        let suburl = "panel/api/server/status";
        let resp = self
            .xui_get_call(suburl, AuthMethod::Token)
            .await?
            .error_for_status()?;
        let metrics = resp.json::<MetricsResponse>().await?;

        Ok(metrics)
    }
    // =============================================================
    /// Downloads a backup of the x-ui database
    pub async fn xui_call_db(&self, workspace: &WorkSpace) -> Result<Output, BoxError> {
        let suburl = "panel/api/server/getDb";
        let stream = self
            .xui_get_call(suburl, AuthMethod::Token)
            .await?
            .error_for_status()?
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));

        let mut reader = StreamReader::new(stream);

        let fname = format!(
            "{}-{}-xui-backup.db",
            self.name,
            Utc::now().format("%Y-%m-%d_%H-%M-%S")
        );

        let backup_dir_path = workspace.dirs.data_dir.join(XUI_BACKUP_DIR);
        if !backup_dir_path.exists() {
            fs::create_dir(&backup_dir_path).await?;
            // Data dir is already 700, and we set 700 for XUI backup dir too
            set_permissions(&backup_dir_path, Permissions::from_mode(0o700))?;
        }

        let backup_path = backup_dir_path.join(&fname);
        let mut file = fs::File::create(&backup_path).await?;

        io::copy(&mut reader, &mut file).await?;

        set_permissions(&backup_path, Permissions::from_mode(0o600))?;

        // For the sake of uniformity, instead of returning () we chose Output
        // like the Ansible calls.
        Ok(ok_output())
    }
    // =============================================================
    /// Gets all the inbounds belonging to the server using x-ui API
    pub async fn xui_call_inbounds(&self) -> Result<XuiApiInboundsResponse, BoxError> {
        let suburl = "panel/api/inbounds/list";
        let resp = self
            .xui_get_call(suburl, AuthMethod::Token)
            .await?
            .error_for_status()?;

        let xui_inbounds = resp.json::<XuiApiInboundsResponse>().await?;

        Ok(xui_inbounds)
    }
    // =============================================================
    /// A function to fetch all clients from the server's application
    /// panel.
    pub async fn xui_call_clients(&self) -> Result<String, BoxError> {
        // Since xcplane doesn't own the cloud clients, we fetch them through
        // the API call for each server, and build a table
        let subhostname = self.subhostname();
        let sublink_prefix = format!("https://{}/{}/", &subhostname, &self.secrets.xui_subpath);
        let jsublink_prefix = format!("https://{}/{}/", &subhostname, &self.secrets.xui_jsubpath);

        let xui_inbounds = self.xui_call_inbounds().await?.obj;

        let tab = "      ";
        let mut all_clients = format!(
            "{:<20}{:<20}{tab}{} & {}\n{}\n",
            "inbound",
            "email",
            "sublink",
            "json sublink",
            "------------------------------------------------------------\
	     ------------------------------------------------------------"
        );

        for inbound in xui_inbounds {
            let mut inbound_clients = String::from("");
            // We intentionally omit mother inbound clients since they are
            // created by xcplane solely for checking inbound health. Stale
            // clients (those without a sub_id) are also excluded.
            for stat in inbound
                .client_stats
                .iter()
                .filter(|cs| !cs.email.starts_with("mother_inbound") && !cs.sub_id.is_empty())
            {
                // inbound - email - sublink - jsublink
                let this_client = format!(
                    "{:<20}{:<20}{tab}{}{tab}{}\n",
                    &inbound.remark,
                    &stat.email,
                    sublink_prefix.clone() + &stat.sub_id,
                    jsublink_prefix.clone() + &stat.sub_id
                );

                inbound_clients += &this_client;
            }

            all_clients += &(inbound_clients + "\n");
        }

        Ok(all_clients)
    }
    // =============================================================
    pub async fn test_xui_api(&self) -> Result<(), BoxError> {
        let metrics = self.xui_call_metrics().await?;
        info!(metrics = ?metrics, "metrics");

        let api_inbounds = self.xui_call_inbounds().await?;
        info!(api_inbounds = ?api_inbounds, "xui API inbounds");

        Ok(())
    }
}
