// SPDX-License-Identifier: GPL-3.0-or-later

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use isocountry::CountryCode;
use rand::{RngCore, rngs::OsRng};
use serde::{Deserialize, Deserializer, Serializer};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use url::Host;

use crate::ansible::AnsibleConn;
use crate::constants::{ALPHABET, ANSIBLE_PORT, ROOT_USER};
use crate::types::{KetServer, Secrets, ServerState, SvcKind, XuiToken};

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

impl KetServer {
    // =============================================================
    /// The default country code for a [`KetServer`] is USA
    pub fn default_country_code() -> CountryCode {
        CountryCode::USA
    }
    // =============================================================
    /// All service kinds this server holds
    pub fn svc_kinds(&self) -> Vec<SvcKind> {
        let mut v = vec![SvcKind::Ssh, SvcKind::Nginx];
        for inbound in &self.inbounds {
            v.push(SvcKind::Xray(inbound.name.clone()));
        }

        v
    }
    // =============================================================
    /// The number of service kinds this server holds
    pub fn svc_num(&self) -> usize {
        self.svc_kinds().len()
    }
    // =============================================================
    /// All service kinds of this server in a vector of strings
    pub fn svc_kinds_string(&self) -> Vec<String> {
        self.svc_kinds()
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }
    // =============================================================
    /// Computes the DNS hostname
    pub fn dnshostname(&self) -> String {
        self.dns_subdomain.clone() + "." + &self.domain.to_string()
    }
    // =============================================================
    /// Computes the UI hostname
    pub fn uihostname(&self) -> String {
        self.ui_subdomain.clone() + "." + &self.domain.to_string()
    }
    // =============================================================
    /// Computes the subscription hostname
    pub fn subhostname(&self) -> String {
        self.sub_subdomain.clone() + "." + &self.domain.to_string()
    }
    // =============================================================
    /// Parsing function for reading the domain as url::Host
    pub fn deserialize_host<'de, D>(domain: D) -> Result<Host<String>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(domain)?;
        Host::parse(&s).map_err(serde::de::Error::custom)
    }
    // =============================================================
    /// Serializer function for domain as url::Host to string
    pub fn serialize_host<S>(host: &Host, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&host.to_string())
    }
    // =============================================================
    /// A function that produces a deterministic, derived value from a server's
    /// name, domain and subdomains. The value is used as a random path for
    /// Nginx health checking.
    pub fn health_path_gen(&self) -> String {
        let seed = format!(
            "name={}|domain={}|dsub={}|usub={}|ssub={}",
            // The server name and subdomains are already enforced to be proper
            // names and not have spaces.
            self.name.clone(),
            self.domain.to_string().trim(),
            self.dns_subdomain.clone(),
            self.ui_subdomain.clone(),
            self.sub_subdomain.clone(),
        );

        let digest = Sha256::digest(seed.as_bytes());
        let hex = format!("{:x}", digest);

        hex[..16].to_string()
    }
    // =============================================================
    /// Populates the struct of [`Secrets`] for a [`KetServer`]
    pub fn fill_secrets(&mut self) {
        let mut mother_subids = HashMap::<String, String>::new();
        for inbound in &self.inbounds {
            mother_subids
                .entry(inbound.name.clone())
                .or_insert(Self::secret_gen(7));
        }
        // Adding a dummy secret for the representative Xray inbound
        // a.k.a. super Xray.
        let super_name = SvcKind::super_name();
        mother_subids
            .entry(super_name.clone())
            .or_insert(super_name);

        self.secrets = Secrets {
            health_path: self.health_path_gen(),
            doh_endpoint: Self::secret_gen(16),
            xui_username: Self::username_gen(16),
            xui_password: Self::secret_gen(16),
            // xui_token will be initialized when the server becomes Production
            xui_token: XuiToken::empty(),
            xui_webpath: Self::secret_gen(10),
            xui_subpath: Self::secret_gen(8),
            xui_jsubpath: Self::secret_gen(8),
            mother_subids,
        };
    }
    // =============================================================
    /// Creates a random string suitable for URLs and passwords having
    /// security strength of nbytes
    fn secret_gen(nbytes: usize) -> String {
        let mut bytes = vec![0u8; nbytes];
        OsRng.fill_bytes(&mut bytes);

        URL_SAFE_NO_PAD.encode(bytes)
    }
    // =============================================================
    /// Creates a random string from alphanumeric characters suitable for
    /// usernames. For random passwords and URL paths, [`secret_gen`] will be
    /// used.
    fn username_gen(len: usize) -> String {
        let mut random_str = String::with_capacity(len);

        while random_str.len() < len {
            let mut b = [0u8; 1];
            OsRng.fill_bytes(&mut b);

            let indx = (b[0] as usize) % ALPHABET.len();
            random_str.push(ALPHABET[indx] as char);
        }

        random_str
    }
    // =============================================================
    /// Builds the Ansible inventory
    pub fn build_inventory(&mut self) {
        let ansible_conn = AnsibleConn {
            ansible_host: Some(self.ip.to_string()),
            ansible_port: {
                match self.state.load() {
                    ServerState::Offgrid => Some(ANSIBLE_PORT),
                    ServerState::Production => Some(self.ssh_port),
                }
            },
            ansible_user: Some(ROOT_USER),
            ansible_connection: None,
        };

        self.ansible_conn.store(Some(Arc::new(ansible_conn)));
    }
}

// =============================================================
