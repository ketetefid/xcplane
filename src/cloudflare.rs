// SPDX-License-Identifier: GPL-3.0-or-later

use reqwest::{Client, header as Header};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, sync::Arc};
use tokio::time::Duration;
use tracing::{debug, error, info, instrument};

use crate::constants::{CLOUDFLARE_SPORTS, XCPLANE_AGENT};
use crate::types::{BoxError, Cloud, KetServer, SvcKind};

//////////////////////////////////////////////////////

/// Holds the response in a GET call to Cloudflare API
#[derive(Deserialize, Debug)]
pub struct CFGetResponse<T> {
    #[serde(default)]
    pub result: Vec<T>,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<CFError>,
}

/// Holds the response in a POST call to Cloudflare API
#[derive(Deserialize, Debug)]
pub struct CFPostResponse<T> {
    pub result: T,
    pub success: bool,
    #[serde(default)]
    pub errors: Vec<CFError>,
}

//////////////////////////////////////////////////////

/// Holds the information about the Cloudflare account returned in API calls
#[derive(Debug, Deserialize, Default)]
pub struct CFAccount {
    pub id: String,
    pub name: String,
}

/// Holds Cloudflare error code and message
#[derive(Deserialize, Debug)]
pub struct CFError {
    pub code: i32,
    pub message: String,
}

/// Holds Cloudflare token verification status
#[derive(Deserialize, Default)]
pub struct CFToken {
    pub status: String,
    pub id: String,
}

/// The query to the Cloudflare Zone API is a vector of this struct
#[derive(Debug, Deserialize, Default)]
pub struct CFZone {
    pub id: String,
    pub name: String,
    pub account: CFAccount,
}

/// A Cloudflare list struct
#[derive(Deserialize, Clone, Debug, Default)]
pub struct CFList {
    pub id: String,
    pub name: String,
    pub description: String,
    pub kind: String,
    pub num_items: u32,
}

/// Holds information about a Cloudflare ruleset in a zone
#[derive(Debug, Deserialize, Default, Clone)]
pub struct CFRuleSet {
    pub id: String,
    pub kind: String,
    pub name: String,
    pub phase: String,
    // Sometimes the API response lacks this field
    #[serde(default)]
    pub rules: Vec<CFRule>,
}

/// Holds information about a Cloudflare rule in a ruleset in a zone
#[derive(Debug, Deserialize, Default, Clone)]
pub struct CFRule {
    pub id: String,
    pub action: String,
    pub description: String,
    pub enabled: bool,
    pub expression: String,
}

/// The CFState struct is comprised of these always required fields:
///   - zone name
///   - zone ID
///   - if the server's domain is a too deep subdomain of the zone
///   - if all the inbound ports are a subset of Cloudflare ports
///
/// The optional list data is only needed if a server wants Cloudflare
/// proxy on top of DNS management.
#[derive(Debug, Serialize, Clone)]
pub struct CFState {
    pub zone_name: String,
    pub zone_id: String,
    pub too_deep: bool,
    pub cloudflare_only: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub list_data: Option<CFListData>,
}

/// This struct contains necessary information when the orange Cloudflare proxy
/// is enabled for a server (cloudflare_proxied). Note that for fail2ban
/// deployment, only the list_id will be used.
#[derive(Debug, Serialize, Clone)]
pub struct CFListData {
    pub account_name: String,
    pub account_id: String,
    pub ruleset_id: String,
    pub rule_id: String,
    pub list_id: String,
}

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

impl KetServer {
    /// Gets the Cloudflare zone according to the server's domain
    #[instrument(name = "zone", skip_all)]
    pub async fn get_cf_zone(&self, cf_client: &Client) -> Result<CFZone, BoxError> {
        let cf_endpoint = "https://api.cloudflare.com/client/v4/zones";

        let res = cf_client
            .get(cf_endpoint)
            .send()
            .await?
            .error_for_status()
            .inspect_err(|e| error!(error = %e, "Cloudflare API token is likely invalid"))?
            .text()
            .await?;

        let domain_str = self.domain.to_string().to_lowercase();
        let zone_res = serde_json::from_str::<CFGetResponse<CFZone>>(&res)?;

        let zone = zone_res
            .result
            .into_iter()
            .filter(|zone| {
                let zone_name = zone.name.to_lowercase();
                zone_name == domain_str || domain_str.ends_with(&format!(".{}", zone_name))
            })
            // The longest matching suffix is the most specific zone
            .max_by_key(|zone| zone.name.len())
            .ok_or(format!(
                "Domain '{}' is not within any Cloudflare zone. \
		 Have you added it to Cloudflare properly?",
                domain_str
            ))?;

        debug!(id = zone.id, "zone was found");

        Ok(zone)
    }
    // =============================================================
    /// Gets or creates the xcplane-managed Cloudflare ruleset belonging to the
    /// domain of this server
    #[instrument(name = "ruleset", skip_all)]
    pub async fn get_cf_ruleset(
        &self,
        cf_client: &Client,
        zone_id: &str,
    ) -> Result<String, BoxError> {
        // Note: a list is global to all zones, but a ruleset and a rule are
        // specific to a zone.
        let cf_endpoint = format!(
            "https://api.cloudflare.com/client/v4/zones/{}/rulesets",
            zone_id
        );

        let cf_ruleset_res = cf_client
            .get(&cf_endpoint)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        let cf_rulesets = serde_json::from_str::<CFGetResponse<CFRuleSet>>(&cf_ruleset_res)?;

        let ruleset_opt = cf_rulesets.result.iter().find(|ruleset| {
            ruleset.kind == "zone" && ruleset.phase == "http_request_firewall_custom"
        });

        let the_ruleset = match ruleset_opt {
            Some(ruleset) => {
                info!(status = "found", id = ruleset.id);
                ruleset.to_owned()
            }
            None => {
                // We need to create our ruleset
                let json_body = serde_json::json!
                    ({"name": "xcplane",
                      "kind": "zone",
                      "phase": "http_request_firewall_custom"});

                // The same call above but with POST. Note: 'Content-Type:
                // application/json' is automatically added in reqwest when
                // .json is used
                let cf_ruleset_res = cf_client
                    .post(&cf_endpoint)
                    .json(&json_body)
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await?;

                let cf_ruleset =
                    serde_json::from_str::<CFPostResponse<CFRuleSet>>(&cf_ruleset_res)?;

                info!(status = "created", id = cf_ruleset.result.id);
                cf_ruleset.result
            }
        };

        // Only the ruleset ID is needed for further operations
        Ok(the_ruleset.id)
    }
    // =============================================================
    /// Gets or creates the xcplane-managed Cloudflare rule in the given
    /// ruleset that belongs to the domain (zone) of this server
    #[instrument(name = "rule", skip_all)]
    pub async fn get_cf_rule(
        &self,
        cf_client: &Client,
        zone_id: &str,
        ruleset_id: &str,
    ) -> Result<String, BoxError> {
        let cf_endpoint = format!(
            "https://api.cloudflare.com/client/v4/zones/{}/rulesets/{}",
            &zone_id, &ruleset_id
        );

        // Note that it returns still a Ruleset
        let cf_ruleset_res = cf_client
            .get(&cf_endpoint)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;

        // It was a GET method, but the call fits a POST response shape
        let cf_ruleset = serde_json::from_str::<CFPostResponse<CFRuleSet>>(&cf_ruleset_res)?;

        // Now we need to extract the rules vector and check for our own rule
        let rule_opt = cf_ruleset.result.rules.iter().find(|rule| {
            rule.action == "block"
                && rule.description == "xcplane-managed fail2ban rule"
                && rule.enabled
        });

        let the_rule = match rule_opt {
            Some(rule) => {
                info!(status = "found", id = rule.id);
                rule.to_owned()
            }
            None => {
                // We need to create our fail2ban rule
                let json_body = serde_json::json!({
                  "action":"block",
                  "expression":"(ip.src in $xcplane)",
                  "description":"xcplane-managed fail2ban rule",
                  "enabled":true
                });

                // This time the endpoint is different
                let cf_endpoint = format!(
                    "https://api.cloudflare.com/client/v4/zones/{}/rulesets/{}/rules",
                    zone_id, ruleset_id
                );

                let cf_ruleset_res = cf_client
                    .post(&cf_endpoint)
                    .json(&json_body)
                    .send()
                    .await?
                    .error_for_status()?
                    .text()
                    .await?;
                // It returns a Ruleset response
                let cf_ruleset =
                    serde_json::from_str::<CFPostResponse<CFRuleSet>>(&cf_ruleset_res)?;

                // And we need to extract our rule again
                let cf_rule = cf_ruleset
                    .result
                    .rules
                    .iter()
                    .find(|&rule| {
                        rule.action == "block"
                            && rule.description == "xcplane-managed fail2ban rule"
                            && rule.enabled
                    })
                    .cloned()
                    .ok_or::<BoxError>(
                        format!(
                            "Couldn't create a fail2ban rule for Cloudflare-proxied server '{}'",
                            self.name
                        )
                        .into(),
                    )?;
                info!(status = "created", id = cf_rule.id);

                cf_rule
            }
        };

        // At this point our work is done. The rule just needs to exist, and
        // fail2ban will need the list, not a ruleset or rule.
        Ok(the_rule.id)
    }
    // =============================================================
    /// Stores the required Cloudflare parameters needed for DNS configuration
    /// and optional connection proxy management (oranged proxy)
    #[instrument(name = "set_state", skip_all, fields(server = %self.name))]
    pub async fn set_cf_state(
        &self,
        cf_client: &Client,
        cf_list: &Option<String>,
    ) -> Result<(), BoxError> {
        // 1. Check if the inbound ports of this server are a subset of the
        // Cloudflare secure ports, and if all of its inbounds are Cloudflare
        // compatible

        let mut inbounds_ports_hs = HashSet::<u16>::new();
        let mut all_cloudflare_compatible = true;
        // Excluding the super inbound
        for inbound in self
            .inbounds
            .iter()
            .filter(|inb| inb.name != SvcKind::super_name())
        {
            // Collecting the inbound ports
            inbounds_ports_hs.insert(inbound.port);
            // Are all inbounds compatible with Cloudflare?
            if !inbound.kind.cloudflare_compatible() {
                all_cloudflare_compatible = false;
            }
        }

        let mut cf_ports_hs = HashSet::<u16>::new();
        for port in CLOUDFLARE_SPORTS {
            cf_ports_hs.insert(port);
        }

        // A server is cloudflare_only if both are true:
        // - all of its inbounds are using Cloudflare ports
        // - all of its inbounds are Cloudflare compatible
        let cloudflare_only =
            if inbounds_ports_hs.is_subset(&cf_ports_hs) && all_cloudflare_compatible {
                true
            } else {
                false
            };

        // 2. Compute the zone parameters for this server's domain
        let cf_zone = self.get_cf_zone(cf_client).await?;
        let zone_id = cf_zone.id;
        let zone_name = cf_zone.name;

        // 3. Check if the domain is a too deep subdomain of the Cloudflare zone
        let domain_str = self.domain.to_string().to_lowercase();
        let nested_subdomains = domain_str
            .strip_suffix(&zone_name)
            .and_then(|s| s.strip_suffix('.'))
            .map(|s| s.split('.').count())
            .unwrap_or(0);

        let too_deep = nested_subdomains > 0;

        /*
        4. Constructing Cloudflare state data

        If Cloudflare proxy is requested for this server, we ensure a ruleset
        and a rule exist in its zone.

        A 'list' is a global list of IPs for an 'account'. The list is referenced
        by 'rules' in 'rulesets' in different zones. A specific list should have
        already been fetched/created by the daemon, so what we do is:

        - Fetch or create our specific ruleset for this zone
        - Fetch or create our specific rule in our specific ruleset. This rule
          will reference the list created by the daemon.

        Note that fail2ban only needs the list, but it's our duty to connect the
        dots for it to work properly. Also note that while the user has enabled
        the proxy, the server's domain might be too deeply nested (too_deep) and
        the app will automatically disable the orange proxy for Ansible. Having
        said that, if the user has requested it, we will create the ruleset &
        the rule for the zone anyway.

        Also bear in mind that for a free-tier Cloudflare account, maximum one
        ruleset is allowed. So when we create a list, all servers will share
        that one list. This is a double-edged sword--don't lock yourself out of
        your entire cloud.
         */

        let mut cf_list_data = None;

        if self.cloudflare_proxied {
            let ruleset_id = self.get_cf_ruleset(cf_client, &zone_id).await?;
            let rule_id = self.get_cf_rule(cf_client, &zone_id, &ruleset_id).await?;
            let list_id = cf_list
                .as_ref()
                .ok_or("cf_list is None while it should contain Some value.")?
                .to_owned();

            cf_list_data = Some(CFListData {
                account_name: cf_zone.account.name,
                account_id: cf_zone.account.id,
                ruleset_id,
                rule_id,
                list_id,
            });
        }

        let cfstate = CFState {
            zone_name,
            zone_id,
            too_deep,
            cloudflare_only,
            list_data: cf_list_data,
        };

        self.cfstate.store(Some(Arc::new(cfstate)));

        debug!("state is set.");
        Ok(())
    }
}
// =============================================================
/// For the given cloud, this function gets/creates a Cloudflare list if
/// necessary, and then sets the Cloudflare state for all of its servers
pub async fn prepare_cloudflare_state(
    cloud: &Cloud,
    cf_client: &Client,
    cf_list: &mut Option<String>,
) -> Result<(), BoxError> {
    info!("preparing Cloudflare states of the cloud servers");
    // If any server wants the Cloudflare proxy, we need to prepare a global
    // list for its fail2ban setup.
    if cloud.servers.iter().any(|s| s.cloudflare_proxied) {
        match prepare_cf_list(&cloud, cf_client).await {
            Ok(id) => *cf_list = Some(id),
            Err(e) => {
                // We issue a log only, as this might be caused by several
                // factors such as when the user already has another list in his
                // free account, or when the token has insufficient permissions.
                error!(error = e, "Cloudflare block list setup failed");
            }
        }
    }

    // Compute the needed Cloudflare parameters for all servers
    for server in &cloud.servers {
        server.set_cf_state(&cf_client, &cf_list).await?;
    }

    debug!("All Cloudflare states are set.");
    Ok(())
}
// =============================================================
/// Computes the Cloudflare account ID and retrieves or creates a block list
#[instrument(name = "list", skip_all)]
async fn prepare_cf_list(cloud: &Cloud, cf_client: &Client) -> Result<String, BoxError> {
    // Extracting account_id by calling get_cf_zone on the first server (every
    // server's domain must already be added to Cloudflare)
    let cfzone = cloud.servers[0].get_cf_zone(cf_client).await?;
    let account_id = cfzone.account.id;

    // Getting the available lists in the account
    let cf_endpoint = format!(
        "https://api.cloudflare.com/client/v4/accounts/{}/rules/lists",
        account_id
    );

    let cf_lists_res = cf_client
        .get(&cf_endpoint)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    let cf_lists = serde_json::from_str::<CFGetResponse<CFList>>(&cf_lists_res)?;

    let the_list_opt = cf_lists
        .result
        .iter()
        // Cloudflare sometimes changes fields and matching by description is
        // the most reliable
        .find(|list| {
            list.description == "xcplane-managed blocklist" && list.kind == "ip" && cf_lists.success
        });

    let the_list = match the_list_opt {
        Some(list) => {
            info!(status = "found", id = list.id);
            list.to_owned()
        }
        None => {
            // No such list has been found--we're creating it
            let json_body = serde_json::json!({
            "name": "xcplane",
            "description": "xcplane-managed blocklist",
            "kind": "ip"
            });

            // The same call above but with POST creates a list
            let cf_list_res = cf_client
                .post(&cf_endpoint)
                .json(&json_body)
                .send()
                .await?
                .error_for_status()?
                .text()
                .await?;

            let cf_list = serde_json::from_str::<CFPostResponse<CFList>>(&cf_list_res)?;

            info!(status = "created", id = cf_list.result.id);
            cf_list.result
        }
    };

    Ok(the_list.id)
}
// =============================================================
/// Checks if the supplied Cloudflare token is valid or not
pub async fn test_cf_token(cf_client: &Client) -> Result<(), BoxError> {
    let cf_endpoint = "https://api.cloudflare.com/client/v4/user/tokens/verify";

    let res = cf_client
        .get(cf_endpoint)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;

    // Note: this is a result of GET method, but the response doesn't return a
    // vector and instead matches a Cloudflare POST response.
    let verify_res = serde_json::from_str::<CFPostResponse<CFToken>>(&res).inspect_err(|e| {
        error!(
            error = %e,
            "Cloudflare API token either is invalid or has expired"
        )
    })?;

    if verify_res.success && verify_res.result.status == "active" {
        debug!("Cloudflare token is valid");
        return Ok(());
    }

    Err("The Cloudflare token either is invalid or has expired.".into())
}
// =============================================================
/// Constructs a header-based authorizing client which all cloud servers will
/// use for their Cloudflare contacts
pub fn create_cf_client(token: &str) -> Result<Client, BoxError> {
    let mut headers = Header::HeaderMap::new();
    let mut auth_value = Header::HeaderValue::from_str(&format!("Bearer {}", token))?;
    auth_value.set_sensitive(true);
    headers.insert(Header::AUTHORIZATION, auth_value);
    headers.insert(
        Header::ACCEPT,
        Header::HeaderValue::from_static("application/json"),
    );

    let client = Client::builder()
        .user_agent(XCPLANE_AGENT)
        .default_headers(headers)
        .timeout(Duration::from_secs(15))
        .build()?;

    debug!("Client for Cloudflare calls was created.");

    Ok(client)
}
// =============================================================
