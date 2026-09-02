# Ansible Integration

Ansible is xcplane's system-level execution mechanism.

## Design principle

The declarative cloud, reconciliation logic, runtime state, and resulting
operational state belong to xcplane. Ansible is not the source of truth for the
fleet.

xcplane decides **what** needs to happen; Ansible performs the corresponding
remote system operations.

## AnsibleRun

`AnsibleRun` is the structure that performs a series of `AnsibleAction`s
according to the following procedure:

1. receives the requested xcplane actions;
2. maps actions to Ansible task files;
3. prepares the variables passed to Ansible;
4. executes the run;
5. returns the results and logs to xcplane.

xcplane then processes and persists the results.

## AnsibleAction

The current action model includes the following in order:

| ID |    AnsibleAction   |
| -- | ------------------ |
| 0  | PortsCheck         |
| 1  | DnsReset           |
| 2  | Basics             |
| 3  | CloudflareDel      |
| 4  | Cloudflare         |
| 5  | Acme               |
| 6  | Nginx              |
| 7  | DoH                |
| 8  | FirewallBootstrap  |
| 9  | OutblockedUpdate   |
| 10 | BaseSetup          |
| 11 | FullSetup          |
| 12 | NginxRestart       |
| 13 | NginxRestoreConfig |
| 14 | XrayRestart        |
| 15 | XrayRestoreDB      |
| 16 | Bootstrap          |
| 17 | XuiAuth            |
| 18 | DelInbound         |
| 19 | AddInbound         |
| 20 | PanelSettings      |
| 21 | Fail2ban           |
| 22 | Firewall           |
| 23 | SSH                |
| 24 | DestroyServer      |

The exact composition of actions depends on the operation being performed where
one or a group of these actions are selected.

When performing a group of actions, their order is preserved meaning the earlier
actions take precedence over the later ones.

## Mapping to task files

```text
FullSetup = Fully provisions a server using "full_setup.yaml";
BaseSetup = Prepares the base of server; used in FullSetup as "base_setup.yaml";
Basics = Installs basic packages on the server using "basics_install.yaml";
PortsCheck = Checks if xcplane-managed ports are free on the server or not using "check_ports.yaml";
DnsReset = Resets DNS to Google & Cloudflare using "dns_reset.yaml";
CloudflareDel = Deletes DNS records using "cloudflare_del.yaml";
Cloudflare = Manages DNS records using "cloudflare_setup.yaml";
Acme = Manages certificates using "acme_get_cert.yaml";
SSH = Sets up SSH using "ssh_setup.yaml";
Nginx = Sets up Nginx using "nginx_setup.yaml";
DoH = Sets up DNS-over-HTTPS server using "doh_setup.yaml";
Bootstrap = Bootstraps system services using "bootstrap.yaml";
Fail2ban = Manages fail2ban using "fail2ban_setup.yaml";
Firewall = Deploys the final form of firewall using "firewall.yaml";
FirewallBootstrap = Deploys the skeleton of firewall using "firewall_bootstrap.yaml";
OutblockedUpdate = Updates the direct outbound connection set in firewall using "outblocked_update.yaml";
XuiLogin = Performs a cookie-based login to the XUI panel using "xui_login.yaml";
XuiAuth = Prepares header-bashed authorization for the XUI panel using "xui_auth.yaml";
PanelSettings = Sets up XUI panel settings using "xui_setup_panel.yaml";
AddInbound = Adds an inbound using "xui_add_inbound.yaml";
DelInbound = Deletes an inbound using "xui_del_inbound.yaml";
NginxRestart = Restarts Nginx as a corrective action using "nginx_restart.yaml";
NginxRestoreConfig = Restores Nginx config file as a corrective action using "nginx_restore_config.yaml";
XrayRestart = Restarts XUI panel as a corrective action using "xui_restart.yaml";
XrayRestoreDB = Restores XUI DB as a corrective action using "xui_restore_db.yaml";
DestroyServer = Destroys a Production server and turns it into Offgrid using "destroy_server.yaml";
```
## Passed variables to Ansible

For every AnsibleRun a group of required variables is passed to Ansible. For
reference, these data are stored in a folder named as the server name in the data
directory (sensitive data are redacted first). For example:
`server1-0_2_3_5_6_16_17_18_19_20_21_22-general_vars.yaml`

Which shows what data was passed for what group of AnsibleAction's.
Furthermore, the inventory file can be found in the same directory with the same
ID group.

## Logging Ansible output

When Ansible has finished an AnsibleRun, the output log is stored in the log
directory as:

`<server nane>-<IDs>-<Date and itme>-ansible-stdout.log`

and 

`<server nane>-<IDs>-<Date and itme>-ansible-stderr.log`


For example:
`myserver-2_4_7_18_19-2026_08_09_11_54_28-ansible-stderr.log`

## Read more
[Corrective Actions](corrective-actions.md)
