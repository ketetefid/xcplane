# Networking and Security

xcplane treats networking and host security as part of infrastructure
management.

Managed concerns include:

- SSH configuration;
- firewall configuration;
- Nginx;
- TLS/ACME;
- Fail2Ban;
- DNS/DoH;
- Cloudflare integration;
- Xray networking and inbounds.

## Firewall

Firewall is managed by `nftables` on each server, and whenever a remote action
is performed through Ansible, the firewall is updated correspondingly. This
keeps the infrastructure always firewalled while maintaining its fluidity.

## SSH

SSH changes are deliberately treated as critical operations. The Ansible
workflow places destructive SSH changes late so that a failure earlier in a
run does not unnecessarily lock the controller out of the server.

## Nginx and TLS

Nginx and TLS configuration are fully managed by the daemon and xcplane keeps
them synchronized with the other working elements of the server such as firewall
and Cloudflare state.

Each server has three Nginx servers:
- DNS which is a reverse proxy to the private Unbound DoH and is used by the system itself
- UI which is a reverse proxy to the web interface of the application panel, and exposes the secret health-path
- Subscription which is a reverse proxy to the subscription engine of the panel

## Fail2ban

Fail2ban monitors the Nginx servers and SSH and protects them against external malicious attempts.

Current jails are:

- SSH jail
- nginx-exploit: catches combination of methods and paths not used in the server, along with forbidden ones
- nginx-unknownpath: bans any attempt to access a non-existing or not-whitelisted path
- nginx-badmethod: bans any attempt with a not-whitelisted method
- nginx-limit-req: bans any attempt with an excessive number of requests

Inbound connections are not monitored by fail2ban because in regions with
disrupted internet connection by censors, it might lead to false positives and
ban legitimate users.

### Cloudflare blocklist

When a server is put behind Cloudflare, xcplane creates needed rules/rulesets
for the server in Cloudflare along with a shared block list. Whenever a malicious
activity is logged and banned, a Cloudflare ban action is triggered which adds
the offending origin IP to the block list. The block list exists in the account
level of Cloudflare; therefore, all the fleet can update the list and benefit from
it.

Note: Since the current jails are strict, the user must be cautious not to lock
themselves out of their entire server fleet.

# Read more

[Cloudflare](cloudflare.md)
