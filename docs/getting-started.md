# Getting Started

This guide is the shortest path from a fresh controller to a managed
Production server.

## Prerequisites

### Controller

Requirements:

- Linux
- [Xray](https://github.com/XTLS/Xray-core/releases/) >= 26
- [Ansible](https://docs.ansible.com/projects/ansible/latest/installation_guide/intro_installation.html) >= core 2.19.0
- Cloudflare token

The controller can be any recent 64bit Linux distribution, as long as Xray, Ansible,
and xcplane can run on it.

The Cloudflare token must have the following levels of access/permission:

| Scope   | Item                 | level | 
|---------|----------------------|-------|
| Zone    | DNS                  | Edit  |
| Account | Account Rulesets     | Edit  |
| Account | Account Filter Lists | Edit  |

If the user never intends to enable `cloudflare-proxied` for any server in the
cloud configuration file, only DNS permission will be required.

### Managed servers

Requirements:

- Ubuntu 24.04 or newer
- systemd
- SSH access from the controller
- Public IPv4 and/or IPv6

**The controller's public SSH key must be installed on the root@server, and SSH must initially be listening on port 22**.
When the server becomes Production, the port will be changed to the specified
value, or to 79 as the default port.

The server can be a VPS or a dedicated server from any provider, or even a
self-hosted [virtual] machine, as long as its global IP routing works as
intended. It can be IPv4-only or IPv6-only, or it may support both. When
choosing a server, the user is advised to always prioritize network connection
quality over other specs of the server.

**Docker is not supported**. This is because xcplane treats infrastructure not
as something that is deployed once, but as a living system that continuously
evolves toward its desired state. In other words, Docker is built for deploying
workloads, while xcplane is built for evolving infrastructure.

#### Ubuntu on servers
For now, Ubuntu is the only supported distribution on the servers.

## Installing xcplane on the controller

You may either download the binary from the releases, or clone the repository
and build it from source using

```sh
cargo build -r
```
When building from source, make sure Cargo and Rust >= 1.80 are installed on the
controller.

The binary will be built in `./target/release/xcplane` which can be copied
anywhere as a standalone program.

## Workspace and the first run

On the first run, xcplane determines the working directories and creates:
- the workspace
- an example configuration
- Cloudflare auth file where the token should be placed

*xcplane does not need root.* If invoked as a normal user, the workspace will be
the standard XDG directories:

- Reading config files from ~/.config/xcplane/
- Storing data in ~/.local/share/xcplane/
- Storing cache in ~/.cache/xcplane/
- Storing runtime states in /run/user/{uid}/xcplane/
- Storing logs in ~/.local/state/xcplane/

As root, it will work with FHS directories:

- Reading config files from /etc/xcplane/
- Storing data in /var/lib/xcplane/
- Storing cache in /var/cache/xcplane/
- Storing runtime data in /run/xcplane/
- Storing logs in /var/log/xcplane/

Just run `xcplane` or `xcplane daemon` and it will complete the first run.

## Add a server

A newly declared server starts in **Offgrid** state. For a minimum cloud
configuration, at least one cloud inbound, and one server must be
defined. Create `cloud.toml` file in the config directory:

**`~/.config/xcplane/cloud.toml`**
```toml
[[settings.inbound-set]]
name = "inbound_name" # a unique name among inbounds
total = 25            # total number of clients for this inbound
port = 2053           # the port on which the inbound will serve

[[servers]]
name = "server1"                # a unique name among servers
ip = "2a14:cde4:1325:17dd::1"   # the server's public IP 
domain = "this.deep.domain.xyz" # a domain whose TLD is already added to Cloudflare

```

Note that the domain does not need to be the exact TLD (top-level domain) that
you have acquired. For example, if you have bought domain.com you can set any
deep level of the TLD as the server domain (as long as the standards allow).

Refer to [Configuration](configuration.md) for more information, and to explore
other fields and the default values.

## Expand the cloud

To expand the production cloud (which is empty now), we provision servers
one-by-one using:

```sh
xcplane expand <server-name>
```

or just `xcplane expand`

The daemon constructs the required Ansible work and provisions the server.
Once the setup is complete, the server enters **Production** state and normal
monitoring begins.

## Verify

Once the cloud has at least one Production server, you can check its status, see
the credentials, and list the clients the cloud has created.

Check the health status of the cloud:
```sh
xcplane status
```

See the credentials of the cloud using:
```sh
xcplane creds -s
```

The clients of the production cloud can be viewed via:
```sh
xcplane clients
```

At this point, xcplane is monitoring the Production server, and
you can use the private DNS-over-HTTPS server, and the Xray clients it has
created.

## Next steps

- Read [Configuration](configuration.md)
- Read [Server lifecycle](server-lifecycle.md).
- Read [Reconciliation](reconciliation.md).
