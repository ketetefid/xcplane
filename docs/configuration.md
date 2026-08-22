# Configuration

xcplane uses a declarative cloud configuration in TOML format.

## Cloud configuration

The configuration describes the desired cloud, including servers and global
settings.

The configuration has two major parts: Global settings and servers
definitions. Some items have default values, while the others must be defined.

### Global settings

Global settings are comprised of global cloud options and the pool of Xray inbounds.

The global cloud options are:
```toml
[settings]
# Enable automated corrective actions when a service fails repeatedly (default: true)
auto-fix = true

# Number of consecutive failures before triggering automatic fixes (default: 10)
fix-threshold = 20

# Monitoring interval between health checks in seconds (default: 60)
monitor-interval = 120
```

The pool of Xray inbounds defines what inbounds servers can choose. A server can
define its inbounds explicitly or implicitly:

- If no inbounds are defined for a server, all of the cloud pool will be
assigned to that server.
- If inbounds are defined explicitly, only the defined ones will be assigned to
the server. The chosen inbounds must be from the cloud pool.

Each inbound in the pool will have `[[settings.inbound-set]]` header:

```toml
[[settings.inbound-set]]
name = "3day"            # Unique name among inbounds
port = 2083              # The connection port
total = 10               # Total number of clients for this inbound

kind = "VlessTcpReality" # The kind of inbound. Five are available:
                         # VlessXhttpReality (default), VlessTcpReality, VlessXhttpTls
		            	 # VlessWsTls, VlessGrpcTls

traffic = 20             # Total traffic allowance in GB. 0 means unlimited (default: 0)

expiry = 3               # Client account duration in days after first use. 0 means it
                         # never expires (default: 0)

comment = "trial"        # Comment about the inbound (default: "")

```

### Servers definition

Each server definition will have two parts: server specification and server inbounds.

The specification has the following items:

```toml
[[servers]]
# A unique name for the server
name = "xray_node_us_east_1"

# The server's public IPv4 or IPv6 address
ip = "203.0.113.10"

# Base domain for services which must be managed by Cloudflare. It can be
# a subdomain of the TLD
domain = "example.com"

# ISO country code (default: USA)
region = "US"

# State: 'Offgrid' (not provisioned/managed yet) or 'Production' (being
# monitored and ready to serve traffic) (default: Offgrid)
state = "Offgrid"

# Traffic quota in Terabytes per month. A value of 0 indicates infinite
# traffic allowance (default: 0)
quota = 10

# Whether this server is active for management and monitoring (default:
# true)
enabled = true

# The 'intended' SSH port configuration (default: 79)
ssh-port = 91

# Subdomain specific to DNS over HTTPS a.k.a DoH (default: dns)
dns-subdomain = "doh"

# UI Panel subdomain for XUI web access (default: ui)
ui-subdomain = "x-ui"

# Subscription service subdomain (default: sub)
sub-subdomain = "xsub"

# Cloudflare proxy setting for DNS (oranged proxy) and network traffic
# (enforced by firewall) (default: false)
cloudflare-proxied = true

# The list of countries to which new outbound connections will be blocked.
# Users from these countries can freely connect, but the server will not load
# any external service hosted in these countries (default: CN, RU, IR)
outbound-block = [ "CN", "IR", "RU" ]

```

The inbounds defined for the server must be chosen from the cloud pool. Only
name and port must match and the other items can be customized. A server that
has no explicit inbounds defined will take the cloud pool.

The server inbounds are defined like the pool of inbounds. Only the header will be different:

```toml
[[servers.inbounds]]
name = "3day"
port = 2083
total = 1000
kind = "VlessXhttpTls"
traffic = 2000
expiry = 365
comment = "trial"
```

## Server state

Servers have an explicit lifecycle state:

- **Offgrid** — declared in the cloud but not yet provisioned.
- **Production** — provisioned and eligible for normal monitoring and
  operational management.

A disabled server (`enabled = false`) is excluded from management and
monitoring regardless of whether it is Offgrid or Production.

## Secrets

Secrets are generated randomly for servers, and once servers are provisioned, 
Production secrets such as usernames, passwords, DoH endpoints, and
subscription paths remain stable for the lifetime of a Production server.
Any reconciliation mode does not replace these values.

- The endpoint of the DoH server (doh_endpoint)
- 3XUI panel username (xui_username)
- 3XUI panel password (xui_password)
- 3XUI API token (xui_token)
- The random path at which 3XUI panel is accessible (xui_webpath)
- The random path for sublinks in the subscription server (xui_subpath)
- The random path for json sublinks in the subscription server (xui_jsubpath)
- The mother inbound subscription IDs for each inbound of the server (mother_subids)

## Derived values

Some values are deterministic derivatives rather than independent
configuration. Nginx health path (health_path) is derived from the server's
domain and subdomains.

## Super inbound
Each production server has an extra virtual inbound named `super` which is the
representative of all inbounds and performs actions on behalf of them for the
whole Xray/XUI daemon. The actions include backing up the XUI DB, and applying
corrective actions. The super inbound is automatically added by xcplane.

## Storing and protecting secrets/credentials

Servers secrets and derived values are stored in the cloud database in data
directory. Data, config and log directories are strictly permissioned `700`, and
the DB is set to `600`.

`$HOME/.local/share/xcplane/cloud.db`
or
`/var/lib/xcplane/cloud.db`

The Cloudflare auth file permission is also set to `600` in Config directory:

`$HOME/.config/xcplane/cf-auth.toml`
or
`/etc/xcplane/cf-auth.toml`

**The user is advised to handle the cloud database with extreme care when copying it to another place, not to accidentally expose it.**

The DB does not store the user's Cloudflare token.

## Validation

The new cloud is fully validated before reconciliation. The user can also
validate the configuration file manually using:

```sh
xcplane check
```

## Non-primary parameters
Some specifications are informative fields, and do not play a role in management:

- quota
- region

> TODO: common configuration errors.

## Read more
[Corrective Actions](corrective-actions.md)
