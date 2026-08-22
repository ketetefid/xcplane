# Cloudflare

Cloudflare is an integral part of xcplane. It is necessarily and primarily
needed for managing DNS records. If the user requests Cloudflare proxy, it is
used for constructing firewall rules, too.

**Every server's domain's TLD/zone must be added to Cloudflare**.

xcplane interacts directly with Cloudflare, because the resulting Cloudflare
state is needed by the control plane. Only updating DNS records is delegated to
Ansible.

## Managed state

Known resources include:

- zone/server-specific Cloudflare state
- global block list
- rules
- rulesets
- DNS records

The `Cloudflare` and `CloudflareDel` Ansible actions perform required
DNS-entry updates on remote infrastructure through the Ansible workflow.

## Token permission

The token must have permission to:

- edit DNS in Zone
- edit Acount Rulesets in Account
- edit Account Filter Lists in Account 

If the user never intends to use Cloudflare proxy `cloudflare-proxied = true`
for any server, then only the first permission is required.

## Cloudflare proxy

Cloudflare proxy is utilized in two areas:

- Nginx HTTPS connection
- Inbound connection

When the user sets `cloudflare-proxied = true` for a server, the orange proxy
for UI & DNS hostnames are automatically enabled, and their Nginx servers will
only allow traffic from Cloudflare. However, the subscription server goes
behind the Cloudflare proxy only when all inbound connections can be proxied, too.

An inbound can be proxied through Cloudflare when both of the following conditions are true:

- The inbound kind is a Cloudflare compatible one
- The inbound port uses one of Cloudflare secure ports: `2053, 2083, 2087, 2096, 8443`

Then, the subscription server will be behind Cloudflare, and the firewall system
will only allow Cloudflare traffic to the whole system.

## Overriding Cloudflare proxy

If the domain used by a server is a too deep subdomain of the Cloudflare zone
(which is usually the TLD), then Cloudflare will not proxy the connections by
default, since such a setup needs paid features in Cloudflare.

Therefore, whenever the domain is a too deep subdomain of the zone/TLD,
`cloudflare-proxied` will be overridden to `false`.

## Cloudflare-compatible inbounds

|  Inbound Kind     | Compatible |
| ----------------- | ---------- |
| VlessXhttpReality |     no     |
| VlessTcpReality   |     no     |
| VlessXhttpTls     |     yes    |
| VlessWsTls        |     yes    |
| VlessGrpcTls      |     yes    |
