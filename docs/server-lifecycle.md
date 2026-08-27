# Server Lifecycle

xcplane explicitly models the lifecycle of managed servers.

```text
          configuration
               |
               v
          +---------+
          | Offgrid |
          +----+----+
               |
  provisioned in 'xcplane expand'
               |
               v
         +------------+
         | Production |
         +------------+
               |
               v
          Monitoring
```

## Offgrid

An Offgrid server exists in the desired cloud but has not been fully
provisioned.

During normal startup/reload/remap, its declared specifications can be updated
from the new configuration.

Offgrid servers are not remotely changed by a Rebase.

## Production

A Production server has completed its required provisioning and participates
in normal monitoring and operational management.

A normal **Reload does not change Production server specifications**.

Production server identity and secrets remain stable across any reconciliation
mode.

## Expand

`expand` is the operation that takes an Offgrid server and performs its remote
provisioning through Ansible.

```text
xcplane expand <server-name>
```

If expand is invoked without a server name, it will take and provision any
enabled Offgrid server. If provisioning a specific server is intended, the name
should be put in the command e.g., `xcplane expand server1`.

## Disabled servers

A server with `enabled = false` is excluded from management and monitoring,
whether it is Offgrid or Production.

## Identity change

Once a server becomes Production, its identity is altered for its lifetime, and
in any reconciliation mode its secrets remain stable.

If re-provisioning a server is intended, the user must Remap to Offgrid
followed by a Reload in order to refresh the secrets. Because unless the secrets
are refreshed, xcplane will know that it has been Production before and
re-provisioning will not be performed on an ex-Production unit.

## Name and identity

The identity of a server is tied to its name, and a Production server cannot be
renamed. Changing a server's name equals deleting and readding it to the fleet,
and its identity will be lost in this process.

# Read more

[Reconciliation](reconciliation.md)
