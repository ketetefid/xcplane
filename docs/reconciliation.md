# Reconciliation

xcplane reconciles the declarative cloud configuration against the previously
known cloud.

The configuration file builds the **new cloud**. The cloud loaded from the
database or previous daemon state is the **old cloud**.

## General rules

- Cloud-level settings are taken from the new cloud.
- If the old cloud is empty, the new cloud becomes the basis for the result.
- Servers added to the new cloud are added.
- Servers removed from the new cloud are removed from management.
- A disabled server is excluded from management and monitoring.
- Changes to an existing server are handled through **Reload**, **Remap**, or **Rebase**. 

If server data should be retained without management, keeping the server in
the configuration with `enabled = false` is preferred over deleting it.

Note that performing Remap or Rebase requires explicit interaction with the
running daemon.

## Reload

Reload is the default reconciliation mode, including during startup.
Rules: 

- For existing Production servers, Reload does not change the server's specifications, other than enabling or disabling them.
- For Offgrid servers, Reload accepts the specifications from the new cloud.

**Purpose:** safely merge the new declarative configuration without touching
primary parameters of Production infrastructure.

## Remap

Remap is a forceful change to the local server specification.

It is intended for cases where the remote server has been changed manually and
the local cloud model should reflect those external changes.

**Purpose:** reflect externally changed infrastructure in the cloud model.

## Rebase

Rebase is effectively a Remap followed by forceful reconciliation of the
remote servers.

Rules:

- For Offgrid servers, no remote action is performed.
- For enabled Production servers, xcplane performs remote reconfiguration to converge the server with the requested specifications.

See [Rebase](rebase.md) for the supported changes.

## Reconciliation and identity

Once a server becomes Production, its secrets remain stable for its lifetime.
Remap and Rebase do not change usernames, passwords, DoH endpoints, or
subscription paths.

The inbound mother-sub-ID map may be updated during Rebase when new inbounds
are added.

The only exception is the Nginx health-path which is a deterministic value
derived from the server's domain and subdomains.

## Examples

Please see the [quick start video](media/xcplane.mp4) where all three forms of
reconciliation are performed.
