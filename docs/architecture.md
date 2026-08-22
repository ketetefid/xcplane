# Architecture

xcplane is a stateful infrastructure control plane for a fleet of Linux
servers.

## High-level model

```text
                         xcplane
                            |
              +-------------+-------------+
              |                           |
      Direct integrations          Ansible execution
              |                           |
       Cloudflare / panels         System-level actions
              |                           |
              +-------------+-------------+
                            |
                     Managed servers
```

The daemon owns the higher-level state, decisions, reconciliation, and
orchestration. Ansible performs system-level work when a task is better handled
through remote automation.

Managed servers serve independently of each other which is a choice for the sake
of privacy, and only controller initiates a connection to them.

## Daemon lifecycle

The daemon has four broad phases:

1. **Startup**
   - determine workspace
   - validate workspace
   - acquire daemon lock
   - create/listen on the daemon socket
   - validate prerequisites
   - validate credentials, paths, permissions, and build GeoIP data

2. **Core startup**
   - construct runtime data
   - create the database when necessary
   - load the previous cloud from daemon state or the database
   - build and fully validate the new cloud from configuration

3. **Reconciliation**
   - reconcile old and new cloud state
   - manage Cloudflare state required by the cloud
   - update the persisted cloud state in DB

4. **Monitoring and operation**
   - load service history
   - construct application-panel API clients for Production servers
   - construct service workers
   - start monitoring
   - listen for daemon commands

## Direct integrations

xcplane directly manages Cloudflare resources whose resulting state is needed
by the control plane, including lists, rules, rulesets, and related zone/server
state.

xcplane also communicates directly with the configured application panel.

## Ansible

The daemon creates an Ansible workflow when system-level work is required:

1. maps xcplane actions to Ansible task files;
2. prepares the variables passed to Ansible;
3. executes the run;
4. returns the result to xcplane.

Ansible is an execution mechanism inside the control plane, not the
source of truth for the cloud.

## State

The declarative configuration builds the new cloud. The previous cloud is
loaded from persisted daemon state or the database, and becomes the old cloud
used for reconciliation.

The database stores the resulting operational cloud state.

> TODO: Add a detailed state/data-flow diagram.

## Read more
[Ansible](ansible.md)
[Cloudflare](cloudflare.md)
[Application Panels](panels/README.md)

