# Daemon

xcplane is a long-running daemon that owns the operational cloud state.

## Startup

At startup the daemon:

- determines and validates the workspace;
- acquires a unique daemon lock;
- creates the PID file and listens on its Unix socket at <runtime_dir>/xcplane/xcplane.{sock,pid};
- checks prerequisites;
- initializes runtime state;
- loads the previous cloud;
- validates the new cloud;
- reconciles state;
- loads the service status history and starts monitoring

## Commands

The daemon then listens for commands such as status, creds, clients, reload,
remap, and rebase.

Current list of supported xcplane commands are:

```text
  daemon     Starts in daemon mode
  
  check      Checks the cloud config file
  
  restore    Restores the backed up cloud config from DB
  
  restart    Rebuilds the workspace, rechecks the prerequisites and does a 'reload'
  
  shutdown   Shuts down the daemon gracefully
  
  reload     Reads the cloud config file and adds or deletes servers from the
             monitoring group to match the newly defined cloud. For existing servers, only
             parameters of Of fgrid servers are reconciled, and any attempt to change an
             existing Production server will be ignored, with the exception of disabling or
             enabling them. This is the default reconciliation mode
			 
  remap      Remap is an enhanced reload: it changes the existing cloud to match the
             new one, without performing any action on the remote servers. It is used when
             the remote servers are manually altered, and a reflection of those changes is
             needed in the monitoring system

  rebase     Rebase mode reads the cloud config file, updates the current cloud and
             performs the necessary set of remote actions to reconcile the state of servers
             with the new declarative configuration. This mode is essentially a remap with
             full remote reconciliation, but performs it only if the server is already
             provisioned (Production state)
  
  status     Inquires about the health status of cloud
  
  reset-fix  Resets the fix tries for all of the service monitoring tasks
  
  expand     Expands the cloud by running full setup on an enabled Offgrid server
  
  creds      Shows the credentials of production servers
  
  clients    Shows the clients of production servers and their sublinks
  
  help       Print this message or the help of the given subcommand(s)
```

## Monitoring

xcplane daemon constructs long-running workers for monitoring services in
servers, and for maintaining the cloud state.

The current categories include:

- Signal watcher;
- Socket listener;
- GeoIP updater;
- DB Aggregator;
- ACME checker/renewer for each Production server;
- XUI DB backup pruner for each Production server;
- SSH checker for each Production server;
- Nginx checker for each Production server;
- Inbound checker for every inbound of each Production server.

As well as the list above, the daemon monitors provisioning severs. As it is
clear, Global services such as GeoIP updating and signal handling can run even
when there are no Production servers.

During daemon startup, service history is read and then is periodically written
to DB by the aggregator in each monitoring interval. The application panel
database for each server is also backed up.

## Logs

xcplane uses structured tracing and separates operational logs from Ansible
execution output and errors.

By default, xcplane uses in-line log format for stdout and its log file which are stored in:
`$HOME/.local/state/xcplane/xcplane.<date>.log`

or 

`/var/log/xcplane/xcplane.<date>.log`

### Json format
If json format is desired, it can enabled for the log file (stdout retains the
default format) by setting `XCPLANE_JSON_LOG` environment variable (to any
value).

### Performance metrics in logs
Detailed performance measurements for daemon operations can be enabled via
setting `XCPLANE_TIMINGS` environment variable.

### Rotation of logs
Log file is rotated daily by default.

### Ansible logs
The raw Ansible log of stdout and stderr is saved separately whenever AnsibleRun
is performed for a group of AnsibleActions. The daemon logs the path to the
Ansible log files (which reside in the same directory).

### Log level

The default log level is `info` which can be changed using `RUST_LOG`
environment variable. Possible standard values are `error`,`warn`,`info`,`debug`
and `trace`. Currently xcplane itself doesn't have any log deeper than `debug`.
