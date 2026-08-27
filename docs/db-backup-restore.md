# Database

The database stores the persisted cloud and operational state. The cloud state
is updated in it whenever reconciliation has fully been completed.

## Aggregator
The daemon writes the service stats of the running cloud to DB in every
monitoring interval.

## Source of truth
In xcplane design, the DB remains the source of truth; the config file builds
the desired-state.

## Backup 
Whenever reconciliation is performed successfully, and before the DB is updated,
the current database is backed up and saved in a snapshot. The current running
cloud is also backed up and written to a toml file.

The backed up data reside in `<data directory>/Cloud-Backups/`

As well as the cloud data, the application panel database is also backed up
periodically if all Xray inbounds are healthy for each server in `<data directory>/Xui-Backups/`

## Restore
Whenever a form of reconciliation succeeds, the DB updates the saved cloud. An
exact copy of cloud configuration file i.e. `cloud.toml` is also saved in the
DB. This allows the cloud state to be fully reconstructed in a new controller
just with the DB file.

To move the cloud to another controller, the DB must be first placed in its proper
location, and then the restore command is issued: `xcplane restore`

The exact copy of cloud config file which was saved post-reconciliation will be
restored in the config directory.

## Location
The database resides in `data directory`:
`$HOME/.local/share/xcplane/cloud.db`

or 

`/var/lib/xcplane/cloud.db`

## Restoration to a previous state

xcplane backs up the DB and the cloud config before attempting a
reconciliation. Therefore, the previous states can still be recovered.

To recover a previous state of the cloud, first the daemon must be shut down,
and then the desired version of DB from `Cloud-Backups` directory must replace
the cloud DB file.

Note that manual restoration of DB is an emergency action and should not be
practiced normally, as the remote state of the fleet might have diverged
significantly from the chosen version.
