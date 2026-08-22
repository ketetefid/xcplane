# Rebase

Rebase is xcplane's forceful remote reconciliation operation.

It allows an already provisioned Production server to be reconfigured to match
a new desired cloud specification without rebuilding the server.

## Supported changes

The reconciliation model supports changing all primary parameters:

- changing the server IP;
- changing the domain;
- changing the SSH port;
- changing subdomains;
- changing Cloudflare proxy state;
- changing countries used for direct outbound blocking;
- adding new inbounds;
- removing inbounds with the forced flag.

In xcplane, inbounds are considered immutable. Therefore, modifying an existing
inbound is not supported and returns an error.

There are non-primary parameters such as quota and region for each server
which are changed according to the new model, but they do not trigger
remote operations.

## Rules

- health_path is always set from the newly defined data;
- The other secrets/paths remain stable;
- If necessary, the table of mother sub-IDs is updated to accommodate new inbounds;
- If server is disabled, its parameters will not change and Rebase is skipped;
- Changing non-primary parameters do not trigger remote actions;
- Rebase from Offgrid to Production is invalid and returns an error;
- Rebase from Production to Offgrid is treated as a Remap.

## Stable Production identity

Rebase does not replace Production secrets such as:

- usernames;
- passwords;
- DoH endpoints;
- subscription paths.

The Nginx health path is an exception to the general "stable value" rule because
it is a deterministic derivative of the server domain and subdomains.

## Forced rebase

Deleting inbounds is considered a destructive action that needs "--forced"
flag. Therefore, the user must invoke the flag and issue `xcplane rebase
--forced`.
