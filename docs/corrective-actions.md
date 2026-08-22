# Corrective actions
xcplane has this capability to apply fixing actions on Production servers
whenever a major service deviates from its intended state. By default, this is
enabled.

Corrective actions are applied in steps. Each step is tried and if it failed,
the next step would be attempted. Currently there are two fixing steps for
each service.

## Supported services
Corrective actions are available only for Nginx and Xray. 

## Failure threshold
When the number of consequent failed monitoring intervals reaches a threshold,
fixing actions are triggered.

## Nginx
If Nginx isn't working, provided that SSH connectivity is present, as the first
step, xcplane will try to restart it via Ansible. If restarting Nginx didn't
help, the second step would be re-uploading its config file.

## Xray
If the supervisor Xray service is unhealthy (i.e., at least one inbound is
unhealthy), provided that SSH & Nginx services are working, xcplane will try to,
first, restart the x-ui service, and if it didn't work, it would try to restore
the last known good xui DB into the server.

## Read more
[Configuration](configuration.md)
