# Application Panels

xcplane manages the infrastructure independently of the application panel. The
application panel is an integration used by xcplane for application-level
operations and state.

## 3XUI

3XUI is currently the supported application panel.

xcplane communicates with the 3XUI API directly for querying information about
the state of Production servers.

### Installation
When a server is being provisioned, `xui_install.yaml` task in Ansible installs
the panel non-interactively. xcplane-managed secrets which are passed to Ansible
are used during installation, and the following environment variables are set by
xcplane:

- XUI_NONINTERACTIVE
- XUI_SSL_MODE
- XUI_DB_TYPE
- XUI_PANEL_PORT
- XUI_USERNAME
- XUI_PASSWORD
- XUI_WEB_BASE_PATH

When the installation is successful `XUI_TOKEN` is read from the result file and
stored in the server secrets.

## Version
xcplane uses a controlled, tested version of the panel for installation. After
the successful deployment, the user may update it through the panel itself.

## Authentication
xcplane supports both cookie-based and header-based authentication methods. The
cookie-based method is currently used during installation, and header-based auth
is later utilized for subsequent calls to the panel API.

## Inbound kinds
xcplane supports five deployment profiles that have been selected for their
maturity, effective censorship resistance, and long-term
maintainability. Advanced Xray transport combinations are intentionally not
exposed because the Rust app wants to guarantee the correctness of the profiles
it manages.

- VlessXhttpReality,  // for direct connection, Cloudflare incompatible
- VlessTcpReality,    // for direct connection, Cloudflare incompatible
- VlessXhttpTls,      // for both direct and Cloudflare proxied connections
- VlessWsTls,         // for both direct and Cloudflare proxied connections
- VlessGrpcTls,       // for both direct and Cloudflare proxied connections in HTTP/2 environments

## Client management
While it is supported to create a specific number of clients during inbound creation,
xcplane does not manage the clients. The application panel is already very good
at this task, and, furthermore, client management is out of scope of xcplane as
it focuses on the infrastructure.

## Read more
[Cloudflare](../cloudflare.md)
