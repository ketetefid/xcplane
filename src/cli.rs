// SPDX-License-Identifier: GPL-3.0-or-later

use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::Path;
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc::Sender, oneshot};

use crate::cloud::parse_cloud_config;
use crate::daemon::{DaemonComm, DaemonReply, DaemonRequest};
use crate::types::{BoxError, SvcHealth, SvcKind, WorkSpace};

/// A struct for processing CLI arguments
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct CommArgs {
    #[command(subcommand)]
    pub mode: Option<CliComm>,
}

/// CLI communication commands
#[derive(Subcommand, Clone, Debug, Serialize, Deserialize)]
pub enum CliComm {
    /// Starts in daemon mode
    Daemon,

    /// Checks the cloud config file
    Check,

    /// Restores the backed up cloud config from DB
    Restore,

    /// Rebuilds the workspace, rechecks the prerequisites and does a 'reload'
    Restart,

    /// Shuts down the daemon gracefully
    Shutdown,

    /// Reads the cloud config file and adds or deletes servers from the
    /// monitoring group to match the newly defined cloud. For existing servers,
    /// only parameters of Offgrid servers are reconciled, and any attempt to
    /// change an existing Production server is ignored, with the exception
    /// of disabling or enabling them. This is the default reconciliation mode.
    Reload,

    /// Remap is an enhanced reload: it changes the existing cloud to match
    /// the new one, without performing any action on the remote servers. It is
    /// used when the remote servers are manually altered, and a reflection of
    /// those changes is needed in the monitoring system.
    Remap,

    /// Rebase mode reads the cloud config file, updates the current cloud and
    /// performs the necessary set of remote actions to reconcile the state of
    /// servers with the new declarative configuration. This mode is essentially
    /// a remap with full remote reconciliation, but performs it only if the
    /// server is already provisioned (Production state).
    Rebase(RebaseOpts),

    /// Inquires about the health status of cloud
    Status(StatusOpts),

    /// Resets the fix tries for all of the service monitoring tasks
    ResetFix,

    /// Expands the cloud by running full setup on an enabled Offgrid server
    Expand(ExpandOpts),

    /// Shows the credentials of production servers
    Creds(ShowSecrets),

    /// Shows the clients of production servers and their sublinks
    Clients,
}

/// The response that is given back to the socket. This response will be
/// serialized based on ReplyFormat.
#[derive(Serialize)]
#[serde(tag = "result", content = "data")]
pub enum SocketMessage {
    Ok(DaemonReply),
    Err(String),
}

/// The format of the reply that is sent back to the socket
#[derive(PartialEq)]
pub enum ReplyFormat {
    Toml,
    Json,
}

// Structs for constructing 'xcplane status' response

/// A summary of service retrieved from the service tasks
pub struct SvcSummary {
    pub service: SvcKind,
    pub server: String,
    pub health: SvcHealth,
}

#[derive(Serialize, Debug)]
pub struct SvcHealthSummary {
    pub service: String,
    pub health: SvcHealth,
}

#[derive(Serialize, Debug)]
pub struct ServerSummary {
    pub server: String,
    pub services: Vec<SvcHealthSummary>,
}

/// A struct holding information about a task and if it is running or not
#[derive(Serialize, Debug)]
pub struct TaskSummary {
    pub task: String,
    pub running: bool,
}

/// A summary of the cloud health which is given as the response of 'status'
/// CLI argument
#[derive(Serialize, Debug)]
pub struct CloudSummary {
    pub tasks: Vec<TaskSummary>,
    pub cloud: Vec<ServerSummary>,
}

/// This struct holds the links and secrets of servers when communicating with
/// the daemon, and is used when the users asks it to show the credentials.
#[derive(Serialize, Debug)]
pub struct ServerCreds {
    pub doh: String,
    pub ui: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xui_username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xui_password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xui_token: Option<String>,
}

/// Holds 'forced' flag when performing a Rebase command. A forced Rebase is
/// necessary if inbound deletion is intended.
#[derive(PartialEq, Args, Clone, Debug, Serialize, Deserialize)]
pub struct RebaseOpts {
    /// Applies the Rebase even if it is destructive
    #[arg(short, long)]
    pub forced: bool,
}

/// Whether usernames and passwords/tokens should be displayed alongside the
/// weblinks or not.
#[derive(Args, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct ShowSecrets {
    /// Shows all secrets including usernames and passwords
    #[arg(short, long)]
    pub show_all: bool,
}

#[derive(Args, Clone, Debug, Serialize, Deserialize)]
pub struct ExpandOpts {
    /// Expands the cloud by performing full setup on this server
    pub server: Option<String>,
}

#[derive(Args, Clone, Debug, Serialize, Deserialize)]
pub struct StatusOpts {
    /// Shows the full details of cloud summary
    #[arg(short, long)]
    pub full: bool,
}
///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

impl SocketMessage {
    /// A universal method to prettify the socket reply based on ReplyFormat
    fn serialize_pretty(&self) -> Result<String, BoxError> {
        match self {
            // Message variant is intended to be readable and we directly send
            // it
            SocketMessage::Ok(DaemonReply::Message(mes)) => Ok(mes.to_owned()),
            SocketMessage::Ok(reply) if reply.reply_format() == ReplyFormat::Toml => {
                Ok(toml::to_string_pretty(self)?)
            }

            _ => Ok(serde_json::to_string_pretty(self)?),
        }
    }
}

impl CliComm {
    /// Constructs the matching command to be sent to the Daemon
    fn daemon_command(self) -> DaemonComm {
        match self {
            CliComm::Restart => DaemonComm::Restart,
            CliComm::Shutdown => DaemonComm::Shutdown,
            CliComm::Status(status_opts) => DaemonComm::Status(status_opts),
            CliComm::Reload => DaemonComm::Reload,
            CliComm::Remap => DaemonComm::Remap,
            CliComm::Rebase(rebase_opts) => DaemonComm::Rebase(rebase_opts),
            CliComm::ResetFix => DaemonComm::ResetFix,
            CliComm::Expand(expand_opts) => DaemonComm::Expand(expand_opts),
            CliComm::Creds(show_secrets) => DaemonComm::Credentials(show_secrets),
            CliComm::Clients => DaemonComm::Clients,

            /*
            CliComm::Daemon variant directly starts the daemon itself and
            doesn't produce a DaemonComm. There is the same situation for the
            others where there is no interaction with a running daemon.
            Normally, through the app this arm becomes unreachable, however, we
            need to take care of direct socket interaction.
             */
            CliComm::Daemon | CliComm::Check | CliComm::Restore => DaemonComm::Unknown,
        }
    }
}
// =============================================================
/// Listens to the Unix socket for commands, sends them to the daemon channel
/// receiver, and gives a response back to the socket
pub async fn socket_listener(
    listener: UnixListener,
    tx: Sender<DaemonRequest>,
) -> Result<(), BoxError> {
    loop {
        let (unix_stream, _socket_addr) = listener.accept().await?;
        let txc = tx.clone();

        tokio::spawn(async move {
            let mut reader = BufReader::new(unix_stream);
            let mut buf = String::new();

            // Since the socket might be used by any process, we don't abort the
            // program just for an error here
            if reader.read_line(&mut buf).await.is_ok() {
                let cli_command_str = buf.trim();

                // Clap guards all the arguments, but if the socket is used
                // directly, we will accept only proper json commands that can
                // be deserialized to CliComm
                if let Some(cli_command) = serde_json::from_str::<CliComm>(&cli_command_str).ok() {
                    let (reply_tx, reply_rx) = oneshot::channel();

                    // Send the sender side of the oneshot channel to the
                    // daemon core
                    let request = DaemonRequest {
                        reply: reply_tx,
                        command: cli_command.daemon_command(),
                    };

                    txc.send(request).await?;

                    let response = reply_rx.await?;

                    // The payload data received from the daemon can remain typed
                    // while we convert it here to a serialized SocketMessage
                    let socket_message = match response {
                        Ok(reply) => SocketMessage::Ok(reply),
                        Err(e) => SocketMessage::Err(e.to_string()),
                    };

                    // We decide how to present information in serialize_pretty
                    let socket_reply = socket_message.serialize_pretty()?;

                    reader.get_mut().write_all(socket_reply.as_bytes()).await?;
                    reader.get_mut().shutdown().await?;
                }
            }
            Ok::<_, BoxError>(())
        });
    }
}
// =============================================================
/// Handles CLI interaction with the socket, and get the response
pub async fn send_socket_command(
    socket_path: &Path,
    clicomm: &CliComm,
) -> Result<String, BoxError> {
    let clicomm_str = serde_json::to_string(&clicomm)?;
    let mut unix_stream = UnixStream::connect(socket_path)
        .await
        .inspect_err(|_| eprintln!("The daemon is probably not running."))?;
    // Send all commands in "lines" so buffreader can read it easily
    unix_stream.write_all(clicomm_str.as_bytes()).await?;
    unix_stream.write_all(b"\n").await?;
    // Don't forget to flush data and close the stream
    unix_stream.shutdown().await?;

    let mut buf = String::new();
    println!("Sent {} to the socket.", clicomm_str);
    unix_stream.read_to_string(&mut buf).await?;

    Ok(buf.trim().to_string())
}
// =============================================================
/// Directly prints the socket reply, or shows it in a pager (less)
pub fn process_socket_reply(clicomm: &CliComm, reply: &str) -> Result<(), BoxError> {
    match clicomm {
        // For credentials, we use the pager as a more secure option
        CliComm::Creds(_) => show_with_less(reply)?,
        CliComm::Clients => show_with_less(reply)?,
        _ => println!("Received reply from the socket:\n\n{}", reply),
    }

    Ok(())
}
// =============================================================
/// Uses 'less' to show the credentials of servers
fn show_with_less(reply: &str) -> Result<(), BoxError> {
    let mut child = std::process::Command::new("less")
        .arg("-R") // preserve ANSI colors
        .stdin(Stdio::piped())
        .spawn()?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(reply.as_bytes())?;
        // flush the buffers explicitly for the sake of safety
        stdin.flush()?;
    }

    child.wait()?;

    Ok(())
}
// =============================================================
/// Checks the cloud config and reports back to the user
pub async fn check_config(workspace: &WorkSpace) -> () {
    println!("Checking cloud configuration ...");
    match parse_cloud_config(workspace).await {
        Ok(_) => println!("The cloud config is Ok."),
        Err(e) => eprintln!("Error in cloud config: {}", e),
    }
}
// =============================================================
