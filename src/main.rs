// SPDX-License-Identifier: GPL-3.0-or-later

use clap::Parser;
use xcplane::cli::{CliComm, CommArgs, check_config, process_socket_reply, send_socket_command};
use xcplane::daemon::{core::daemonize, startup::prepare_workspace};
use xcplane::db::restore_config;
use xcplane::types::BoxError;

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let args = CommArgs::parse();

    // Sets up the working directories and paths
    let workspace = prepare_workspace()?;

    // Processes commands
    match args.mode.unwrap_or(CliComm::Daemon) {
        CliComm::Daemon => daemonize(workspace).await?,
        CliComm::Check => check_config(&workspace).await,
        CliComm::Restore => restore_config(&workspace).await?,
        clicomm => {
            let reply = send_socket_command(&workspace.daemon.sock_file, &clicomm).await?;
            process_socket_reply(&clicomm, &reply)?;
        }
    };

    Ok(())
}
