// SPDX-License-Identifier: GPL-3.0-or-later

use std::{path::PathBuf, time::SystemTime};
use tokio::task::JoinHandle;

use crate::{
    constants::{RETAINED_XUI_DBNUM, XUI_BACKUP_DIR},
    types::{BoxError, KetServer, WorkSpace},
};

///////////////////////////////////////////////////////////////////
// ============================================================= //
///////////////////////////////////////////////////////////////////

/// Returns the path to the latest backed up x-ui DB
pub fn last_xui_db(
    workspace: &WorkSpace,
    server: &KetServer,
) -> JoinHandle<Result<PathBuf, BoxError>> {
    let backup_path = workspace.dirs.data_dir.join(XUI_BACKUP_DIR);
    let server_str = format!("{}-", server.name);
    // This is a blocking call because of the possible large number of IO
    // operations.
    tokio::task::spawn_blocking(move || {
        let latest_db_path = std::fs::read_dir(&backup_path)?
            // The backups were created by the program. If something isn't
            // accessible, we intentionally leave them out.
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| {
                let name_ostr = entry.file_name();
                let name = name_ostr.to_str()?;

                if name.starts_with(&server_str) && name.ends_with("backup.db") {
                    Some(entry.path())
                } else {
                    None
                }
            })
            .max()
            .ok_or("Couldn't find any DB to choose from.")?;

        Ok(latest_db_path)
    })
}
// =============================================================
/// Prunes the x-ui DBs for the server
pub fn prune_xui_backups(
    workspace: &WorkSpace,
    server: &KetServer,
) -> JoinHandle<Result<(), BoxError>> {
    // A structure to hold the DB path and its creation time for sorting
    struct FileEntry {
        path: PathBuf,
        created_time: SystemTime,
    }

    let backup_path = workspace.dirs.data_dir.join(XUI_BACKUP_DIR);
    let server_str = format!("{}-", server.name);

    tokio::task::spawn_blocking(move || {
        if backup_path.exists() {
            let mut all_dbs: Vec<FileEntry> = std::fs::read_dir(&backup_path)?
                // The backups were created by the program. If something isn't
                // accessible, we intentionally leave them out.
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| {
                    let name_ostr = entry.file_name();
                    let name = name_ostr.to_str()?;

                    if name.starts_with(&server_str) && name.ends_with("backup.db") {
                        // If we can't read the metadata, we ignore the DB file
                        let created_time = entry.metadata().ok()?.created().ok()?;
                        let file_entry = FileEntry {
                            path: entry.path(),
                            created_time,
                        };

                        Some(file_entry)
                    } else {
                        None
                    }
                })
                .collect();

            all_dbs.sort_by(|a, b| b.created_time.cmp(&a.created_time));

            let dbs_num = all_dbs.len();
            let count = RETAINED_XUI_DBNUM.min(dbs_num);

            if dbs_num > count {
                all_dbs
                    .drain(count..)
                    .try_for_each(|db_entry| std::fs::remove_file(&db_entry.path))?;
            }
        }

        Ok(())
    })
}
// =============================================================
/// Deletes backed up x-ui DBs of the server
pub fn delete_xui_backups(
    workspace: &WorkSpace,
    server: &KetServer,
) -> JoinHandle<Result<(), BoxError>> {
    let backup_path = workspace.dirs.data_dir.join(XUI_BACKUP_DIR);
    let server_str = format!("{}-", server.name);

    tokio::task::spawn_blocking(move || {
        if backup_path.exists() {
            std::fs::read_dir(&backup_path)?
                .filter_map(|entry| entry.ok())
                .filter_map(|entry| {
                    let name_ostr = entry.file_name();
                    let name = name_ostr.to_str()?;

                    if name.starts_with(&server_str) && name.ends_with("backup.db") {
                        Some(entry.path())
                    } else {
                        None
                    }
                })
                .try_for_each(|p| std::fs::remove_file(p))?;
        }

        Ok(())
    })
}
// =============================================================
