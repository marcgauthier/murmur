use crate::{Error, Result, model::*, storage};
use spool::{Durability, Mutation};
use std::{
    fs,
    path::{Path, PathBuf},
};

/// Copy one file and fsync the destination. Best-effort 0600 on unix.
fn copy_file_sync(src: &Path, dst: &Path) -> Result<()> {
    fs::copy(src, dst)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(dst, fs::Permissions::from_mode(0o600));
    }
    let f = fs::File::open(dst)?;
    f.sync_all()?;
    Ok(())
}

fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(dir, fs::Permissions::from_mode(0o700));
    }
    // Match Spool: Windows does not support opening a directory for fsync.
    #[cfg(not(windows))]
    fs::File::open(dir)?.sync_all()?;
    Ok(())
}

/// Copy a spool store directory (manifest, keys.enc, segments) to a new
/// directory. Ignores checkpoint markers (COMPLETE) and live locks (LOCK).
/// `dst` must not exist.
pub(crate) fn copy_store_dir(src: &Path, dst: &Path) -> Result<()> {
    if !src.join("manifest").is_file() {
        return Err(Error::Invalid("backup storage does not exist".into()));
    }
    fs::create_dir(dst)
        .map_err(|_| Error::Invalid("restore destination must be a new directory".into()))?;
    let cleanup = |_: &Error| {
        let _ = fs::remove_dir_all(dst);
    };
    let step = (|| -> Result<()> {
        copy_file_sync(&src.join("manifest"), &dst.join("manifest"))?;
        let keys = src.join("keys.enc");
        if keys.is_file() {
            copy_file_sync(&keys, &dst.join("keys.enc"))?;
        }
        let src_seg = src.join("segments");
        let dst_seg = dst.join("segments");
        fs::create_dir(&dst_seg)?;
        if src_seg.is_dir() {
            let mut names: Vec<_> = fs::read_dir(&src_seg)?
                .collect::<std::io::Result<Vec<_>>>()?
                .into_iter()
                .map(|e| e.file_name())
                .collect();
            names.sort();
            for name in names {
                let s = src_seg.join(&name);
                if !s.is_file() {
                    continue;
                }
                copy_file_sync(&s, &dst_seg.join(&name))?;
            }
        }
        sync_dir(&dst_seg)?;
        sync_dir(dst)?;
        if let Some(parent) = dst.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = fs::File::open(parent).and_then(|f| f.sync_all());
            }
        }
        Ok(())
    })();
    if let Err(e) = step {
        cleanup(&e);
        return Err(e);
    }
    Ok(())
}

/// Copy a spool checkpoint's stream set (manifest, keyring, segments) from a
/// temporary checkpoint directory to a final backup directory on a possibly
/// different filesystem. `files` are absolute paths under `tmp_dir`.
pub(crate) fn copy_checkpoint_files(tmp_dir: &Path, files: &[PathBuf], dst: &Path) -> Result<()> {
    fs::create_dir(dst)
        .map_err(|_| Error::Invalid("backup destination must be a new directory".into()))?;
    let cleanup = || {
        let _ = fs::remove_dir_all(dst);
    };
    let step = (|| -> Result<()> {
        for f in files {
            let rel = f
                .strip_prefix(tmp_dir)
                .map_err(|_| Error::Corrupt("checkpoint file escapes destination".into()))?;
            if rel.to_str().is_some_and(|s| s.starts_with("..")) {
                return Err(Error::Corrupt("checkpoint file escapes destination".into()));
            }
            let out = dst.join(rel);
            if let Some(parent) = out.parent() {
                fs::create_dir_all(parent)?;
            }
            copy_file_sync(f, &out)?;
        }
        let complete = tmp_dir.join("COMPLETE");
        if complete.is_file() {
            copy_file_sync(&complete, &dst.join("COMPLETE"))?;
        }
        let seg = dst.join("segments");
        if seg.is_dir() {
            sync_dir(&seg)?;
        }
        sync_dir(dst)?;
        if let Some(parent) = dst.parent() {
            if !parent.as_os_str().is_empty() {
                let _ = fs::File::open(parent).and_then(|f| f.sync_all());
            }
        }
        Ok(())
    })();
    if let Err(e) = step {
        cleanup();
        return Err(e);
    }
    Ok(())
}

fn restore_options() -> Options {
    Options {
        mode: OpenMode::Existing,
        ..Options::default()
    }
}

fn key_error(e: spool::Error) -> Error {
    match e.kind() {
        spool::ErrorKind::WrongKey => Error::WrongKey,
        spool::ErrorKind::AuthFailed | spool::ErrorKind::Corrupt => Error::Corrupt(e.to_string()),
        _ => Error::Storage(e.to_string()),
    }
}

/// Verify a restored/live store directory opens as a database and its
/// winning rows rebuild into an integrity-checked SQLite database.
/// Mutates `path` (spool open bumps its manifest); callers must pass a
/// copy when the source must stay immutable.
fn verify_store_dir(path: &Path, key: &[u8]) -> Result<Identity> {
    let opts = restore_options();
    let progress = crate::progress::OpenProgressReporter::new(None);
    let opened = storage::open(path, key, &opts, &progress)?;
    let identity = opened.identity.clone();
    let connection = storage::recover_current(&opened, &progress)?;
    drop(connection);
    let _ = opened.store.close();
    Ok(identity)
}

/// Restore an online backup directory to a new live directory, preserving the
/// backup's encryption key. Verifies the restored copy opens and rebuilds.
/// `dst` must not exist; on verification failure it is removed.
pub(crate) fn restore_backup(
    backup: &Path,
    dst: &Path,
    key: &[u8],
    options: &RestoreOptions,
) -> Result<()> {
    if key.len() != 32 {
        return Err(Error::Invalid("application key must be 32 bytes".into()));
    }
    if !backup.join("manifest").is_file() {
        return Err(Error::Invalid("backup storage does not exist".into()));
    }
    spool::validate_wrapping_key(backup, key, &[], "").map_err(key_error)?;
    copy_store_dir(backup, dst)?;
    let step = (|| -> Result<()> {
        if options.new_database_id.is_some() || options.new_node_id.is_some() {
            let opts = restore_options();
            let opened = storage::open(dst, key, &opts, &crate::progress::OpenProgressReporter::new(None))?;
            let mut identity = opened.identity.clone();
            if let Some(id) = options.new_database_id {
                identity.database = id;
            }
            if let Some(id) = options.new_node_id {
                identity.node = id;
            }
            opened.store.commit(
                &[Mutation::put(b"00/identity".to_vec(), encode(&identity)?)],
                Durability::Sync,
            )?;
            let _ = opened.store.close();
        }
        verify_store_dir(dst, key)?;
        Ok(())
    })();
    if let Err(e) = step {
        let _ = fs::remove_dir_all(dst);
        return Err(e);
    }
    Ok(())
}

/// Verify a backup directory without mutating it, by copying it to a
/// temporary sibling and opening the copy.
pub(crate) fn verify_backup_dir(backup: &Path, key: &[u8]) -> Result<()> {
    if key.len() != 32 {
        return Err(Error::Invalid("application key must be 32 bytes".into()));
    }
    if !backup.join("manifest").is_file() {
        return Err(Error::Invalid("backup storage does not exist".into()));
    }
    spool::validate_wrapping_key(backup, key, &[], "").map_err(key_error)?;
    let parent = backup
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    for _ in 0..8 {
        let tmp = parent.join(format!(
            ".verify-tmp-{}-{:016x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            rand::random::<u64>()
        ));
        if tmp.exists() {
            continue;
        }
        copy_store_dir(backup, &tmp)?;
        let result = verify_store_dir(&tmp, key);
        let _ = fs::remove_dir_all(&tmp);
        return result.map(|_| ());
    }
    Err(Error::Invalid(
        "could not allocate verification directory".into(),
    ))
}
