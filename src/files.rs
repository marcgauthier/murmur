//! Direct database file API. File operations use Spool and the object store;
//! they do not execute SQL or occupy the SQLite writer.
use crate::{Database, Error, FileMetadata, Result};
use std::{
    io::{Read, Write},
    sync::Arc,
};

fn file_error(error: murmur_replication::Error) -> Error {
    Error::Storage(error.to_string())
}

impl Database {
    fn file_store(&self) -> Result<Arc<murmur_replication::files::Objects>> {
        if self.status().closed {
            return Err(Error::Closed);
        }
        self.replication()
            .and_then(|handle| handle.files())
            .ok_or_else(|| Error::Invalid("file storage is not configured at open".into()))
    }

    /// Stream a file into encrypted storage and publish its replicated metadata.
    pub fn upload_file(&self, name: &str, input: impl Read) -> Result<FileMetadata> {
        self.file_store()?.upload(name, input).map_err(file_error)
    }

    /// List nondeleted file metadata, sorted by name, including remote files
    /// whose contents have not yet been fetched.
    pub fn list_files(&self) -> Result<Vec<FileMetadata>> {
        self.file_store()?.list().map_err(file_error)
    }

    /// Find files whose names contain the case-sensitive query. An empty query
    /// returns all files. This searches metadata names, not file contents.
    pub fn search_files(&self, query: &str) -> Result<Vec<FileMetadata>> {
        Ok(self
            .list_files()?
            .into_iter()
            .filter(|file| file.name.contains(query))
            .collect())
    }

    /// Look up a file by exact name. Deleted files are returned as None.
    pub fn file_metadata(&self, name: &str) -> Result<Option<FileMetadata>> {
        Ok(self
            .file_store()?
            .metadata(name)
            .map_err(file_error)?
            .filter(|file| !file.deleted))
    }

    /// Stream locally available file contents into the supplied writer.
    /// Returns an error if the contents have not yet been fetched from a peer.
    pub fn get_file(&self, name: &str, output: impl Write) -> Result<FileMetadata> {
        self.file_store()?.read(name, output).map_err(file_error)
    }

    /// Check whether nondeleted file contents are present locally.
    /// Report whether the file is local, known at recently observed peers, or unavailable.
    pub fn file_availability(&self, name: &str) -> Result<murmur_replication::files::Availability> {
        self.file_store()?.availability(name).map_err(file_error)
    }

    /// Check whether this node still has old-generation objects or transfers
    /// that require its previous file key.
    pub fn file_key_rotation_status(&self) -> Result<murmur_replication::files::KeyRotationStatus> {
        self.file_store()?.key_rotation_status().map_err(file_error)
    }

    pub fn file_available(&self, name: &str) -> Result<bool> {
        self.file_store()?.available(name).map_err(file_error)
    }

    /// Publish a deletion tombstone for the named file.
    pub fn delete_file(&self, name: &str) -> Result<()> {
        self.file_store()?.delete(name).map_err(file_error)
    }
}
