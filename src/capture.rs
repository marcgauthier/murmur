//! The session pointer stays on the database thread and is destroyed before its
//! connection. SQLite owns the database handle independently of Rust addresses.
use crate::{Error, Result};
use rusqlite::{Connection, ffi};
pub(crate) struct Capture {
    ptr: *mut ffi::sqlite3_session,
}
impl Capture {
    pub fn new(connection: &Connection, tables: impl Iterator<Item = String>) -> Result<Self> {
        let mut ptr = std::ptr::null_mut();
        // SAFETY: live connection on its owner thread; CString lasts through call.
        check(unsafe {
            ffi::sqlite3session_create(connection.handle(), c"main".as_ptr(), &mut ptr)
        })?;
        let capture = Self { ptr };
        let mut enabled = 1;
        check(unsafe {
            ffi::sqlite3session_object_config(
                ptr,
                ffi::SQLITE_SESSION_OBJCONFIG_SIZE,
                std::ptr::from_mut(&mut enabled).cast(),
            )
        })?;
        for table in tables {
            let name =
                std::ffi::CString::new(table).map_err(|_| Error::Invalid("NUL in table".into()))?;
            check(unsafe { ffi::sqlite3session_attach(ptr, name.as_ptr()) })?;
        }
        Ok(capture)
    }
    pub fn memory(&self) -> usize {
        (unsafe { ffi::sqlite3session_memory_used(self.ptr) }) as usize
    }
    pub fn size(&self) -> usize {
        (unsafe { ffi::sqlite3session_changeset_size(self.ptr) }) as usize
    }
    pub fn bytes(&mut self) -> Result<Vec<u8>> {
        let mut n = 0;
        let mut p = std::ptr::null_mut();
        check(unsafe { ffi::sqlite3session_changeset(self.ptr, &mut n, &mut p) })?;
        let out = if n == 0 {
            Vec::new()
        } else {
            unsafe { std::slice::from_raw_parts(p.cast::<u8>(), n as usize) }.to_vec()
        };
        unsafe { ffi::sqlite3_free(p) };
        Ok(out)
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        unsafe { ffi::sqlite3session_delete(self.ptr) }
    }
}
fn check(code: i32) -> Result<()> {
    if code == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(Error::Sql(rusqlite::Error::SqliteFailure(
            ffi::Error::new(code),
            None,
        )))
    }
}
