use std::path::PathBuf;

use anyhow::{Context, Result};
use tokio::net::{unix::SocketAddr, UnixListener, UnixStream};

pub struct GuardedSocket {
    listener: Option<UnixListener>,
    path: PathBuf,
}

impl GuardedSocket {
    pub async fn accept(&self) -> Result<(UnixStream, SocketAddr)> {
        Ok(self.listener.as_ref().unwrap().accept().await?)
    }

    pub fn bind(path: PathBuf) -> Result<GuardedSocket> {
        let listener = UnixListener::bind(&path).context(format!(
            "Failed to create socket file at {}.",
            &path.display()
        ))?;
        Ok(GuardedSocket {
            listener: Some(listener),
            path,
        })
    }
}

impl Drop for GuardedSocket {
    fn drop(&mut self) {
        // Ensure we close file descriptor by dropping listener before unlinking
        drop(self.listener.take());
        let result = std::fs::remove_file(&self.path);
        if let Err(err) = result {
            println!(
                "Warning: Failed to remove socket file during cleanup: {}",
                err
            )
        }
    }
}
