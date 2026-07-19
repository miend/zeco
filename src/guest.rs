use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use iroh::{endpoint::Connection, Endpoint, EndpointAddr};
use tokio::{fs::create_dir_all, spawn};
use tokio_util::sync::CancellationToken;

use crate::{
    guarded_socket::GuardedSocket,
    protocol::{EasyCodeRead, ALPN},
    zellij::{self},
};

pub struct Guest {
    connection: Connection,
    // We store the endpoint in the struct to keep it alive. When it goes out of scope, iroh
    // machinery is shut down
    _endpoint: Endpoint,
    socket: GuardedSocket,
}

impl Guest {
    pub async fn connect(
        endpoint: Endpoint,
        zellij_base_path: PathBuf,
        host_endpoint_addr: impl Into<EndpointAddr>,
        psk: &str,
    ) -> Result<Guest> {
        let connection = endpoint.connect(host_endpoint_addr, ALPN).await?;
        let (mut send, mut recv) = connection.open_bi().await?;
        send.write_all(psk.as_bytes()).await?;
        send.finish()?;
        let mut success = [0];
        recv.read_exact(&mut success).await?;
        if success != [1] {
            bail!("Host declined provided secret.");
        }
        println!("Host let you in.");
        drop(send);
        drop(recv);

        let mut stream = connection.accept_uni().await?;
        let version: String = stream.struct_read().await?;
        let name: String = stream.struct_read().await?;
        println!("Remote Session is {name}. You too are expected to use version {version}.");

        let dir = zellij_base_path.join(version);
        create_dir_all(&dir)
            .await
            .context("Failed to create zellij directory")?;
        let remote_session_name = format!("{name}-remote");
        let local_socket_path = dir.join(&remote_session_name);
        let socket = GuardedSocket::bind(local_socket_path).await?;

        Ok(Guest {
            connection,
            _endpoint: endpoint,
            socket,
        })
    }

    pub async fn serve(&self, cancellation_token: CancellationToken) -> Result<()> {
        // TODO: Move this to main and re-enable
        // let _t = spawn_blocking(|| attach_zellij(remote_session_name));
        loop {
            tokio::select! {
                result = self.socket.accept() => {
                    match result {
                        Ok((stream, _)) => {
                            let c = self.connection.clone();
                            spawn(zellij::handle_zellij_socket(stream, c));
                        }
                        Err(_) => println!("Failed to accept connection on socket."),
                    }
                }
                _ = cancellation_token.cancelled() => {
                        return Ok(())
                }
            }
        }
    }

    #[cfg_attr(not(test), expect(dead_code))]
    pub fn socket_path(&self) -> PathBuf {
        self.socket.path()
    }
}
