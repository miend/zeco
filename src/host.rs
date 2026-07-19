use anyhow::{bail, Result};
use iroh::{
    endpoint::{Connection, Incoming},
    Endpoint,
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::spawn;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::{
    protocol::EasyCodeWrite,
    zellij::{self, ZellijSessionInfo},
};

pub fn generate_psk() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(32)
        .map(char::from)
        .collect()
}

#[derive(Debug)]
pub struct Host {
    // We store the endpoint in the struct to keep it alive. When it goes out of scope, iroh
    // machinery is shut down
    _endpoint: Endpoint,
    connection: Connection,
    session_info: ZellijSessionInfo,
}

impl Host {
    pub async fn accept(
        endpoint: Endpoint,
        session_info: ZellijSessionInfo,
        psk: &str,
    ) -> Result<Host> {
        // TODO: Move much of the message printing up to the CLI layer
        info!(
            "Sharing Zellij session '{}' (version {})",
            session_info.name, session_info.version
        );
        info!("The guest can join with:");
        info!("\tzeco join {} {}", endpoint.id(), psk);
        info!(
            "WARNING! Everyone with these credentials can execute arbitrary commands in your shell. \
            Only hand over to people you fully trust."
        );
        info!("Waiting for guest to join. Press Ctrl-C to quit.");

        let incoming: Incoming = endpoint.accept().await.unwrap();
        let connection = incoming.accept()?.await?;
        info!("Connection established.");

        let (mut send, mut recv) = connection.accept_bi().await?;
        assert_eq!(psk.len(), 32); // String::length is in bytes
        let mut buf = [0; 32];
        recv.read_exact(&mut buf).await?;
        if buf != psk.as_bytes() {
            send.write_all(&[0]).await?;
            bail!("Guest provided wrong secret. Quit.");
        }
        send.write_all(&[1]).await?;
        send.finish()?;
        info!("Guest authenticated successfully!");
        drop(send);
        drop(recv);

        let mut s = connection.open_uni().await?;
        s.struct_write(&session_info.version).await?;
        s.struct_write(&session_info.name).await?;
        s.finish()?;

        Ok(Host {
            _endpoint: endpoint,
            session_info,
            connection,
        })
    }

    pub async fn serve(self, cancellation_token: CancellationToken) -> Result<()> {
        loop {
            let z = self.session_info.clone();
            tokio::select! {
                x = self.connection.accept_bi() => {
                    match x {
                        Ok((send, recv)) => {
                            spawn(zellij::handle_zellij_session(send, recv, z));
                        }
                        Err(e) => bail!("Failed to accept channel from guest: {:?}", e),
                    }
                }
                _ = cancellation_token.cancelled() => {
                    return Ok(())
                }
            }
        }
    }
}
