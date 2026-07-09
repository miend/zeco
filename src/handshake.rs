// Our goal is to establish two iroh::Connections
// for the host and the guest.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use iroh::{
    endpoint::{presets::Preset, Connection, Incoming},
    Endpoint, EndpointAddr, SecretKey,
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::{fs::create_dir_all, spawn};
use tokio_util::sync::CancellationToken;

use crate::{
    guarded_socket::GuardedSocket,
    protocol::{EasyCodeRead, EasyCodeWrite},
    zellij::{self, ZellijSessionInfo},
};

const ALPN: &[u8] = &[3, 1, 4, 1, 5, 9, 2, 6];

pub async fn init_endpoint(preset: impl Preset) -> Result<Endpoint> {
    let secret_key = SecretKey::generate();
    Endpoint::builder(preset)
        .secret_key(secret_key)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await
        .map_err(|e| anyhow::anyhow!("failed to bind iroh endpoint: {e}"))
}

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
    //TODO: I think only host needs this struct, include this in host's side of reorg
    session_info: ZellijSessionInfo,
}

impl Host {
    pub async fn accept(
        endpoint: Endpoint,
        session_info: ZellijSessionInfo,
        psk: &str,
    ) -> Result<Host> {
        // TODO: Move much of the message printing up to the CLI layer
        println!(
            "Sharing Zellij session {} (version {})",
            session_info.name, session_info.version
        );
        println!("The guest now can join with the following command:");
        println!("\tzeco join {} {}", endpoint.id(), psk);
        println!(
            "WARNING! Everyone with these credentials can execute arbitrary commands in your shell. \
            Only hand over to people you fully trust."
        );
        println!("Waiting for guest to join. Press Ctrl-C to quit.");

        let incoming: Incoming = endpoint.accept().await.unwrap();
        let connection = incoming.accept()?.await?;
        println!("Connection established.");

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
        println!("Guest authenticated successfully!");
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

    pub fn socket_path(&self) -> PathBuf {
        self.socket.path()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use anyhow::Context;
    use iroh::endpoint::presets;
    use tempfile::tempdir;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{UnixListener, UnixStream},
        time::timeout,
        try_join,
    };

    use super::*;

    // This performs host and guest side setups and sends bytes across the wire, confirming that
    // traffic makes it across guest_socket -> guest_iroh_node -> host_iroh_node -> host_socket and
    // can be read back out.
    #[tokio::test]
    async fn host_and_guest_complete_roundtrip() -> Result<()> {
        let cancellation_token = CancellationToken::new();
        let session_name = "test-session";

        // Zellij normally creates and owns the host-side socket, but we'll make a temp placeholder for testing
        let host_socket_dir = tempdir()?;
        let host_socket_path = host_socket_dir.path().join(session_name);

        let guest_socket_dir = tempdir()?;

        let session_info = ZellijSessionInfo {
            path: host_socket_path.clone(),
            version: String::from("contract_version_1"),
            name: String::from(session_name),
        };

        let host_endpoint = init_endpoint(presets::Minimal)
            .await
            .context("Failed to bind host endpoint")?;
        let host_addr = host_endpoint.addr();

        let guest_endpoint = init_endpoint(presets::Minimal)
            .await
            .context("Failed to bind guest endpoint")?;
        let psk = generate_psk();

        let (host, guest) = try_join!(
            Host::accept(host_endpoint, session_info, &psk),
            Guest::connect(
                guest_endpoint,
                guest_socket_dir.path().to_path_buf(),
                host_addr,
                &psk,
            )
        )
        .context("Failed to prepare either the host or guest")?;

        // tokio::select! on the host task, the guest task, and a validation task
        // host and guest tasks both block forever while they proxy zellij session data, only returning
        // when the cancellation token gets cancelled. Our third task can actually send data across the
        // wire and validate it, and return a success/cancel the other two
        tokio::select! {
            res = host.serve(cancellation_token.clone()) => bail!("host task ended unexpectedly: {res:?}"),
            res = guest.serve(cancellation_token.clone()) => bail!("guest task ended unexpectedly: {res:?}"),
            res = simulate_zellij_traffic(host_socket_path, guest.socket_path()) => {
                if let Err(e) = res {
                    bail!("zellij traffic simulation ended unexpectedly: {e:?}")
                };
                Ok(())
            }
        }
    }

    async fn simulate_zellij_traffic(
        host_socket_path: PathBuf,
        guest_socket_path: PathBuf,
    ) -> Result<()> {
        let timeout_limit = Duration::from_secs(5);
        let guest_input = b"6.28318530";

        // zellij normally owns the host-side socket, but in testing we need to simulate the host
        // socket existing and listening
        let host_listener = UnixListener::bind(&host_socket_path)?;

        // UnixListener::accept() will only resolve when something connects to it. This won't happen
        // until we forward traffic from the host-side iroh node, as the QUIC stream is only
        // visible to the host peer once bytes are written to it. So we simultaneously listen on
        // the host socket while connecting the guest stream and writing bytes.
        let (host_result, guest_result) = tokio::try_join!(
            async {
                host_listener
                    .accept()
                    .await
                    .context("Fake zellij server never received a connection")
            },
            async {
                let mut guest_stream =
                    timeout(timeout_limit, UnixStream::connect(guest_socket_path))
                        .await
                        .context("Timed out connecting to guest's test socket")?
                        .context("Failed to connect to guest's test socket")?;

                guest_stream
                    .write_all(guest_input)
                    .await
                    .context("Failed to write bytes to guest test socket")?;

                Ok::<_, anyhow::Error>(guest_stream)
            },
        )?;

        let (mut host_stream, _) = host_result;

        let mut host_output_buffer = vec![0; guest_input.len()];
        host_stream
            .read_exact(&mut host_output_buffer)
            .await
            .context("Failed to read out bytes from host test socket")?;

        assert_eq!(guest_input, &host_output_buffer.as_slice());

        // Write some bytes back the other way (host -> guest)
        let host_input = b"2.71828182";
        let mut guest_stream = guest_result;
        host_stream
            .write_all(host_input)
            .await
            .context("Failed to write bytes to host test socket")?;

        let mut guest_output_buffer = vec![0; host_input.len()];
        guest_stream
            .read_exact(&mut guest_output_buffer)
            .await
            .context("Failed to read out bytes from guest socket")?;

        assert_eq!(host_input, &guest_output_buffer.as_slice());

        Ok(())
    }
}
