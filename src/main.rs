mod guarded_socket;
mod handshake;
mod protocol;
mod zellij;

use anyhow::{Context, Result};
use clap::Parser;
use iroh::{endpoint::presets, EndpointId};
use tokio::{
    select,
    signal::{self, unix::SignalKind},
};
use tokio_util::sync::CancellationToken;
use zellij::get_current_session;

use crate::{
    handshake::{generate_psk, init_endpoint, Host},
    zellij::get_base_path,
};

#[derive(Debug, Parser)]
pub struct JoinArgs {
    #[arg(help = "Peer to peer Endpoint ID of the host you want to join")]
    host: EndpointId,
    #[arg(help = "Pre Shared Secret, also provided by the host")]
    secret: String,
}

#[derive(Parser, Debug)]
pub enum Command {
    Host,
    Join(JoinArgs),
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Command::parse();
    let cancellation_token = CancellationToken::new();
    tokio::spawn(listen_for_shutdown(cancellation_token.clone()));

    let zellij_base_path = get_base_path()?;

    let endpoint = async {
        init_endpoint(presets::N0)
            .await
            .context("Failed to bind endpoint")
    };

    match args {
        Command::Host => {
            let session_info = get_current_session()?;
            let psk = generate_psk();
            let host = Host::accept(endpoint.await?, session_info, &psk).await?;
            host.serve(cancellation_token).await
        }

        Command::Join(args) => {
            let guest = handshake::Guest::connect(
                endpoint.await?,
                zellij_base_path,
                args.host,
                &args.secret,
            )
            .await?;

            guest.serve(cancellation_token).await
        }
    }
}

async fn listen_for_shutdown(cancellation_token: CancellationToken) -> Result<()> {
    let mut sigterm_listener = signal::unix::signal(SignalKind::terminate())
        .context("Couldn't set up SIGTERM listener")?;

    select! {
        _ = signal::ctrl_c() => {},
        _ = sigterm_listener.recv() => {},
    }

    println!("Performing a graceful shutdown...");
    cancellation_token.cancel();
    Ok(())
}
