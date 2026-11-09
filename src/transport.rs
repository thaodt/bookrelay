//! TCP setup is explicit so source-port experiments preserve TLS verification.

use anyhow::{Context, ensure};
use futures_util::{SinkExt, StreamExt};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::Duration,
};
use tokio::net::{TcpSocket, TcpStream, lookup_host};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, client_async_tls_with_config,
    tungstenite::{
        Error as WsError, Message, client::IntoClientRequest, error::ProtocolError, http::Request,
        protocol::WebSocketConfig,
    },
};

use crate::auth::Credentials;

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Sign the actual upgrade request after TCP setup, before the WS handshake.
/// Transport and source tasks need no knowledge of an exchange's credential type.
pub trait RequestAuthenticator: Send + Sync {
    fn authenticate(&self, request: &mut Request<()>) -> anyhow::Result<()>;
}

#[derive(Clone, Debug, Default)]
pub struct ConnectOptions {
    pub source_port: Option<u16>,
    pub remote_ip: Option<IpAddr>,
}

pub struct Connection {
    pub socket: Socket,
    pub local: SocketAddr,
    pub peer: SocketAddr,
}

pub fn validate_ws_url(url: &str) -> anyhow::Result<()> {
    let request = url.into_client_request()?;
    ensure!(
        matches!(request.uri().scheme_str(), Some("ws" | "wss")) && request.uri().host().is_some(),
        "expected websocket URL with hostname"
    );
    Ok(())
}

pub async fn connect(
    url: &str,
    auth: &Credentials,
    options: &ConnectOptions,
) -> anyhow::Result<Connection> {
    connect_with_authenticator(url, Some(auth), options).await
}

/// Public market feeds receive no account credentials or authentication headers.
pub async fn connect_public(url: &str, options: &ConnectOptions) -> anyhow::Result<Connection> {
    connect_with_authenticator(url, None, options).await
}

pub async fn connect_with_authenticator(
    url: &str,
    auth: Option<&dyn RequestAuthenticator>,
    options: &ConnectOptions,
) -> anyhow::Result<Connection> {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut request = url.into_client_request()?;
        let host = request
            .uri()
            .host()
            .context("websocket URL has no hostname")?
            .to_owned();
        let scheme = request
            .uri()
            .scheme_str()
            .context("websocket URL has no scheme")?;
        ensure!(
            matches!(scheme, "ws" | "wss"),
            "expected ws:// or wss:// URL"
        );
        let port = request
            .uri()
            .port_u16()
            .unwrap_or(if scheme == "wss" { 443 } else { 80 });
        let peer = match options.remote_ip {
            Some(ip) => SocketAddr::new(ip, port),
            None => lookup_host((host.as_str(), port))
                .await?
                .next()
                .context("DNS returned no address")?,
        };
        let tcp = if peer.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        if let Some(port) = options.source_port {
            let ip = if peer.is_ipv4() {
                IpAddr::V4(Ipv4Addr::UNSPECIFIED)
            } else {
                IpAddr::V6(Ipv6Addr::UNSPECIFIED)
            };
            tcp.bind(SocketAddr::new(ip, port))
                .context("cannot bind requested source port")?;
        }
        let stream = tcp.connect(peer).await?;
        stream.set_nodelay(true)?;
        let local = stream.local_addr()?;
        if let Some(auth) = auth {
            auth.authenticate(&mut request)?;
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(4 << 20))
            .max_frame_size(Some(4 << 20));
        // The original hostname stays in the request: fixed IP does not disable SNI/cert checks.
        let (socket, _) = client_async_tls_with_config(request, stream, Some(config), None).await?;
        Ok(Connection {
            socket,
            local,
            peer,
        })
    })
    .await
    .context("websocket connection timed out")?
}

/// Sends Close, or flushes the echo queued by a received peer Close, then
/// finishes the bounded handshake while discarding frames already in flight.
/// Errors, including a missing peer response, remain visible to the caller.
pub async fn close(socket: &mut Socket) -> anyhow::Result<()> {
    let grace = Duration::from_secs(10);
    let mut discarded = 0u64;
    tokio::time::timeout(grace, async {
        match socket.close(None).await {
            Ok(()) | Err(WsError::AlreadyClosed) | Err(WsError::ConnectionClosed) => {}
            Err(WsError::Protocol(ProtocolError::SendAfterClosing)) => {
                // tokio-tungstenite's close() sends another Message::Close.
                // After a peer Close, tungstenite has already queued its echo
                // and rejects that second send before flushing the echo.
                socket.flush().await?;
            }
            Err(e) => return Err(e.into()),
        }
        while let Some(result) = socket.next().await {
            match result {
                Ok(Message::Close(_))
                | Err(WsError::AlreadyClosed)
                | Err(WsError::ConnectionClosed) => break,
                Err(e) => return Err(e.into()),
                _ => discarded += 1,
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .with_context(|| format!("websocket close handshake timed out after {grace:?}, drained {discarded} post-cut frames"))?
}
