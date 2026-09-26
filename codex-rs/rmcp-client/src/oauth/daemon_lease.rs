use std::path::{Path, PathBuf};
use anyhow::{anyhow, bail, Context, Result};
use codex_app_server_protocol::McpGetAuthTokenResponse;
use codex_uds::UnixStream;
use codex_utils_home_dir::find_codex_home;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio_tungstenite::client_async_with_config;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};

const UDS_WEBSOCKET_HANDSHAKE_URL: &str = "ws://localhost/mcp";

/// Resolves the app-server daemon control socket path, checking both the
/// standard CODEX_HOME directory and the shared daemon socket directory.
pub fn resolve_daemon_control_socket_path() -> Option<PathBuf> {
    if let Ok(codex_home) = find_codex_home() {
        let sock = codex_home
            .join("app-server-control")
            .join("app-server-control.sock");
        if sock.exists() {
            return Some(sock);
        }
    }

    if let Ok(daemon_dir) = codex_uds::shared_daemon_socket_directory() {
        if daemon_dir.exists() {
            if let Ok(entries) = std::fs::read_dir(&daemon_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_none() && path.exists() {
                        return Some(path);
                    }
                }
            }
        }
    }

    None
}

/// Requests a leased MCP OAuth access token from the running app-server daemon.
pub async fn lease_access_token_from_daemon(
    server_name: &str,
    force_refresh: bool,
) -> Result<McpGetAuthTokenResponse> {
    let socket_path = resolve_daemon_control_socket_path()
        .ok_or_else(|| anyhow!("Codex app-server daemon control socket not found"))?;
    lease_access_token_from_socket(&socket_path, server_name, force_refresh).await
}

/// Requests a leased MCP OAuth access token from the given daemon socket path.
pub async fn lease_access_token_from_socket(
    socket_path: &Path,
    server_name: &str,
    force_refresh: bool,
) -> Result<McpGetAuthTokenResponse> {
    debug!(
        socket = %socket_path.display(),
        server = %server_name,
        force_refresh,
        "leasing MCP OAuth access token from app-server daemon"
    );

    let request = UDS_WEBSOCKET_HANDSHAKE_URL
        .into_client_request()
        .map_err(|err| anyhow!("invalid UDS websocket handshake URL: {err}"))?;
    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| format!("failed to connect to daemon socket at {}", socket_path.display()))?;
    let (mut ws_stream, _) = client_async_with_config(request, stream, None)
        .await
        .with_context(|| format!("failed to upgrade websocket on daemon socket at {}", socket_path.display()))?;

    let req = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "mcp/getAuthToken",
        "params": {
            "server_name": server_name,
            "force_refresh": force_refresh
        }
    });

    ws_stream
        .send(Message::Text(req.to_string()))
        .await
        .context("failed to send mcp/getAuthToken request to daemon")?;

    while let Some(msg) = ws_stream.next().await {
        match msg.context("error reading from daemon websocket")? {
            Message::Text(text) => {
                let v: Value = serde_json::from_str(&text)
                    .with_context(|| format!("invalid JSON received from daemon: {text}"))?;
                if v.get("id") == Some(&json!(1)) {
                    if let Some(err) = v.get("error") {
                        let err_msg = err
                            .get("message")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown error");
                        bail!("Daemon error retrieving auth token for '{server_name}': {err_msg}");
                    }
                    if let Some(res) = v.get("result") {
                        let resp: McpGetAuthTokenResponse = serde_json::from_value(res.clone())
                            .context("failed to deserialize McpGetAuthTokenResponse")?;
                        let _ = ws_stream.close(None).await;
                        return Ok(resp);
                    }
                }
            }
            Message::Close(frame) => {
                let reason = frame.map(|f| f.reason.to_string()).unwrap_or_default();
                bail!("daemon closed websocket connection before responding: {reason}");
            }
            _ => {}
        }
    }

    bail!("daemon closed connection without sending a response for mcp/getAuthToken")
}
