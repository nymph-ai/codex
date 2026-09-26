use std::sync::Arc;
use std::time::Duration;

use codex_config::types::McpServerTransportConfig;
use codex_mcp::McpRuntimeContext;
use codex_rmcp_client::StreamableHttpRedirectMode;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::auth_manager::AuthManager;
use crate::config_manager::ConfigManager;
use crate::thread_manager::ThreadManager;

const MCP_OAUTH_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct McpOAuthRefreshWorker {
    shutdown: CancellationToken,
    _task: JoinHandle<()>,
}

impl McpOAuthRefreshWorker {
    pub(crate) fn shutdown(&self) {
        self.shutdown.cancel();
    }
}

impl Drop for McpOAuthRefreshWorker {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub(crate) fn spawn(
    config_manager: ConfigManager,
    auth_manager: Arc<AuthManager>,
    thread_manager: Arc<ThreadManager>,
) -> McpOAuthRefreshWorker {
    spawn_with_interval(
        config_manager,
        auth_manager,
        thread_manager,
        MCP_OAUTH_REFRESH_INTERVAL,
    )
}

pub(crate) fn spawn_with_interval(
    config_manager: ConfigManager,
    auth_manager: Arc<AuthManager>,
    thread_manager: Arc<ThreadManager>,
    refresh_interval: Duration,
) -> McpOAuthRefreshWorker {
    let shutdown = CancellationToken::new();
    let worker_shutdown = shutdown.clone();
    let task = tokio::spawn(async move {
        loop {
            if worker_shutdown.is_cancelled() {
                break;
            }

            if let Err(err) =
                refresh_expiring_tokens(&config_manager, &auth_manager, &thread_manager).await
            {
                debug!("MCP OAuth refresh check encountered an error: {err}");
            }

            tokio::select! {
                _ = worker_shutdown.cancelled() => break,
                _ = tokio::time::sleep(refresh_interval) => {}
            }
        }
    });

    McpOAuthRefreshWorker {
        shutdown,
        _task: task,
    }
}

async fn refresh_expiring_tokens(
    config_manager: &ConfigManager,
    auth_manager: &Arc<AuthManager>,
    thread_manager: &Arc<ThreadManager>,
) -> anyhow::Result<()> {
    let config = config_manager.load_latest_config(None).await?;
    let auth = auth_manager.auth().await;
    let mcp_config = thread_manager.mcp_manager().runtime_config(&config).await;
    let runtime_context = McpRuntimeContext::new(
        thread_manager.environment_manager(),
        config.cwd.to_path_buf(),
    );
    let effective_servers = codex_mcp::effective_mcp_servers(&mcp_config, auth.as_ref());

    for (name, server) in effective_servers {
        let redirect_mode = if server.is_agent_plugin() {
            StreamableHttpRedirectMode::AgentPluginV1
        } else {
            StreamableHttpRedirectMode::Legacy
        };
        let server_cfg = server.config();
        let (url, http_headers, env_http_headers) = match &server_cfg.transport {
            McpServerTransportConfig::StreamableHttp {
                url,
                http_headers,
                env_http_headers,
                ..
            } => (url.clone(), http_headers.clone(), env_http_headers.clone()),
            _ => continue,
        };

        let oauth_credential_name = server_cfg.oauth_credential_name(&name);
        let Ok(Some(snapshot)) = codex_rmcp_client::stored_oauth_credential_snapshot(
            oauth_credential_name.as_ref(),
            &url,
            mcp_config.mcp_oauth_credentials_store_mode,
            mcp_config.auth_keyring_backend_kind,
        ) else {
            continue;
        };

        if !snapshot.tokens.has_refresh_token() {
            continue;
        }

        if !codex_rmcp_client::token_needs_refresh(snapshot.tokens.expires_at) {
            continue;
        }

        let Ok(http_client) = runtime_context.resolve_http_client(&name, server_cfg) else {
            continue;
        };
        let Ok(default_headers) =
            codex_rmcp_client::build_default_headers(http_headers, env_http_headers)
        else {
            continue;
        };

        info!("Proactively refreshing OAuth tokens for MCP server `{name}` before expiry");
        match codex_rmcp_client::refresh_oauth_tokens(
            oauth_credential_name.as_ref(),
            &url,
            snapshot.tokens,
            snapshot.store,
            default_headers,
            http_client,
            redirect_mode,
            /*force_refresh*/ false,
        )
        .await
        {
            Ok(_) => {
                info!("Successfully refreshed OAuth tokens for MCP server `{name}`");
            }
            Err(err) => {
                warn!("Background OAuth refresh failed for MCP server `{name}`: {err}");
            }
        }
    }

    Ok(())
}
