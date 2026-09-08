//! Refresh notified catalogs without reconnecting unrelated servers or changing
//! an already captured binding. Network work happens outside the revision gate;
//! publishing waits for calls using the old revision to finish.

use std::collections::HashMap;
use std::time::Duration;

use tokio::time::Instant;
use tracing::warn;

use super::McpConnectionSet;
use crate::mcp::CODEX_APPS_MCP_SERVER_NAME;
use crate::rmcp_client::list_tools_for_client_uncached;

const REFRESH_TIMEOUT: Duration = Duration::from_secs(10);
const RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Default)]
pub(super) struct RefreshState {
    generation: u64,
    retry_at: Option<Instant>,
}

impl McpConnectionSet {
    /// At most one bounded fetch per dirty server per capture. Holding the
    /// refresh mutex coalesces concurrent readers; taking the generation before
    /// fetching keeps notifications received during that fetch pending.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "catalog publication is serialized with captured tool calls"
    )]
    pub(super) async fn refresh_notified_tool_catalogs(&self) -> HashMap<String, Option<u64>> {
        let mut generations = HashMap::new();
        let mut states = self.tool_catalog_refresh.lock().await;
        for (server_name, view) in &self.servers {
            let Some(transport) = view.connection.client.ready_transport() else {
                continue;
            };
            let generation = transport.tool_list_generation();
            let state = states.entry(server_name.clone()).or_default();
            if generation == state.generation {
                generations.insert(server_name.clone(), Some(generation));
                continue;
            }
            generations.insert(server_name.clone(), None);
            if state
                .retry_at
                .is_some_and(|retry_at| Instant::now() < retry_at)
            {
                continue;
            }
            let timeout = view
                .tool_timeout
                .unwrap_or(REFRESH_TIMEOUT)
                .min(REFRESH_TIMEOUT);
            let result = tokio::time::timeout(timeout, async {
                if server_name == CODEX_APPS_MCP_SERVER_NAME {
                    self.hard_refresh_codex_apps_tools_cache().await?;
                    return Ok(());
                }
                let client = view.connection.client().await?;
                let tools = list_tools_for_client_uncached(
                    server_name,
                    /*is_codex_apps_mcp_server*/ false,
                    "notification",
                    &client.client,
                    Some(timeout),
                    view.catalog_item_limit,
                    client.server_instructions.as_deref(),
                )
                .await?;
                let mut revision = self.tool_catalog_revision.write().await;
                let mut overrides = self.tool_catalog_overrides.write().await;
                overrides.insert(server_name.clone(), tools);
                // Even an identical catalog needs a new binding: previously
                // prepared calls captured the prior notification generation.
                *revision += 1;
                Ok::<(), anyhow::Error>(())
            })
            .await;
            match result {
                Ok(Ok(())) => {
                    state.generation = generation;
                    state.retry_at = None;
                    if transport.tool_list_generation() == generation {
                        generations.insert(server_name.clone(), Some(generation));
                    }
                }
                result => {
                    // Do not expose stale permissions in a new binding. Keep
                    // this server unavailable until a complete fetch succeeds;
                    // retry later even without another notification.
                    state.retry_at = Some(Instant::now() + RETRY_DELAY);
                    warn!(server_name, ?result, "MCP tool catalog refresh failed");
                }
            }
        }
        generations
    }
}
