use std::sync::Arc;

use anyhow::bail;
use codex_hooks::HookMcpCall;
use codex_hooks::HookMcpExecutor;
use codex_mcp::McpRuntime;
use codex_protocol::ThreadId;
use futures::FutureExt;
use futures::future::BoxFuture;
use serde_json::Value;

pub(crate) struct CoreHookMcpExecutor {
    pub(crate) runtime: Arc<McpRuntime>,
    // Session-scoped MCP tools require the owning thread ID in request metadata.
    pub(crate) thread_id: ThreadId,
    pub(crate) session_owned_fabric: bool,
}

impl HookMcpExecutor for CoreHookMcpExecutor {
    fn execute(&self, call: HookMcpCall) -> BoxFuture<'_, anyhow::Result<String>> {
        async move {
            let mut metadata = call.metadata.unwrap_or_default();
            metadata.insert(
                "threadId".to_string(),
                Value::String(self.thread_id.to_string()),
            );
            if self.session_owned_fabric && call.server == "fabric" {
                // Hooks are background callers too: an unsupported kernel
                // must never hand its legacy inbox to an unseen hook result.
                if !self
                    .runtime
                    .latest_server_capabilities("fabric")
                    .as_ref()
                    .is_some_and(crate::session::fabric_inbox::supports_kernel)
                {
                    bail!("Fabric kernel does not advertise session-owned delivery");
                }
                metadata.insert(
                    "fabricDelivery".to_string(),
                    Value::String("session".to_string()),
                );
            }

            let result = self
                .runtime
                .latest_call_tool(
                    &call.server,
                    &call.tool,
                    call.environment_id.as_deref(),
                    Some(Value::Object(call.input)),
                    Some(Value::Object(metadata)),
                    Some(call.timeout),
                    /*wait_for_server*/ false,
                )
                .await?;
            let text = result
                .content
                .iter()
                .filter_map(|content| {
                    (content.get("type").and_then(Value::as_str) == Some("text"))
                        .then(|| content.get("text").and_then(Value::as_str))
                        .flatten()
                })
                .collect::<Vec<_>>()
                .join("\n");
            if result.is_error == Some(true) {
                bail!("MCP tool returned an error: {text}");
            }

            Ok(text)
        }
        .boxed()
    }
}
