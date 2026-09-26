//! Selects the owner of MCP OAuth refresh and credential persistence.

/// MCP OAuth policy pinned for the lifetime of a connection.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum McpOAuthRefreshMode {
    /// Keep Codex's existing refresh and persistence path.
    Legacy,
    /// Let RMCP coordinate refresh through Codex's credential store.
    Coordinated,
    /// Centralize OAuth refresh in the long-running app-server daemon and lease access tokens over UDS.
    #[default]
    DaemonLease,
}
