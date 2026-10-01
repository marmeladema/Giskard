//! The MCP server list Claude Code reports in its `mcp_status` control response.
//!
//! `mcp_status` answers `{"mcpServers": [{name, status, error?, config, scope, source}]}`. It
//! carries no tool inventory: tools reach the model as `mcp__<server>__<tool>` names in
//! `system/init.tools`, so every status this module builds has empty tool, resource and template
//! lists.

use giskard_core::mcp::{McpAuthStatus, McpServerInfo, McpServerStatus};
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

/// One `mcpServers` entry. Unknown keys (`config`, `scope`) are ignored.
#[derive(Debug, Deserialize)]
struct ServerEntry {
    name: String,
    status: String,
    error: Option<String>,
    source: Option<String>,
}

/// The statuses that mean the server waits for an authentication the user must complete.
///
/// The CLI's own `/mcp` screen names such a state, but its wire spelling was not observed, so
/// both plausible spellings are matched and everything else stays `Unknown`.
const NEEDS_AUTH: [&str; 2] = ["needs-auth", "needs_auth"];

/// Map an `mcp_status` success payload. An entry that does not parse is skipped, so one odd
/// server cannot empty the panel.
pub(crate) fn mcp_servers(payload: &Value) -> Vec<McpServerStatus> {
    let Some(entries) = payload.get("mcpServers").and_then(Value::as_array) else {
        warn!(
            action = "mcp_status",
            "the mcp_status response carried no mcpServers list"
        );
        return Vec::new();
    };
    entries
        .iter()
        .enumerate()
        .filter_map(
            |(index, entry)| match ServerEntry::deserialize(entry.clone()) {
                Ok(entry) => Some(status(entry)),
                Err(error) => {
                    warn!(
                        action = "mcp_status",
                        index,
                        error = %crate::frame::redact_serde_error(&error),
                        "skipping an MCP server entry that did not parse"
                    );
                    None
                }
            },
        )
        .collect()
}

fn status(entry: ServerEntry) -> McpServerStatus {
    debug!(
        action = "mcp_status",
        name = %entry.name,
        status = %entry.status,
        source = crate::log_fields::display_opt(entry.source.as_deref()),
        error = crate::log_fields::display_opt(entry.error.as_deref()),
        "Claude Code reported an MCP server"
    );
    let auth_status = if NEEDS_AUTH.contains(&entry.status.as_str()) {
        McpAuthStatus::NotLoggedIn
    } else {
        McpAuthStatus::Unknown
    };
    let description = match &entry.error {
        Some(error) => format!("{}: {error}", entry.status),
        None => entry.status.clone(),
    };
    McpServerStatus {
        name: entry.name.clone(),
        auth_status,
        server_info: Some(McpServerInfo {
            name: entry.name,
            title: None,
            description: Some(description),
            version: None,
            website_url: None,
        }),
        tools: Vec::new(),
        resources: Vec::new(),
        resource_templates: Vec::new(),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use serde_json::json;
    use tracing_test::traced_test;

    use super::*;

    /// The payload Claude Code 2.1.286 answered with `--mcp-config` naming two stdio servers.
    pub(crate) fn failed_and_pending() -> Value {
        json!({"mcpServers": [
            {
                "name": "broken",
                "status": "failed",
                "error": "ENOENT: no such file or directory, posix_spawn 'stdio'",
                "config": {"type": "stdio", "command": "/nonexistent/mcp-server", "args": []},
                "scope": "dynamic",
                "source": "dynamic"
            },
            {
                "name": "echo",
                "status": "pending",
                "config": {"type": "stdio", "command": "/bin/cat", "args": []},
                "scope": "dynamic",
                "source": "dynamic"
            }
        ]})
    }

    #[test]
    fn an_empty_list_maps_to_no_servers() {
        assert!(mcp_servers(&json!({"mcpServers": []})).is_empty());
    }

    #[test]
    fn a_failed_server_shows_its_error_and_a_pending_one_its_status() {
        let servers = mcp_servers(&failed_and_pending());
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].name, "broken");
        assert_eq!(servers[0].auth_status, McpAuthStatus::Unknown);
        assert_eq!(
            servers[0]
                .server_info
                .as_ref()
                .and_then(|info| info.description.as_deref()),
            Some("failed: ENOENT: no such file or directory, posix_spawn 'stdio'")
        );
        assert_eq!(servers[1].name, "echo");
        assert_eq!(
            servers[1]
                .server_info
                .as_ref()
                .and_then(|info| info.description.as_deref()),
            Some("pending")
        );
        assert!(servers.iter().all(|server| server.tools.is_empty()
            && server.resources.is_empty()
            && server.resource_templates.is_empty()));
    }

    #[test]
    fn a_server_awaiting_authentication_is_not_logged_in() {
        for status in NEEDS_AUTH {
            let servers = mcp_servers(&json!({"mcpServers": [
                {"name": "remote", "status": status, "source": "user"}
            ]}));
            assert_eq!(servers[0].auth_status, McpAuthStatus::NotLoggedIn);
        }
    }

    #[traced_test]
    #[test]
    fn an_entry_that_does_not_parse_is_skipped_with_a_warning() {
        let servers = mcp_servers(&json!({"mcpServers": [
            {"status": "connected"},
            {"name": "ok", "status": "connected"}
        ]}));
        assert_eq!(servers.len(), 1);
        assert_eq!(servers[0].name, "ok");
        assert!(logs_contain(
            "skipping an MCP server entry that did not parse"
        ));
        assert!(logs_contain("index=0"));
    }

    #[traced_test]
    #[test]
    fn a_payload_without_a_list_is_empty_with_a_warning() {
        assert!(mcp_servers(&json!({})).is_empty());
        assert!(logs_contain("carried no mcpServers list"));
    }
}
