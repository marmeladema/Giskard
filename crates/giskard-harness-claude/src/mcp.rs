//! The MCP server list Claude Code reports in its `mcp_status` control response.
//!
//! `mcp_status` answers `{"mcpServers": [...]}`. A **connected** entry is
//! `{name, status: "connected", serverInfo: {name, title, version}, config, scope, source,
//! tools: [{name, annotations}]}`; a failed one is `{name, status: "failed", error, config, scope,
//! source}`, with no `serverInfo` and no `tools`. `tools[].name` is the bare tool name (`echo`, not
//! `mcp__<server>__<tool>`), and the CLI relays neither the description nor the input schema the
//! server advertised, so each tool carries its name only.
//!
//! Resources are not reachable from a stdio host: no control request lists them (the model reaches
//! them through the CLI's own `ListMcpResourcesTool` / `ReadMcpResourceTool`), so every status this
//! module builds says "not reported" (`None`) for resources and resource templates rather than a
//! false zero.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex, PoisonError};

use giskard_core::mcp::{
    McpAuthStatus, McpConnection, McpConnectionState, McpServerInfo, McpServerStatus, McpTool,
};
use serde::Deserialize;
use serde_json::Value;
use tracing::{debug, warn};

/// One `mcpServers` entry. Unknown keys (`scope`, the rest of `config`) are ignored.
#[derive(Debug, Deserialize)]
struct ServerEntry {
    name: String,
    status: String,
    error: Option<String>,
    source: Option<String>,
    #[serde(rename = "serverInfo")]
    server_info: Option<ServerInfoEntry>,
    /// Kept as raw values so that one odd tool entry is skipped without losing the server.
    tools: Option<Vec<Value>>,
    config: Option<ConfigEntry>,
}

/// The `serverInfo` the server gave the CLI in its MCP handshake.
#[derive(Debug, Deserialize)]
struct ServerInfoEntry {
    name: String,
    title: Option<String>,
    version: Option<String>,
}

/// One `tools` entry. Unknown keys (`annotations`) are ignored.
#[derive(Debug, Deserialize)]
struct ToolEntry {
    name: String,
}

/// The transport part of the server's configuration.
#[derive(Debug, Deserialize)]
struct ConfigEntry {
    r#type: Option<String>,
}

/// The status of a server waiting for an authentication the user must complete. The 2.1.287
/// binary's strings settled the spelling: `needs-auth` occurs, `needs_auth` does not.
const NEEDS_AUTH: &str = "needs-auth";

/// The unrecognised statuses already warned about, so each costs one log line per process rather
/// than one per menu open. Bounded by the CLI's status vocabulary.
static UNRECOGNISED_STATUSES: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

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
    let connection = connection(&entry.status, entry.error.as_deref());
    let tools = tools(&entry.name, entry.tools.as_deref().unwrap_or_default());
    debug!(
        action = "mcp_status",
        name = %entry.name,
        status = %entry.status,
        connection = ?connection.state,
        source = crate::log_fields::display_opt(entry.source.as_deref()),
        tools = tools.len(),
        error = crate::log_fields::display_opt(entry.error.as_deref()),
        "Claude Code reported an MCP server"
    );
    let transport = entry
        .config
        .as_ref()
        .and_then(|config| config.r#type.as_deref());
    // A stdio server has no authentication concept; a remote one (`http`, `sse`) says nothing
    // about its auth unless it waits for a login.
    let auth_status = if entry.status == NEEDS_AUTH {
        McpAuthStatus::NotLoggedIn
    } else if transport == Some("stdio") {
        McpAuthStatus::Unsupported
    } else {
        McpAuthStatus::Unknown
    };
    McpServerStatus {
        name: entry.name,
        auth_status,
        connection: Some(connection),
        server_info: entry.server_info.map(|info| McpServerInfo {
            name: info.name,
            title: info.title,
            description: None,
            version: info.version,
            website_url: None,
        }),
        tools,
        resources: None,
        resource_templates: None,
    }
}

/// The CLI's status vocabulary (2.1.287) onto the neutral connection state. An unrecognised status
/// is `Unknown` with its raw name as the error, warned about once per status string.
fn connection(status: &str, error: Option<&str>) -> McpConnection {
    let state = match status {
        "connected" => McpConnectionState::Connected,
        "pending" | "reconnecting" => McpConnectionState::Starting,
        "failed" => McpConnectionState::Failed,
        "disabled" => McpConnectionState::Disabled,
        NEEDS_AUTH => McpConnectionState::AuthenticationRequired,
        other => {
            let first = UNRECOGNISED_STATUSES
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .insert(other.to_owned());
            if first {
                warn!(
                    action = "mcp_status",
                    status = %other,
                    "unrecognised MCP server status"
                );
            }
            return McpConnection {
                state: McpConnectionState::Unknown,
                error: Some(other.to_owned()),
            };
        }
    };
    McpConnection {
        state,
        error: error.map(str::to_owned),
    }
}

/// One `McpTool` per named entry; an entry without a name is skipped with a warning.
fn tools(server: &str, entries: &[Value]) -> Vec<McpTool> {
    entries
        .iter()
        .enumerate()
        .filter_map(
            |(index, entry)| match ToolEntry::deserialize(entry.clone()) {
                Ok(tool) => Some(McpTool {
                    name: tool.name,
                    title: None,
                    description: None,
                    input_schema: Value::Null,
                    output_schema: None,
                }),
                Err(error) => {
                    warn!(
                        action = "mcp_status",
                        name = %server,
                        index,
                        error = %crate::frame::redact_serde_error(&error),
                        "skipping an MCP tool entry without a name"
                    );
                    None
                }
            },
        )
        .collect()
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

    /// The `mcp_status` payload of the `mcp-status` fixture (line 2), recorded on 2.1.287: a
    /// connected stdio server with one tool, and a broken one.
    pub(crate) fn connected_and_failed() -> Value {
        let path = format!(
            "{}/tests/fixtures/mcp-status.out.jsonl",
            env!("CARGO_MANIFEST_DIR")
        );
        let text = std::fs::read_to_string(path).unwrap();
        let line: Value = serde_json::from_str(text.lines().nth(1).unwrap()).unwrap();
        line["response"]["response"].clone()
    }

    /// Assert `servers` is what `connected_and_failed` maps to.
    pub(crate) fn assert_connected_and_failed(servers: &[McpServerStatus]) {
        assert_eq!(servers.len(), 2);
        let mini = &servers[0];
        assert_eq!(mini.name, "mini");
        assert_eq!(
            mini.connection,
            Some(McpConnection {
                state: McpConnectionState::Connected,
                error: None,
            })
        );
        assert_eq!(mini.auth_status, McpAuthStatus::Unsupported);
        assert_eq!(
            mini.server_info,
            Some(McpServerInfo {
                name: "mini".into(),
                title: Some("Mini Server".into()),
                description: None,
                version: Some("0.1.0".into()),
                website_url: None,
            })
        );
        let tools: Vec<_> = mini.tools.iter().map(|tool| tool.name.as_str()).collect();
        assert_eq!(tools, ["echo"]);
        assert_eq!(mini.tools[0].input_schema, Value::Null);
        assert_eq!(mini.resources, None);
        assert_eq!(mini.resource_templates, None);

        let broken = &servers[1];
        assert_eq!(broken.name, "broken");
        assert_eq!(
            broken.connection,
            Some(McpConnection {
                state: McpConnectionState::Failed,
                error: Some("ENOENT: no such file or directory, posix_spawn 'stdio'".into()),
            })
        );
        assert_eq!(broken.auth_status, McpAuthStatus::Unsupported);
        assert_eq!(broken.server_info, None);
        assert!(broken.tools.is_empty());
        assert_eq!(broken.resources, None);
    }

    fn state(server: &McpServerStatus) -> Option<&McpConnectionState> {
        server
            .connection
            .as_ref()
            .map(|connection| &connection.state)
    }

    #[test]
    fn an_empty_list_maps_to_no_servers() {
        assert!(mcp_servers(&json!({"mcpServers": []})).is_empty());
    }

    #[test]
    fn a_failed_server_carries_its_error_and_a_pending_one_is_starting() {
        let servers = mcp_servers(&failed_and_pending());
        assert_eq!(servers.len(), 2);
        assert_eq!(servers[0].name, "broken");
        assert_eq!(
            servers[0].connection,
            Some(McpConnection {
                state: McpConnectionState::Failed,
                error: Some("ENOENT: no such file or directory, posix_spawn 'stdio'".into()),
            })
        );
        assert_eq!(servers[1].name, "echo");
        assert_eq!(state(&servers[1]), Some(&McpConnectionState::Starting));
        assert!(
            servers
                .iter()
                .all(|server| server.auth_status == McpAuthStatus::Unsupported
                    && server.server_info.is_none()
                    && server.tools.is_empty()
                    && server.resources.is_none()
                    && server.resource_templates.is_none())
        );
    }

    #[test]
    fn a_connected_server_carries_its_server_info_and_tools() {
        assert_connected_and_failed(&mcp_servers(&connected_and_failed()));
    }

    #[test]
    fn a_server_awaiting_authentication_is_not_logged_in() {
        let servers = mcp_servers(&json!({"mcpServers": [
            {"name": "remote", "status": "needs-auth", "config": {"type": "http"}, "source": "user"}
        ]}));
        assert_eq!(servers[0].auth_status, McpAuthStatus::NotLoggedIn);
        assert_eq!(
            state(&servers[0]),
            Some(&McpConnectionState::AuthenticationRequired)
        );
    }

    #[test]
    fn a_remote_server_s_auth_stays_unknown() {
        let servers = mcp_servers(&json!({"mcpServers": [
            {"name": "remote", "status": "connected", "config": {"type": "sse"}},
            {"name": "bare", "status": "connected"}
        ]}));
        assert!(
            servers
                .iter()
                .all(|server| server.auth_status == McpAuthStatus::Unknown)
        );
    }

    #[test]
    fn every_known_status_maps_to_its_connection_state() {
        for (status, expected) in [
            ("connected", McpConnectionState::Connected),
            ("pending", McpConnectionState::Starting),
            ("reconnecting", McpConnectionState::Starting),
            ("failed", McpConnectionState::Failed),
            ("disabled", McpConnectionState::Disabled),
            ("needs-auth", McpConnectionState::AuthenticationRequired),
        ] {
            let servers = mcp_servers(&json!({"mcpServers": [{"name": "s", "status": status}]}));
            assert_eq!(state(&servers[0]), Some(&expected), "{status}");
        }
    }

    #[traced_test]
    #[test]
    fn an_unrecognised_status_is_unknown_and_warned_about_once() {
        let payload = json!({"mcpServers": [
            {"name": "odd", "status": "hibernating-test-only"},
            {"name": "odder", "status": "hibernating-test-only"}
        ]});
        let servers = mcp_servers(&payload);
        assert_eq!(servers.len(), 2);
        assert!(servers.iter().all(|server| server.connection
            == Some(McpConnection {
                state: McpConnectionState::Unknown,
                error: Some("hibernating-test-only".into()),
            })));
        mcp_servers(&payload);
        assert!(logs_contain("unrecognised MCP server status"));
        logs_assert(|lines: &[&str]| {
            let warnings = lines
                .iter()
                .filter(|line| {
                    line.contains("unrecognised MCP server status")
                        && line.contains("status=hibernating-test-only")
                })
                .count();
            if warnings == 1 {
                Ok(())
            } else {
                Err(format!("expected one warning, saw {warnings}"))
            }
        });
    }

    #[traced_test]
    #[test]
    fn a_tool_entry_without_a_name_is_skipped_with_a_warning() {
        let servers = mcp_servers(&json!({"mcpServers": [
            {
                "name": "mini",
                "status": "connected",
                "tools": [{"annotations": {}}, {"name": "echo", "annotations": {}}]
            }
        ]}));
        assert_eq!(servers.len(), 1, "the server is kept");
        let tools: Vec<_> = servers[0]
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect();
        assert_eq!(tools, ["echo"]);
        assert!(logs_contain("skipping an MCP tool entry without a name"));
        assert!(logs_contain("name=mini"));
        assert!(logs_contain("index=0"));
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
