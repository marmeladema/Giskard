use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpAuthStatus {
    Unknown,
    Unsupported,
    NotLoggedIn,
    BearerToken,
    OAuth,
}

/// How the harness sees its connection to the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpConnectionState {
    NotStarted,
    Starting,
    Connected,
    AuthenticationRequired,
    Failed,
    Cancelled,
    Disabled,
    /// A state the adapter did not recognise; `McpConnection.error` carries its raw name.
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpConnection {
    pub state: McpConnectionState,
    /// The harness's reason for a `Failed` (or `Unknown`) state, verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpServerInfo {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub website_url: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default)]
    pub input_schema: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpResource {
    pub name: String,
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpResourceTemplate {
    pub name: String,
    pub uri_template: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct McpServerStatus {
    pub name: String,
    pub auth_status: McpAuthStatus,
    /// The harness's connection to the server; absent when the harness does not say.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<McpConnection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_info: Option<McpServerInfo>,
    #[serde(default)]
    pub tools: Vec<McpTool>,
    /// `None` when the harness cannot report resources; `Some(vec![])` when it reported none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<Vec<McpResource>>,
    /// `None` when the harness cannot report resource templates; `Some(vec![])` when it reported
    /// none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_templates: Option<Vec<McpResourceTemplate>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct McpOauthStart {
    pub authorization_url: String,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn status(
        connection: Option<McpConnection>,
        resources: Option<Vec<McpResource>>,
    ) -> McpServerStatus {
        McpServerStatus {
            name: "mini".to_owned(),
            auth_status: McpAuthStatus::Unsupported,
            connection,
            server_info: None,
            tools: Vec::new(),
            resources,
            resource_templates: None,
        }
    }

    #[test]
    fn a_status_with_a_connection_round_trips() {
        let original = status(
            Some(McpConnection {
                state: McpConnectionState::Failed,
                error: Some("ENOENT".to_owned()),
            }),
            Some(Vec::new()),
        );
        let value = serde_json::to_value(&original).unwrap();
        assert_eq!(
            value["connection"],
            json!({"state": "failed", "error": "ENOENT"})
        );
        assert_eq!(value["resources"], json!([]));
        assert!(value.get("resource_templates").is_none());
        let back: McpServerStatus = serde_json::from_value(value).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn a_status_without_a_connection_omits_the_key() {
        let original = status(None, None);
        let value = serde_json::to_value(&original).unwrap();
        assert!(value.get("connection").is_none());
        assert!(value.get("resources").is_none());
        let back: McpServerStatus = serde_json::from_value(value).unwrap();
        assert_eq!(back, original);
    }

    #[test]
    fn resources_null_absent_and_empty_are_told_apart() {
        let base = json!({"name": "mini", "auth_status": "unknown"});
        let absent: McpServerStatus = serde_json::from_value(base.clone()).unwrap();
        assert_eq!(absent.connection, None);
        assert_eq!(absent.resources, None);
        assert_eq!(absent.resource_templates, None);

        let mut null = base.clone();
        null["resources"] = json!(null);
        null["resource_templates"] = json!(null);
        let null: McpServerStatus = serde_json::from_value(null).unwrap();
        assert_eq!(null.resources, None);
        assert_eq!(null.resource_templates, None);

        let mut empty = base;
        empty["resources"] = json!([]);
        empty["resource_templates"] = json!([]);
        let empty: McpServerStatus = serde_json::from_value(empty).unwrap();
        assert_eq!(empty.resources, Some(Vec::new()));
        assert_eq!(empty.resource_templates, Some(Vec::new()));
    }

    #[test]
    fn every_connection_state_uses_its_snake_case_name() {
        for (state, name) in [
            (McpConnectionState::NotStarted, "not_started"),
            (McpConnectionState::Starting, "starting"),
            (McpConnectionState::Connected, "connected"),
            (
                McpConnectionState::AuthenticationRequired,
                "authentication_required",
            ),
            (McpConnectionState::Failed, "failed"),
            (McpConnectionState::Cancelled, "cancelled"),
            (McpConnectionState::Disabled, "disabled"),
            (McpConnectionState::Unknown, "unknown"),
        ] {
            assert_eq!(serde_json::to_value(&state).unwrap(), json!(name));
        }
    }
}
