//! Templates for `[harnesses.<name>.env]` values.
//!
//! A value is literal text with `{placeholder}`s, resolved once per harness instance — that is,
//! once per project and declaration — when the instance is created. `{{` and `}}` are literal
//! braces. The placeholders are the [`TemplateVar`]s plus `{env:NAME}`: `NAME` as it stands at that
//! line of the table, as in a shell — a variable defined earlier in the same table, else Giskard's
//! own environment.
//!
//! Values may be credentials. Nothing here formats a value into an error or a log field: a parse
//! error names the offending placeholder only when it looks like one (an identifier), and otherwise
//! only a byte offset. (`toml` itself quotes the source line of any value that fails to load, as it
//! does for a syntax error; that is outside this module.)

use std::env::VarError;
use std::fmt;

/// A placeholder that names a fact about the harness instance being created.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemplateVar {
    /// The project's id.
    ProjectId,
    /// The project's display name.
    ProjectName,
    /// The project's directory, as chosen when it was created.
    ProjectDir,
    /// The declaration's `[harnesses.<name>]` key.
    HarnessName,
    /// Giskard's data directory (`GISKARD_DATA_DIR`).
    DataDir,
}

impl TemplateVar {
    pub const ALL: [TemplateVar; 5] = [
        Self::ProjectId,
        Self::ProjectName,
        Self::ProjectDir,
        Self::HarnessName,
        Self::DataDir,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::ProjectId => "project_id",
            Self::ProjectName => "project_name",
            Self::ProjectDir => "project_dir",
            Self::HarnessName => "harness_name",
            Self::DataDir => "data_dir",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|var| var.name() == name)
    }
}

/// The prefix of a placeholder that reads the environment: `{env:NAME}`.
const ENV_PREFIX: &str = "env:";

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Literal(String),
    Var(TemplateVar),
    Env(String),
}

/// A parsed template value.
#[derive(Clone, PartialEq, Eq)]
pub struct EnvTemplate(Vec<Segment>);

/// Why a value is not a valid template. Never carries the value itself.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TemplateError {
    #[error(
        "unknown placeholder `{{{0}}}`; known: {known}, and `{{env:NAME}}`. Write `{{{{` and \
         `}}}}` for literal braces",
        known = TemplateVar::ALL.map(|var| format!("`{{{}}}`", var.name())).join(", ")
    )]
    UnknownPlaceholder(String),
    #[error("`{{env:}}` needs a variable name, as in `{{env:HOME}}`")]
    EmptyEnvName,
    /// Not echoed: like any brace contents, it may be part of a credential.
    #[error(
        "the `{{env:...}}` at byte {0} names an invalid variable: a name must not contain \
         whitespace, `=` or NUL"
    )]
    InvalidEnvName(usize),
    #[error("unbalanced `{{` or `}}` at byte {0}; write `{{{{` and `}}}}` for literal braces")]
    Unbalanced(usize),
    /// Braces around something that is not a placeholder name. Its contents are not echoed.
    #[error(
        "the `{{...}}` at byte {0} is not a placeholder; write `{{{{` and `}}}}` for literal braces"
    )]
    NotAPlaceholder(usize),
}

impl EnvTemplate {
    pub fn parse(source: &str) -> Result<Self, TemplateError> {
        let mut segments = Vec::new();
        let mut literal = String::new();
        let mut rest = source.char_indices().peekable();
        while let Some((offset, ch)) = rest.next() {
            match ch {
                '{' if matches!(rest.peek(), Some((_, '{'))) => {
                    rest.next();
                    literal.push('{');
                }
                '}' if matches!(rest.peek(), Some((_, '}'))) => {
                    rest.next();
                    literal.push('}');
                }
                '}' => return Err(TemplateError::Unbalanced(offset)),
                '{' => {
                    let mut name = String::new();
                    let mut closed = false;
                    for (_, ch) in rest.by_ref() {
                        if ch == '}' {
                            closed = true;
                            break;
                        }
                        if ch == '{' {
                            return Err(TemplateError::Unbalanced(offset));
                        }
                        name.push(ch);
                    }
                    if !closed {
                        return Err(TemplateError::Unbalanced(offset));
                    }
                    if !literal.is_empty() {
                        segments.push(Segment::Literal(std::mem::take(&mut literal)));
                    }
                    segments.push(Self::placeholder(&name, offset)?);
                }
                ch => literal.push(ch),
            }
        }
        if !literal.is_empty() {
            segments.push(Segment::Literal(literal));
        }
        Ok(Self(segments))
    }

    fn placeholder(name: &str, offset: usize) -> Result<Segment, TemplateError> {
        if let Some(variable) = name.strip_prefix(ENV_PREFIX) {
            if variable.is_empty() {
                return Err(TemplateError::EmptyEnvName);
            }
            // A name no environment can hold would only ever be "unset" at runtime: refuse it at
            // boot instead. Whitespace is refused too, so `{env: PATH}` is a typo, not a lookup.
            if variable
                .chars()
                .any(|ch| ch == '=' || ch == '\0' || ch.is_whitespace())
            {
                return Err(TemplateError::InvalidEnvName(offset));
            }
            return Ok(Segment::Env(variable.to_owned()));
        }
        if let Some(var) = TemplateVar::from_name(name) {
            return Ok(Segment::Var(var));
        }
        // Only an identifier is echoed back: a brace pair inside a credential is not a
        // placeholder, and its contents must not reach an error message.
        let identifier = !name.is_empty()
            && name.len() <= 64
            && name
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_');
        if identifier {
            Err(TemplateError::UnknownPlaceholder(name.to_owned()))
        } else {
            Err(TemplateError::NotAPlaceholder(offset))
        }
    }

    /// The template written back as a `config.toml` value: literal braces doubled again. Parsing
    /// the result yields this template.
    pub fn source(&self) -> String {
        let mut out = String::new();
        for segment in &self.0 {
            match segment {
                Segment::Literal(text) => {
                    out.push_str(&text.replace('{', "{{").replace('}', "}}"));
                }
                Segment::Var(var) => {
                    out.push('{');
                    out.push_str(var.name());
                    out.push('}');
                }
                Segment::Env(name) => {
                    out.push('{');
                    out.push_str(ENV_PREFIX);
                    out.push_str(name);
                    out.push('}');
                }
            }
        }
        out
    }

    /// Substitute every placeholder, or name the first one that has no value.
    pub fn render(
        &self,
        context: &EnvTemplateContext<'_>,
        lookup_env: &dyn Fn(&str) -> Result<String, VarError>,
    ) -> Result<String, Unresolvable> {
        let mut out = String::new();
        for segment in &self.0 {
            match segment {
                Segment::Literal(text) => out.push_str(text),
                Segment::Var(var) => match context.get(*var) {
                    // A literal with NUL fails the config load, and no environment value can hold
                    // one; a fact can (a quoted TOML key, a project name), and no process can be
                    // given it.
                    Some(value) if value.contains('\0') => {
                        return Err(Unresolvable::VarContainsNul(*var));
                    }
                    Some(value) => out.push_str(value),
                    None => return Err(Unresolvable::Var(*var)),
                },
                Segment::Env(name) => match lookup_env(name) {
                    Ok(value) => out.push_str(&value),
                    Err(VarError::NotPresent) => return Err(Unresolvable::Env(name.clone())),
                    Err(VarError::NotUnicode(_)) => {
                        return Err(Unresolvable::EnvNotUtf8(name.clone()));
                    }
                },
            }
        }
        Ok(out)
    }
}

/// Segments only by kind: a literal may be a credential.
impl fmt::Debug for EnvTemplate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let placeholders: Vec<String> = self
            .0
            .iter()
            .filter_map(|segment| match segment {
                Segment::Literal(_) => None,
                Segment::Var(var) => Some(format!("{{{}}}", var.name())),
                Segment::Env(name) => Some(format!("{{{ENV_PREFIX}{name}}}")),
            })
            .collect();
        formatter
            .debug_struct("EnvTemplate")
            .field("placeholders", &placeholders)
            .finish()
    }
}

/// A placeholder with no value at resolution time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unresolvable {
    Var(TemplateVar),
    /// `{env:NAME}` with `NAME` neither set earlier in the table nor in Giskard's environment.
    Env(String),
    /// A placeholder whose value contains NUL, which no process environment can carry.
    VarContainsNul(TemplateVar),
    /// `{env:NAME}` with `NAME` set in Giskard's environment to bytes that are not UTF-8, which
    /// a template (and the `String`-valued overlay it feeds) cannot carry.
    EnvNotUtf8(String),
}

impl Unresolvable {
    /// Why the placeholder has no usable value, for messages: "has no value" or "is not valid
    /// UTF-8".
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Var(_) | Self::Env(_) => "has no value",
            Self::VarContainsNul(_) => "contains NUL",
            Self::EnvNotUtf8(_) => "is not valid UTF-8",
        }
    }
}

/// The placeholder as written: `{data_dir}`, `{env:HOME}`.
impl fmt::Display for Unresolvable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(var) | Self::VarContainsNul(var) => write!(formatter, "{{{}}}", var.name()),
            Self::Env(name) | Self::EnvNotUtf8(name) => {
                write!(formatter, "{{{ENV_PREFIX}{name}}}")
            }
        }
    }
}

/// The facts a template resolves against. `None` is a fact this caller does not have.
#[derive(Debug, Clone, Default)]
pub struct EnvTemplateContext<'a> {
    pub project_id: Option<&'a str>,
    pub project_name: Option<&'a str>,
    pub project_dir: Option<&'a str>,
    pub harness_name: Option<&'a str>,
    pub data_dir: Option<&'a str>,
}

impl EnvTemplateContext<'_> {
    fn get(&self, var: TemplateVar) -> Option<&str> {
        match var {
            TemplateVar::ProjectId => self.project_id,
            TemplateVar::ProjectName => self.project_name,
            TemplateVar::ProjectDir => self.project_dir,
            TemplateVar::HarnessName => self.harness_name,
            TemplateVar::DataDir => self.data_dir,
        }
    }
}

/// A variable skipped because its template named a placeholder with no usable value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsetVariable {
    pub name: String,
    pub placeholder: Unresolvable,
}

impl fmt::Display for UnsetVariable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} is skipped: {} {}",
            self.name,
            self.placeholder,
            self.placeholder.reason()
        )
    }
}

/// A declaration's environment after resolution.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct ResolvedEnv {
    /// The variables to set, in declaration order.
    pub entries: Vec<(String, String)>,
    /// The variables skipped, in declaration order.
    pub unset: Vec<UnsetVariable>,
}

impl fmt::Debug for ResolvedEnv {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedEnv")
            .field(
                "names",
                &self
                    .entries
                    .iter()
                    .map(|(name, _)| name)
                    .collect::<Vec<_>>(),
            )
            .field("unset", &self.unset)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> EnvTemplateContext<'static> {
        EnvTemplateContext {
            project_id: Some("01P"),
            project_name: Some("Demo"),
            project_dir: Some("/src/demo"),
            harness_name: Some("claude"),
            data_dir: None,
        }
    }

    fn env(name: &str) -> Result<String, VarError> {
        match name {
            "HOME" => Ok("/home/me".to_owned()),
            "RAW" => Err(VarError::NotUnicode(
                std::os::unix::ffi::OsStringExt::from_vec(vec![0xff]),
            )),
            _ => Err(VarError::NotPresent),
        }
    }

    fn render(source: &str) -> Result<String, Unresolvable> {
        EnvTemplate::parse(source).unwrap().render(&context(), &env)
    }

    #[test]
    fn placeholders_and_escapes_render() {
        assert_eq!(render("plain").unwrap(), "plain");
        assert_eq!(render("").unwrap(), "");
        assert_eq!(
            render("{project_dir}/.cache/{harness_name}").unwrap(),
            "/src/demo/.cache/claude"
        );
        assert_eq!(render("{project_name}:{project_id}").unwrap(), "Demo:01P");
        assert_eq!(render("{env:HOME}/bin").unwrap(), "/home/me/bin");
        assert_eq!(render("{{literal}} }}{{").unwrap(), "{literal} }{");
        assert_eq!(render("é{harness_name}ü").unwrap(), "éclaudeü");
    }

    #[test]
    fn missing_values_are_reported_by_placeholder() {
        assert_eq!(
            render("{data_dir}/x"),
            Err(Unresolvable::Var(TemplateVar::DataDir))
        );
        let missing = render("{env:NOPE}").unwrap_err();
        assert_eq!(missing, Unresolvable::Env("NOPE".into()));
        assert_eq!(missing.to_string(), "{env:NOPE}");
        assert_eq!(missing.reason(), "has no value");
        let nul = EnvTemplate::parse("x{project_name}")
            .unwrap()
            .render(
                &EnvTemplateContext {
                    project_name: Some("a\0b"),
                    ..context()
                },
                &env,
            )
            .unwrap_err();
        assert_eq!(nul, Unresolvable::VarContainsNul(TemplateVar::ProjectName));
        assert_eq!(nul.to_string(), "{project_name}");
        assert_eq!(nul.reason(), "contains NUL");
        let raw = render("{env:RAW}/x").unwrap_err();
        assert_eq!(raw, Unresolvable::EnvNotUtf8("RAW".into()));
        assert_eq!(
            UnsetVariable {
                name: "X".into(),
                placeholder: raw,
            }
            .to_string(),
            "X is skipped: {env:RAW} is not valid UTF-8"
        );
    }

    #[test]
    fn malformed_templates_are_rejected_without_echoing_secrets() {
        assert_eq!(
            EnvTemplate::parse("{projet_dir}"),
            Err(TemplateError::UnknownPlaceholder("projet_dir".into()))
        );
        assert_eq!(
            EnvTemplate::parse("{env:}"),
            Err(TemplateError::EmptyEnvName)
        );
        for source in ["x{env: PATH}", "x{env:PATH }", "x{env:A=B}", "x{env:A\tB}"] {
            let err = EnvTemplate::parse(source).unwrap_err();
            assert_eq!(err, TemplateError::InvalidEnvName(1), "{source:?}");
            assert!(!err.to_string().contains("PATH"), "{err}");
        }
        for (source, offset) in [("a{b", 1), ("a}b", 1), ("{a{b}}", 0)] {
            let err = EnvTemplate::parse(source).unwrap_err();
            assert_eq!(err, TemplateError::Unbalanced(offset), "{source}");
        }
        for source in ["x{s3cr3t-t0k3n}", "x{}"] {
            let err = EnvTemplate::parse(source).unwrap_err();
            assert_eq!(err, TemplateError::NotAPlaceholder(1), "{source}");
            assert!(!err.to_string().contains("s3cr3t"), "{err}");
        }
        let message = EnvTemplate::parse("{nope}").unwrap_err().to_string();
        assert!(message.contains("`{nope}`"), "{message}");
        assert!(message.contains("`{project_dir}`"), "{message}");
        assert!(message.contains("`{env:NAME}`"), "{message}");
        assert!(message.contains("`{{`"), "{message}");
    }

    #[test]
    fn source_round_trips() {
        for source in [
            "",
            "plain",
            "{{x}} }}{{",
            "{project_dir}/.cache/{harness_name}",
            "a{env:HOME}b{{c}}{data_dir}",
        ] {
            let template = EnvTemplate::parse(source).unwrap();
            assert_eq!(template.source(), source);
            assert_eq!(EnvTemplate::parse(&template.source()).unwrap(), template);
        }
    }

    #[test]
    fn debug_names_placeholders_only() {
        let template = EnvTemplate::parse("secret-{harness_name}-{env:HOME}").unwrap();
        let debug = format!("{template:?}");
        assert!(
            debug.contains("{harness_name}") && debug.contains("{env:HOME}"),
            "{debug}"
        );
        assert!(!debug.contains("secret"), "{debug}");
    }
}
