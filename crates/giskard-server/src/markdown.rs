//! Server-side Markdown rendering for agent messages (spec §11.2).
//!
//! Agents emit GitHub-flavored Markdown; the browser shows it rendered. Rendering happens here,
//! in Rust, for two reasons: it keeps the security-sensitive HTML generation off the client, and
//! it lets us reuse the same path [`linkify_text`](crate::linkify::linkify_text) pass that already
//! powers clickable file links.
//!
//! ## Safety
//!
//! The output is treated as trusted HTML by the client (`innerHTML`), so this module must never
//! emit anything the agent could weaponize:
//!
//! - every text run is HTML-escaped;
//! - raw HTML in the source (`Event::Html` / `Event::InlineHtml`) is **escaped to inert text**,
//!   never passed through;
//! - link/image URLs are checked against a scheme allowlist (`http`/`https`/`mailto`); anything
//!   else renders as plain text with no `href`;
//! - detected workspace paths become `<button class="path-link">` elements — the same affordance
//!   the client already wires up — instead of navigable links.
//!
//! Rendering never interprets Markdown inside code spans or fenced code blocks (no linkify, no
//! emphasis): code is shown verbatim.

use std::path::Path;

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag};

use crate::highlight::highlight_snippet;
use crate::linkify::linkify_text;

/// Render agent Markdown `text` to sanitized HTML, wrapping workspace paths (resolved against
/// `workspace_root`) in `.path-link` buttons.
pub fn render_markdown(text: &str, workspace_root: &Path) -> String {
    let options =
        Options::ENABLE_TABLES | Options::ENABLE_STRIKETHROUGH | Options::ENABLE_TASKLISTS;
    let parser = Parser::new_ext(text, options);

    let mut out = String::with_capacity(text.len() + text.len() / 2);
    // One frame per open tag; every `Event::End` pops exactly one. Keeping the close string on the
    // stack means we never have to interpret the (version-sensitive) payload of `TagEnd`, so the
    // output stays balanced regardless of which tags we special-case.
    let mut stack: Vec<Frame> = Vec::new();
    let mut in_table_head = false;
    let mut code_block: Option<ActiveCodeBlock> = None;

    for event in parser {
        match event {
            Event::Start(tag) => {
                if let Tag::CodeBlock(kind) = tag {
                    code_block = Some(ActiveCodeBlock::new(kind));
                    continue;
                }
                let frame = open_tag(&mut out, tag, in_table_head);
                if frame.opens_table_head {
                    in_table_head = true;
                }
                stack.push(frame);
            }
            Event::End(_) => {
                if let Some(block) = code_block.take() {
                    push_code_block(&mut out, block);
                    continue;
                }
                if let Some(frame) = stack.pop() {
                    out.push_str(&frame.close);
                    if frame.closes_table_head {
                        in_table_head = false;
                    }
                }
            }
            Event::Text(t) => {
                if let Some(block) = &mut code_block {
                    block.source.push_str(&t);
                } else {
                    push_linkified(&mut out, &t, workspace_root);
                }
            }
            // Inline code is literal: escape, never linkify.
            Event::Code(t) => {
                if let Some(block) = &mut code_block {
                    block.source.push_str(&t);
                } else {
                    out.push_str("<code>");
                    push_escaped(&mut out, &t);
                    out.push_str("</code>");
                }
            }
            // Raw HTML is rendered as inert, escaped text — never passed through. Math (only
            // emitted with `ENABLE_MATH`, which we do not set) is likewise shown verbatim.
            Event::Html(t)
            | Event::InlineHtml(t)
            | Event::InlineMath(t)
            | Event::DisplayMath(t) => {
                if let Some(block) = &mut code_block {
                    block.source.push_str(&t);
                } else {
                    push_escaped(&mut out, &t);
                }
            }
            Event::SoftBreak => {
                if let Some(block) = &mut code_block {
                    block.source.push('\n');
                } else {
                    out.push('\n');
                }
            }
            Event::HardBreak => {
                if let Some(block) = &mut code_block {
                    block.source.push('\n');
                } else {
                    out.push_str("<br>");
                }
            }
            Event::Rule => out.push_str("<hr>"),
            Event::TaskListMarker(checked) => {
                out.push_str(if checked {
                    "<input type=\"checkbox\" checked disabled> "
                } else {
                    "<input type=\"checkbox\" disabled> "
                });
            }
            // Footnotes are not enabled; ignore any stray references.
            Event::FootnoteReference(_) => {}
        }
    }

    // Defensively render anything left open (malformed/truncated input).
    if let Some(block) = code_block.take() {
        push_code_block(&mut out, block);
    }

    // Defensively close anything left open (malformed/truncated input).
    while let Some(frame) = stack.pop() {
        out.push_str(&frame.close);
    }

    out
}

struct Frame {
    close: String,
    opens_table_head: bool,
    closes_table_head: bool,
}

impl Frame {
    fn new(close: impl Into<String>) -> Self {
        Self {
            close: close.into(),
            opens_table_head: false,
            closes_table_head: false,
        }
    }
}

fn open_tag(out: &mut String, tag: Tag, in_table_head: bool) -> Frame {
    match tag {
        Tag::Paragraph => {
            out.push_str("<p>");
            Frame::new("</p>")
        }
        Tag::Heading { level, .. } => {
            let n = heading_number(level);
            out.push_str(&format!("<h{n}>"));
            Frame::new(format!("</h{n}>"))
        }
        Tag::BlockQuote(_) => {
            out.push_str("<blockquote>");
            Frame::new("</blockquote>")
        }
        Tag::List(Some(start)) => {
            if start == 1 {
                out.push_str("<ol>");
            } else {
                out.push_str(&format!("<ol start=\"{start}\">"));
            }
            Frame::new("</ol>")
        }
        Tag::List(None) => {
            out.push_str("<ul>");
            Frame::new("</ul>")
        }
        Tag::Item => {
            out.push_str("<li>");
            Frame::new("</li>")
        }
        Tag::Emphasis => {
            out.push_str("<em>");
            Frame::new("</em>")
        }
        Tag::Strong => {
            out.push_str("<strong>");
            Frame::new("</strong>")
        }
        Tag::Strikethrough => {
            out.push_str("<del>");
            Frame::new("</del>")
        }
        Tag::Link { dest_url, .. } => match safe_href(&dest_url) {
            Some(href) => {
                out.push_str(&format!(
                    "<a href=\"{}\" target=\"_blank\" rel=\"noopener noreferrer\">",
                    escape_attr(&href)
                ));
                Frame::new("</a>")
            }
            // Disallowed scheme: render the link text as plain inline content.
            None => Frame::new(""),
        },
        // Images are not fetched (they would defeat the point of rendering server-side and open a
        // request-forgery surface); render the alt text inline instead.
        Tag::Image { .. } => Frame::new(""),
        Tag::Table(_) => {
            out.push_str("<table>");
            Frame::new("</tbody></table>")
        }
        Tag::TableHead => {
            out.push_str("<thead><tr>");
            let mut frame = Frame::new("</tr></thead><tbody>");
            frame.opens_table_head = true;
            frame.closes_table_head = true;
            frame
        }
        Tag::TableRow => {
            out.push_str("<tr>");
            Frame::new("</tr>")
        }
        Tag::TableCell => {
            if in_table_head {
                out.push_str("<th>");
                Frame::new("</th>")
            } else {
                out.push_str("<td>");
                Frame::new("</td>")
            }
        }
        // Anything not handled above (e.g. footnote definitions, metadata blocks) contributes no
        // wrapper; its inner text still renders. The empty close keeps the stack balanced.
        _ => Frame::new(""),
    }
}

struct ActiveCodeBlock {
    language_token: Option<String>,
    source: String,
}

impl ActiveCodeBlock {
    fn new(kind: CodeBlockKind) -> Self {
        Self {
            language_token: language_token(&kind),
            source: String::new(),
        }
    }
}

fn push_code_block(out: &mut String, block: ActiveCodeBlock) {
    let highlighted = highlight_snippet(&block.source, block.language_token.as_deref());
    let class = block
        .language_token
        .as_deref()
        .map(|lang| format!(" class=\"language-{}\"", escape_attr(lang)))
        .unwrap_or_default();

    out.push_str("<div class=\"code-block\">");
    out.push_str("<div class=\"code-block-head\"><span>");
    push_escaped(out, &highlighted.language);
    out.push_str("</span></div>");
    out.push_str("<pre><code");
    out.push_str(&class);
    if highlighted.recognized_language {
        out.push_str(" data-highlighted=\"true\"");
    }
    out.push('>');
    out.push_str(&highlighted.html);
    out.push_str("</code></pre></div>");
}

fn heading_number(level: HeadingLevel) -> u8 {
    match level {
        HeadingLevel::H1 => 1,
        HeadingLevel::H2 => 2,
        HeadingLevel::H3 => 3,
        HeadingLevel::H4 => 4,
        HeadingLevel::H5 => 5,
        HeadingLevel::H6 => 6,
    }
}

/// Extract a safe language token for a fenced code block.
fn language_token(kind: &CodeBlockKind) -> Option<String> {
    let CodeBlockKind::Fenced(info) = kind else {
        return None;
    };
    let lang: String = info
        .split_whitespace()
        .next()
        .unwrap_or("")
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-' || *c == '+')
        .collect();
    if lang.is_empty() { None } else { Some(lang) }
}

/// Allow only schemes that cannot execute script or exfiltrate via navigation.
fn safe_href(url: &str) -> Option<String> {
    let trimmed = url.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("http://") || lower.starts_with("https://") || lower.starts_with("mailto:")
    {
        Some(trimmed.to_string())
    } else {
        None
    }
}

/// Escape a text run and wrap any resolved workspace paths in `.path-link` buttons.
fn push_linkified(out: &mut String, text: &str, workspace_root: &Path) {
    let spans = linkify_text(text, workspace_root);
    let mut pos = 0;
    for span in spans {
        // `linkify_text` yields spans in order; skip any that overlap what we already emitted.
        if span.start < pos || span.end > text.len() || span.start > span.end {
            continue;
        }
        push_escaped(out, &text[pos..span.start]);
        out.push_str("<button type=\"button\" class=\"path-link\" data-path=\"");
        out.push_str(&escape_attr(&span.path));
        out.push('"');
        if let Some(line) = span.line {
            out.push_str(&format!(" data-line=\"{line}\""));
        }
        out.push('>');
        push_escaped(out, &text[span.start..span.end]);
        out.push_str("</button>");
        pos = span.end;
    }
    push_escaped(out, &text[pos..]);
}

fn push_escaped(out: &mut String, s: &str) {
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
}

fn escape_attr(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    push_escaped(&mut out, s);
    out
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn render(text: &str) -> String {
        render_markdown(text, Path::new("/nonexistent-root"))
    }

    #[test]
    fn emphasis_and_inline_code_render() {
        assert_eq!(
            render("This is **bold** and *italic* and `code`."),
            "<p>This is <strong>bold</strong> and <em>italic</em> and <code>code</code>.</p>"
        );
    }

    #[test]
    fn headings_and_lists_render() {
        let html = render("# Title\n\n- one\n- two\n");
        assert!(html.contains("<h1>Title</h1>"));
        assert!(html.contains("<ul><li>one</li><li>two</li></ul>"));
    }

    #[test]
    fn ordered_list_start_is_preserved() {
        let html = render("3. three\n4. four\n");
        assert!(html.contains("<ol start=\"3\">"));
    }

    #[test]
    fn fenced_code_block_keeps_language_and_escapes() {
        let html = render("```rust\nlet x = &y < z;\n```");
        assert!(html.contains("<div class=\"code-block\">"));
        assert!(html.contains("<div class=\"code-block-head\"><span>Rust</span></div>"));
        assert!(html.contains("<code class=\"language-rust\" data-highlighted=\"true\">"));
        assert!(html.contains("&lt;"));
        assert!(!html.contains("< z"));
    }

    #[test]
    fn fenced_code_block_with_unknown_language_falls_back_safely() {
        let html = render("```no-such-language\n<&>\n```");
        assert!(
            html.contains("<div class=\"code-block-head\"><span>no-such-language</span></div>")
        );
        assert!(html.contains("<code class=\"language-no-such-language\">"));
        assert!(html.contains("&lt;&amp;&gt;"));
        assert!(!html.contains("data-highlighted=\"true\""));
    }

    #[test]
    fn code_block_without_language_gets_plain_text_label() {
        let html = render("```\nplain text\n```");
        assert!(html.contains("<div class=\"code-block-head\"><span>Plain Text</span></div>"));
        assert!(html.contains("<code>"));
        assert!(html.contains("plain text"));
    }

    #[test]
    fn raw_html_is_escaped_not_passed_through() {
        let html = render("<img src=x onerror=alert(1)> plain");
        assert!(!html.contains("<img"));
        assert!(html.contains("&lt;img src=x onerror=alert(1)&gt;"));
    }

    #[test]
    fn javascript_link_scheme_is_dropped() {
        let html = render("[click](javascript:alert(1))");
        assert!(!html.contains("href"));
        assert!(html.contains("click"));
    }

    #[test]
    fn http_link_is_allowed_and_escaped() {
        let html = render("[docs](https://example.com/a?b=1&c=2)");
        assert!(html.contains("<a href=\"https://example.com/a?b=1&amp;c=2\""));
        assert!(html.contains("target=\"_blank\""));
        assert!(html.contains("rel=\"noopener noreferrer\""));
    }

    #[test]
    fn table_renders_head_and_body() {
        let html = render("| a | b |\n| - | - |\n| 1 | 2 |\n");
        assert!(html.contains("<table><thead><tr><th>a</th><th>b</th></tr></thead><tbody>"));
        assert!(html.contains("<tr><td>1</td><td>2</td></tr></tbody></table>"));
    }

    #[test]
    fn existing_path_is_linkified_but_not_inside_code() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        fs::write(root.join("main.rs"), "fn main() {}").unwrap();

        let html = render_markdown("see main.rs:12 and `main.rs`", &root);
        assert!(html.contains(
            "<button type=\"button\" class=\"path-link\" data-path=\"main.rs\" data-line=\"12\">main.rs:12</button>"
        ));
        // The occurrence inside inline code stays literal.
        assert!(html.contains("<code>main.rs</code>"));
    }

    #[test]
    fn strikethrough_and_task_list_render() {
        assert!(render("~~gone~~").contains("<del>gone</del>"));
        let tasks = render("- [x] done\n- [ ] todo\n");
        assert!(tasks.contains("<input type=\"checkbox\" checked disabled>"));
        assert!(tasks.contains("<input type=\"checkbox\" disabled>"));
    }

    #[test]
    fn plain_text_has_no_stray_markup() {
        assert_eq!(render("just text"), "<p>just text</p>");
    }
}
