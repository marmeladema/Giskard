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
//!
//! ## Nested code fences
//!
//! Agents routinely put a fenced block inside another one with the *same* fence length — a
//! ```` ```markdown ```` plan quoting a ```` ```rust ```` snippet. CommonMark closes the outer block
//! at the inner snippet's bare closing fence, which splits the plan in two. Before parsing,
//! [`balance_nested_fences`] pairs fences by depth instead: inside a block, a fence line carrying
//! an info string opens a nested block, and a bare fence closes the innermost open one. When that
//! reading differs from CommonMark's, the outer opening and closing fences are lengthened so the
//! document means the same thing to a conforming parser; the rewrite is kept only if re-parsing
//! confirms it produced exactly that block. A bare inner opener (```` ``` ```` with no language)
//! stays ambiguous and follows CommonMark.

use std::borrow::Cow;
use std::ops::Range;
use std::path::Path;

use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag};

use crate::highlight::highlight_snippet;
use crate::linkify::linkify_text;

/// Render agent Markdown `text` to sanitized HTML, wrapping workspace paths (resolved against
/// `workspace_root`) in `.path-link` buttons.
pub fn render_markdown(text: &str, workspace_root: &Path) -> String {
    let text = balance_nested_fences(text);
    let text = text.as_ref();
    let parser = Parser::new_ext(text, PARSE_OPTIONS);

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

const PARSE_OPTIONS: Options = Options::ENABLE_TABLES
    .union(Options::ENABLE_STRIKETHROUGH)
    .union(Options::ENABLE_TASKLISTS);

/// Upper bound on nested-fence rewrites attempted per message. Each attempt re-parses the message,
/// so the cap keeps a pathological input linear; blocks past it render as plain CommonMark.
const MAX_FENCE_REWRITES: usize = 64;

/// Rewrite same-length nested code fences so CommonMark pairs them by depth (see the module docs).
/// Returns the input unchanged when no block needs it.
fn balance_nested_fences(input: &str) -> Cow<'_, str> {
    let mut text = Cow::Borrowed(input);
    // Byte offset in `text` before which every block is settled; it only moves forward.
    let mut resume = 0;
    let mut attempts = 0;
    while let Some(nested) = find_nested_fence(&text, resume) {
        if attempts == MAX_FENCE_REWRITES {
            tracing::debug!(
                max_rewrites = MAX_FENCE_REWRITES,
                "nested code fence rewrite limit reached; remaining blocks keep CommonMark pairing"
            );
            break;
        }
        attempts += 1;
        let rewritten = widen_fences(&text, &nested);
        let delta = nested.new_len - nested.open_len;
        let closer = nested.closer.start + delta..nested.closer.end + 2 * delta;
        if fenced_block_ends_at(&rewritten, nested.opener_start, &closer) {
            text = Cow::Owned(rewritten);
            resume = closer.end;
        } else {
            // Typically a container (list item, block quote) ends before the depth-matched closer,
            // so lengthening would leave a runaway fence. Keep CommonMark's reading of this block.
            tracing::debug!(
                opener_offset = nested.opener_start,
                "nested code fence rewrite rejected by re-parse; keeping CommonMark pairing"
            );
            resume = nested.commonmark_end;
        }
    }
    text
}

/// A fenced block whose depth-matched closer differs from the one CommonMark chose.
struct NestedFence {
    /// Byte offset of the opening fence run.
    opener_start: usize,
    open_len: usize,
    /// Byte range of the closing fence run.
    closer: Range<usize>,
    /// Fence length that exceeds every same-character fence line inside the block.
    new_len: usize,
    /// Where CommonMark ended the block, for skipping past it when the rewrite is rejected.
    commonmark_end: usize,
}

/// Find the first fenced code block starting at or after `resume` whose content holds an
/// info-string fence opener, and locate its depth-matched closer.
fn find_nested_fence(text: &str, resume: usize) -> Option<NestedFence> {
    let blocks = Parser::new_ext(text, PARSE_OPTIONS)
        .into_offset_iter()
        .filter_map(|(event, range)| match event {
            Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(_))) if range.start >= resume => {
                Some(range)
            }
            _ => None,
        });
    blocks
        .into_iter()
        .find_map(|block| nested_fence_at(text, block))
}

fn nested_fence_at(text: &str, block: Range<usize>) -> Option<NestedFence> {
    let line_start = text[..block.start].rfind('\n').map_or(0, |i| i + 1);
    let opener_line_end = text[line_start..]
        .find('\n')
        .map_or(text.len(), |i| line_start + i);
    // Only blocks whose fence begins its line (after indentation) are rewritten; a fence behind a
    // block-quote or list marker keeps CommonMark pairing.
    let open = fence_line(&text[line_start..opener_line_end])?;
    let opener_start = line_start + open.offset;

    let mut depth = 0usize;
    let mut nested = false;
    let mut widest = open.len;
    let mut pos = opener_line_end;
    while pos < text.len() {
        let start = pos + 1;
        let end = text[start..].find('\n').map_or(text.len(), |i| start + i);
        pos = end;
        let Some(line) = fence_line(&text[start..end]) else {
            continue;
        };
        // A shorter run, or the other fence character, can neither open nor close at this depth.
        if line.ch != open.ch || line.len < open.len {
            continue;
        }
        widest = widest.max(line.len);
        if line.has_info {
            depth += 1;
            nested = true;
        } else if depth > 0 {
            depth -= 1;
        } else if line.indent <= 3 {
            if !nested {
                // CommonMark already pairs this block the same way.
                return None;
            }
            let run = start + line.offset;
            return Some(NestedFence {
                opener_start,
                open_len: open.len,
                closer: run..run + line.len,
                new_len: widest + 1,
                commonmark_end: block.end,
            });
        }
    }
    None
}

/// Lengthen the opening and closing fence runs of `nested` to `new_len`.
fn widen_fences(text: &str, nested: &NestedFence) -> String {
    let extra = &text[nested.opener_start..nested.opener_start + 1]
        .repeat(nested.new_len - nested.open_len);
    let mut out = String::with_capacity(text.len() + 2 * extra.len());
    out.push_str(&text[..nested.opener_start]);
    out.push_str(extra);
    out.push_str(&text[nested.opener_start..nested.closer.start]);
    out.push_str(extra);
    out.push_str(&text[nested.closer.start..]);
    out
}

/// Whether `text` parses to a fenced code block opening at `opener_start` whose closing fence is
/// the run at `closer`.
fn fenced_block_ends_at(text: &str, opener_start: usize, closer: &Range<usize>) -> bool {
    Parser::new_ext(text, PARSE_OPTIONS)
        .into_offset_iter()
        .find_map(|(event, range)| match event {
            Event::Start(Tag::CodeBlock(CodeBlockKind::Fenced(_)))
                if range.contains(&opener_start) =>
            {
                Some(range)
            }
            _ => None,
        })
        .is_some_and(|range| {
            range.end >= closer.end
                && text
                    .get(closer.end..range.end)
                    .is_some_and(|tail| tail.trim().is_empty())
        })
}

/// A line that has the shape of a code fence.
struct FenceLine {
    /// Indentation in columns (tabs advance to the next multiple of four).
    indent: usize,
    /// Byte offset of the fence run within the line.
    offset: usize,
    ch: char,
    len: usize,
    has_info: bool,
}

fn fence_line(line: &str) -> Option<FenceLine> {
    let mut indent = 0;
    let mut offset = 0;
    for c in line.chars() {
        match c {
            ' ' => indent += 1,
            '\t' => indent += 4 - indent % 4,
            _ => break,
        }
        offset += 1;
    }
    let rest = &line[offset..];
    let ch = rest.chars().next().filter(|c| *c == '`' || *c == '~')?;
    let len = rest.chars().take_while(|c| *c == ch).count();
    if len < 3 {
        return None;
    }
    let info = rest[len..].trim();
    // A backtick fence's info string cannot contain a backtick (that line is inline code).
    if ch == '`' && info.contains('`') {
        return None;
    }
    Some(FenceLine {
        indent,
        offset,
        ch,
        len,
        has_info: !info.is_empty(),
    })
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

    fn code_block_count(html: &str) -> usize {
        html.matches("<div class=\"code-block\">").count()
    }

    #[test]
    fn nested_fence_with_language_stays_inside_outer_block() {
        let text =
            "Plan:\n\n```markdown\n# Step 1\n\n```rust\nfn main() {}\n```\n\nDone.\n```\n\nAfter.";
        let html = render(text);
        assert_eq!(code_block_count(&html), 1, "{html}");
        assert!(html.contains("<code class=\"language-markdown\""));
        // The inner fences are content, verbatim, and the prose after the outer block is prose.
        let code_end = html.find("</code>").unwrap();
        assert!(html.find("fn </span>").unwrap() < code_end, "{html}");
        assert!(html.find("Done.").unwrap() < code_end, "{html}");
        assert!(html.ends_with("<p>After.</p>"), "{html}");
    }

    #[test]
    fn nested_fences_pair_by_depth() {
        let text =
            "```markdown\n```bash\nls\n```\n~~~\n```json\n````yaml\na: 1\n````\n```\n```\ntail";
        let balanced = balance_nested_fences(text);
        let html = render(text);
        assert_eq!(code_block_count(&html), 1, "{balanced}\n{html}");
        assert!(html.ends_with("<p>tail</p>"), "{html}");
        // The inner lines are untouched; only the outer fences grew past the widest inner run.
        assert_eq!(
            balanced,
            "`````markdown\n```bash\nls\n```\n~~~\n```json\n````yaml\na: 1\n````\n```\n`````\ntail"
        );
    }

    #[test]
    fn nested_tilde_fences_are_balanced() {
        let html = render("~~~md\n~~~rust\nx\n~~~\n~~~\nafter");
        assert_eq!(code_block_count(&html), 1, "{html}");
        assert!(html.ends_with("<p>after</p>"), "{html}");
    }

    #[test]
    fn nested_fence_in_list_item_is_balanced() {
        let text = "1. Write the plan:\n   ```markdown\n   ```rust\n   x\n   ```\n   ```\n2. Next";
        let html = render(text);
        assert_eq!(code_block_count(&html), 1, "{html}");
        assert!(html.contains("<li>Next</li>"), "{html}");
    }

    #[test]
    fn sequential_and_already_longer_fences_are_left_alone() {
        for text in [
            "```rust\na\n```\n\n```python\nb\n```\n",
            "````markdown\n```rust\nx\n```\n````\n",
            "~~~markdown\n```rust\nx\n```\n~~~\n",
            "```\nplain\n```\n",
        ] {
            assert!(
                matches!(balance_nested_fences(text), Cow::Borrowed(_)),
                "{text}"
            );
        }
    }

    #[test]
    fn unbalanced_nested_fence_keeps_commonmark_pairing() {
        // Nothing closes the outer block at depth zero (e.g. still streaming), so the inner bare
        // fence keeps closing it as CommonMark says.
        let text = "```markdown\n```rust\nx\n```\nmore";
        assert!(matches!(balance_nested_fences(text), Cow::Borrowed(_)));
        let html = render(text);
        assert_eq!(code_block_count(&html), 1, "{html}");
        assert!(html.ends_with("<p>more</p>"), "{html}");
    }

    #[test]
    fn nested_fence_behind_a_container_marker_is_left_alone() {
        for text in [
            "> ```markdown\n> ```rust\n> x\n> ```\n> ```\n",
            "- ```markdown\n  ```rust\n  x\n  ```\n  ```\n",
        ] {
            assert!(
                matches!(balance_nested_fences(text), Cow::Borrowed(_)),
                "{text}"
            );
        }
    }

    #[test]
    fn nested_fence_rewrite_crossing_a_container_is_rejected() {
        // The opener sits in a list item but the depth-matched closer is past the item's end, so
        // lengthening would leave a runaway fence at the top level. The re-parse check rejects
        // the rewrite and the block keeps CommonMark pairing.
        let text = "- item\n  ```markdown\n  ```rust\n  x\n  ```\n\n```\nafter\n```\n";
        let nested = find_nested_fence(text, 0).expect("depth scan finds a candidate");
        assert!(!fenced_block_ends_at(
            &widen_fences(text, &nested),
            nested.opener_start,
            &(nested.closer.start + 1..nested.closer.end + 2)
        ));
        assert!(matches!(balance_nested_fences(text), Cow::Borrowed(_)));
    }

    #[test]
    fn nested_fence_rewrites_stop_at_the_cap() {
        let block = "```md\n```sh\nx\n```\n```\n\n";
        let text = block.repeat(MAX_FENCE_REWRITES + 1);
        let balanced = balance_nested_fences(&text);
        assert_eq!(balanced.matches("````md").count(), MAX_FENCE_REWRITES);
        // The block past the cap keeps CommonMark pairing: its inner bare fence closes it.
        assert!(balanced.ends_with(block));
    }

    #[test]
    fn nested_fence_rewrites_every_block_in_a_message() {
        let block = "```md\n```sh\nx\n```\n```\n\n";
        let html = render(&block.repeat(3));
        assert_eq!(code_block_count(&html), 3, "{html}");
    }
}
