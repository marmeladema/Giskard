//! The stdin `user` message, with user attachments as inline content blocks (plan §3.6).

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use giskard_core::error::HarnessError;
use giskard_core::user_input::UserAttachment;
use serde_json::{Value, json};
use tracing::debug;

/// The CLI's own cap on one stdin line, attachments included.
pub(crate) const MAX_STDIN_LINE_BYTES: usize = 10 * 1024 * 1024;

const IMAGE_TYPES: &[&str] = &["image/png", "image/jpeg", "image/gif", "image/webp"];

/// Text under a non-`text/*` label.
const TEXT_APPLICATION_TYPES: &[&str] = &[
    "application/json",
    "application/xml",
    "application/x-yaml",
    "application/toml",
    "application/javascript",
];

/// Build the stdin line for one user message: attachment blocks first, then the text block.
///
/// Nothing is truncated: a message whose encoded line exceeds the CLI's cap is refused by size.
pub(crate) fn user_message_line(
    text: &str,
    attachments: &[UserAttachment],
) -> Result<String, HarnessError> {
    if text.is_empty() && attachments.is_empty() {
        return Err(HarnessError::Protocol("empty user message".into()));
    }
    let mut content = Vec::with_capacity(attachments.len() + 1);
    for attachment in attachments {
        content.push(attachment_block(attachment)?);
    }
    // An empty text with attachments sends the attachments alone.
    if !text.is_empty() {
        content.push(json!({"type": "text", "text": text}));
    }
    let message = json!({
        "type": "user",
        "message": {"role": "user", "content": content},
    });
    let line = serde_json::to_string(&message)
        .map_err(|error| HarnessError::Protocol(format!("cannot encode user message: {error}")))?;
    if line.len() > MAX_STDIN_LINE_BYTES {
        return Err(HarnessError::Protocol(format!(
            "message is {} bytes encoded; Claude Code accepts at most 10 MiB per message, \
             attachments included",
            line.len()
        )));
    }
    Ok(line)
}

fn attachment_block(attachment: &UserAttachment) -> Result<Value, HarnessError> {
    let mime = attachment.mime_type.trim().to_ascii_lowercase();
    debug!(
        action = "attachment",
        kind = ?attachment.kind,
        mime_type = %mime,
        size = attachment.size,
        "encoding a user attachment"
    );
    // The API rejects wrapped base64.
    let data: String = attachment
        .data_base64
        .chars()
        .filter(|character| !character.is_ascii_whitespace())
        .collect();
    let decoded = BASE64_STANDARD.decode(data.as_bytes()).map_err(|error| {
        HarnessError::Protocol(format!(
            "attachment {:?} is not valid base64: {error}",
            attachment.name
        ))
    })?;
    if IMAGE_TYPES.contains(&mime.as_str()) {
        return Ok(json!({
            "type": "image",
            "source": {"type": "base64", "media_type": mime, "data": data},
        }));
    }
    if mime == "application/pdf" {
        return Ok(json!({
            "type": "document",
            "source": {"type": "base64", "media_type": "application/pdf", "data": data},
        }));
    }
    if mime.starts_with("text/") || TEXT_APPLICATION_TYPES.contains(&mime.as_str()) {
        let text = String::from_utf8(decoded).map_err(|_| {
            HarnessError::Unsupported(format!(
                "attachment {:?} ({mime}) is not UTF-8 text; convert it to UTF-8 or PDF",
                attachment.name
            ))
        })?;
        return Ok(json!({
            "type": "document",
            "source": {"type": "text", "media_type": "text/plain", "data": text},
        }));
    }
    Err(HarnessError::Unsupported(format!(
        "attachment {:?} ({}) is not supported by Claude Code; convert it to text or PDF",
        attachment.name, attachment.mime_type
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use giskard_core::user_input::AttachmentKind;

    fn attachment(name: &str, mime: &str, bytes: &[u8]) -> UserAttachment {
        UserAttachment {
            name: name.into(),
            mime_type: mime.into(),
            size: bytes.len() as u64,
            kind: if mime.starts_with("image/") {
                AttachmentKind::Image
            } else {
                AttachmentKind::File
            },
            data_base64: BASE64_STANDARD.encode(bytes),
        }
    }

    fn content(line: &str) -> Vec<Value> {
        let value: Value = serde_json::from_str(line).unwrap();
        assert_eq!(value["type"], "user");
        assert_eq!(value["message"]["role"], "user");
        value["message"]["content"].as_array().unwrap().clone()
    }

    #[test]
    fn attachments() {
        // A text-only message is one text block.
        let blocks = content(&user_message_line("ping", &[]).unwrap());
        assert_eq!(blocks, vec![json!({"type": "text", "text": "ping"})]);

        // An image precedes the text.
        let png = attachment("shot.png", "image/png", b"\x89PNG\r\n");
        let blocks = content(&user_message_line("look", std::slice::from_ref(&png)).unwrap());
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0]["type"], "image");
        assert_eq!(blocks[0]["source"]["type"], "base64");
        assert_eq!(blocks[0]["source"]["media_type"], "image/png");
        assert_eq!(blocks[0]["source"]["data"], png.data_base64);
        assert_eq!(blocks[1]["type"], "text");

        // A PDF is a base64 document.
        let pdf = attachment("doc.pdf", "application/pdf", b"%PDF-1.7");
        let blocks = content(&user_message_line("read", &[pdf]).unwrap());
        assert_eq!(blocks[0]["type"], "document");
        assert_eq!(blocks[0]["source"]["media_type"], "application/pdf");

        // Text is decoded into a text-source document.
        let notes = attachment("notes.txt", "text/plain", "héllo".as_bytes());
        let blocks = content(&user_message_line("", &[notes]).unwrap());
        assert_eq!(blocks.len(), 1, "an empty text sends the attachment alone");
        assert_eq!(
            blocks[0],
            json!({"type": "document", "source": {"type": "text", "media_type": "text/plain", "data": "héllo"}})
        );
        let json_file = attachment("a.json", "application/json", b"{}");
        let blocks = content(&user_message_line("x", &[json_file]).unwrap());
        assert_eq!(blocks[0]["source"]["data"], "{}");

        // Wrapped base64 is unwrapped.
        let mut wrapped = attachment("shot.png", "image/png", &[7u8; 120]);
        let unwrapped = wrapped.data_base64.clone();
        wrapped.data_base64 = format!("{}\n{}\r\n", &unwrapped[..60], &unwrapped[60..]);
        let blocks = content(&user_message_line("x", &[wrapped]).unwrap());
        assert_eq!(blocks[0]["source"]["data"], unwrapped);

        // An unsupported type is refused by name.
        let xlsx = attachment(
            "sheet.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            b"PK",
        );
        let error = user_message_line("x", &[xlsx]).unwrap_err();
        assert!(
            matches!(&error, HarnessError::Unsupported(message) if message.contains("sheet.xlsx")),
            "{error}"
        );

        // Invalid base64 and non-UTF-8 text are refused by name.
        let mut broken = attachment("b.png", "image/png", b"x");
        broken.data_base64 = "@@@".into();
        assert!(matches!(
            user_message_line("x", &[broken]),
            Err(HarnessError::Protocol(message)) if message.contains("b.png")
        ));
        let latin1 = attachment("l.txt", "text/plain", &[0xe9, 0xff]);
        assert!(matches!(
            user_message_line("x", &[latin1]),
            Err(HarnessError::Unsupported(message)) if message.contains("l.txt")
        ));

        // A message over the cap is refused by size, never truncated.
        let big = attachment("big.pdf", "application/pdf", &vec![0u8; 8 * 1024 * 1024]);
        let error = user_message_line("x", &[big]).unwrap_err();
        assert!(
            matches!(&error, HarnessError::Protocol(message) if message.contains("bytes encoded") && message.contains("10 MiB")),
            "{error}"
        );

        // Nothing at all is not a message.
        assert!(matches!(
            user_message_line("", &[]),
            Err(HarnessError::Protocol(message)) if message == "empty user message"
        ));
    }
}
