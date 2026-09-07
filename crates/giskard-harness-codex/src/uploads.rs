use super::*;
use crate::rpc::codex_request;

const CODEX_UPLOAD_DIR_NAME: &str = "giskard-codex-uploads";

pub(crate) struct PreparedUserInput {
    pub(crate) input: UserInput,
    pub(crate) upload_dir: Option<PathBuf>,
}

pub(crate) async fn prepare_user_input_for_codex_uploads(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    input: &UserInput,
) -> Result<PreparedUserInput, HarnessError> {
    let UserInput::Text { text, attachments } = input;
    if attachments.is_empty() {
        return Ok(PreparedUserInput {
            input: input.clone(),
            upload_dir: None,
        });
    }

    let mut prepared_text = text.clone();
    let mut image_attachments = Vec::new();
    let mut uploaded_files = Vec::new();
    let upload_dir = codex_upload_dir(thread);
    let mut ensured_upload_dir = false;

    let upload_result: Result<(), HarnessError> = async {
        for (index, attachment) in attachments.iter().enumerate() {
            match attachment.kind {
                AttachmentKind::Image => image_attachments.push(attachment.clone()),
                AttachmentKind::File => {
                    if !ensured_upload_dir {
                        let params = codex_codes::FsCreateDirectoryParams {
                            path: serde_json::json!(upload_dir.to_string_lossy()),
                            recursive: Some(true),
                        };
                        let _: codex_codes::FsCreateDirectoryResponse = codex_request(
                            client,
                            CodexOperationContext::for_thread("upload_attachment_mkdir", thread),
                            codex_codes::protocol::methods::FS_CREATEDIRECTORY,
                            &params,
                        )
                        .await?;
                        ensured_upload_dir = true;
                    }
                    let path = codex_upload_path(&upload_dir, index, attachment);
                    let path_string = path.to_string_lossy().to_string();
                    let params = codex_codes::FsWriteFileParams {
                        data_base64: attachment.data_base64.clone(),
                        path: serde_json::json!(path_string),
                    };
                    let _: codex_codes::FsWriteFileResponse = codex_request(
                        client,
                        CodexOperationContext::for_thread("upload_attachment_write", thread),
                        codex_codes::protocol::methods::FS_WRITEFILE,
                        &params,
                    )
                    .await?;
                    uploaded_files.push((
                        safe_upload_file_name(&attachment.name),
                        path.to_string_lossy().to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
    .await;
    if let Err(error) = upload_result {
        cleanup_codex_upload_dir(client, thread, Some(&upload_dir)).await;
        return Err(error);
    }

    if !uploaded_files.is_empty() {
        if !prepared_text.trim().is_empty() {
            prepared_text.push_str("\n\n");
        }
        prepared_text.push_str("Attached files available on the harness host:\n");
        for (name, path) in uploaded_files {
            prepared_text.push_str("- ");
            prepared_text.push_str(&name);
            prepared_text.push_str(": ");
            prepared_text.push_str(&path);
            prepared_text.push('\n');
        }
    }

    Ok(PreparedUserInput {
        input: UserInput::text_with_attachments(prepared_text, image_attachments),
        upload_dir: ensured_upload_dir.then_some(upload_dir),
    })
}

pub(crate) async fn cleanup_active_turn_upload(
    client: &mut dyn CodexTransport,
    active_turns: &mut ActiveTurns,
    thread_id: ThreadId,
) {
    let Some(active) = active_turns.get_mut(&thread_id) else {
        return;
    };
    let upload_dir = active.upload_dir.take();
    cleanup_codex_upload_dir(client, &active.thread, upload_dir.as_ref()).await;
}

pub(crate) async fn cleanup_all_active_turn_uploads(
    client: &mut dyn CodexTransport,
    active_turns: &mut ActiveTurns,
) {
    let thread_ids: Vec<ThreadId> = active_turns.keys().copied().collect();
    for thread_id in thread_ids {
        cleanup_active_turn_upload(client, active_turns, thread_id).await;
    }
}

pub(crate) async fn cleanup_codex_upload_dir(
    client: &mut dyn CodexTransport,
    thread: &ThreadHandle,
    upload_dir: Option<&PathBuf>,
) {
    let Some(upload_dir) = upload_dir else {
        return;
    };
    let params = codex_codes::FsRemoveParams {
        path: serde_json::json!(upload_dir.to_string_lossy()),
        recursive: Some(true),
        force: Some(true),
    };
    if let Err(error) = codex_request::<_, codex_codes::FsRemoveResponse>(
        client,
        CodexOperationContext::for_thread("upload_attachment_cleanup", thread),
        codex_codes::protocol::methods::FS_REMOVE,
        &params,
    )
    .await
    {
        warn!(
            thread_id = %thread.thread,
            harness_thread_id = %thread.harness_thread_id,
            path = %upload_dir.display(),
            error = %error,
            "failed to remove Codex attachment upload directory"
        );
    }
}

fn codex_upload_dir(thread: &ThreadHandle) -> PathBuf {
    let mut rng = rand::thread_rng();
    let nonce_high = rng.next_u64();
    let nonce_low = rng.next_u64();
    std::env::temp_dir()
        .join(CODEX_UPLOAD_DIR_NAME)
        .join(format!(
            "{}-{:016x}{:016x}",
            thread.thread, nonce_high, nonce_low
        ))
}

fn codex_upload_path(dir: &std::path::Path, index: usize, attachment: &UserAttachment) -> PathBuf {
    let mut nonce = [0_u8; 8];
    rand::thread_rng().fill_bytes(&mut nonce);
    dir.join(format!(
        "{index:02}-{}-{}",
        u64::from_le_bytes(nonce),
        safe_upload_file_name(&attachment.name)
    ))
}

fn safe_upload_file_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    let trimmed = sanitized.trim_matches(|ch| ch == '.' || ch == '_').trim();
    if trimmed.is_empty() {
        "attachment".into()
    } else {
        trimmed.chars().take(96).collect()
    }
}
