use std::path::PathBuf;

use tokio::io::AsyncWriteExt;

use super::LocalThreadStore;
use crate::ImportSnapshotChunkParams;
use crate::ReadThreadParams;
use crate::SnapshotRolloutChunk;
use crate::SnapshotRolloutParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

fn thread_store_io_error(err: std::io::Error) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: err.to_string(),
    }
}

/// Exports verbatim rollout lines for cross-daemon restore. Lines are read
/// through the same decompressing reader as history loads, so compressed
/// rollouts snapshot as plain JSONL text.
pub(super) async fn snapshot_rollout(
    store: &LocalThreadStore,
    params: SnapshotRolloutParams,
) -> ThreadStoreResult<SnapshotRolloutChunk> {
    let stored_thread = super::read_thread::read_thread(
        store,
        ReadThreadParams {
            thread_id: params.thread_id,
            include_archived: true,
            include_history: false,
        },
    )
    .await?;
    let rollout_path = stored_thread
        .rollout_path
        .ok_or(ThreadStoreError::ThreadNotFound {
            thread_id: params.thread_id,
        })?;
    let mut reader = codex_rollout::open_rollout_line_reader(&rollout_path)
        .await
        .map_err(thread_store_io_error)?;
    for _ in 0..params.offset {
        match reader.next_line().await.map_err(thread_store_io_error)? {
            Some(_) => {}
            None => {
                return Ok(SnapshotRolloutChunk {
                    lines: Vec::new(),
                    next_offset: None,
                });
            }
        }
    }
    let mut lines = Vec::new();
    for _ in 0..params.limit.max(1) {
        match reader.next_line().await.map_err(thread_store_io_error)? {
            Some(line) => lines.push(line.trim_end_matches(['\n', '\r']).to_string()),
            None => break,
        }
    }
    // Peek one line past the chunk: callers resume at offset + lines
    // returned, so the peeked line is re-read as the next chunk's first line.
    let has_more = reader
        .next_line()
        .await
        .map_err(thread_store_io_error)?
        .is_some();
    let next_offset = if has_more {
        Some(params.offset + lines.len() as u64)
    } else {
        None
    };
    Ok(SnapshotRolloutChunk { lines, next_offset })
}

fn validate_snapshot_id(snapshot_id: &str) -> ThreadStoreResult<()> {
    let valid = !snapshot_id.is_empty()
        && snapshot_id.len() <= 64
        && snapshot_id
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_');
    if valid {
        Ok(())
    } else {
        Err(ThreadStoreError::InvalidRequest {
            message: format!("snapshot id `{snapshot_id}` must match [A-Za-z0-9_-]{{1,64}}"),
        })
    }
}

/// Stages snapshot chunks under `imports/` in this store's home; the final
/// chunk adopts the staged rollout through the same path as `import_rollout`
/// and removes the staging file. Abandoned stagings are inert files under
/// `imports/` and never become discoverable threads.
pub(super) async fn import_snapshot_chunk(
    store: &LocalThreadStore,
    params: ImportSnapshotChunkParams,
) -> ThreadStoreResult<Option<PathBuf>> {
    validate_snapshot_id(&params.snapshot_id)?;
    let imports_dir = store.config.codex_home.join("imports");
    tokio::fs::create_dir_all(&imports_dir)
        .await
        .map_err(thread_store_io_error)?;
    let staging = imports_dir.join(format!("{}.jsonl", params.snapshot_id));
    {
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&staging)
            .await
            .map_err(thread_store_io_error)?;
        for line in &params.lines {
            file.write_all(line.as_bytes())
                .await
                .map_err(thread_store_io_error)?;
            file.write_all(b"\n").await.map_err(thread_store_io_error)?;
        }
        file.flush().await.map_err(thread_store_io_error)?;
    }
    if !params.is_final {
        return Ok(None);
    }
    let destination = super::live_writer::import_rollout(store, staging.clone()).await;
    let _ = tokio::fs::remove_file(&staging).await;
    destination.map(Some)
}
