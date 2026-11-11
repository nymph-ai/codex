//! Daemon-owned thread archives for cross-daemon snapshot/restore.
//!
//! An archive is one zstd-compressed tar written by the daemon that owns
//! the thread: a JSON manifest followed by the raw rollout files of the
//! thread's full lineage, in lineage order. The manifest carries the
//! thread's goal snapshot and attachments plus a SHA-256 digest per file,
//! over both the stored bytes and the decompressed rollout content, so the
//! receiving daemon can refuse divergent or torn state instead of guessing.
//!
//! Export is cold-only: every lineage owner must have no live writer, and
//! the files are packed under the same lifecycle/writer locks that guard
//! archive and delete. With `archive_source`, the source thread is archived
//! first, fencing it against local resume; that is the exact-move mode.
//! Import installs the files atomically, rebuilds the SQLite projections,
//! restores goal and attachments, and rolls back its own installs on error.

use std::collections::HashSet;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use chrono::DateTime;
use chrono::Utc;
use codex_protocol::ThreadId;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest;
use sha2::Sha256;

use super::LocalThreadStore;
use super::thread_rollout_resolver::RolloutLocation;
use crate::ArchiveThreadsParams;
use crate::ExportThreadArchiveParams;
use crate::ExportedThreadArchive;
use crate::ImportThreadArchiveParams;
use crate::ImportedThreadArchive;
use crate::ReadThreadParams;
use crate::ThreadStoreError;
use crate::ThreadStoreResult;

const ARCHIVE_FORMAT_VERSION: u32 = 1;
const ARCHIVE_DIR: &str = "thread-archives";
const MANIFEST_ENTRY: &str = "manifest.json";
const MAX_MANIFEST_BYTES: u64 = 16 * 1024 * 1024;
const MAX_SEGMENTS: usize = 1024;
const ZSTD_LEVEL: i32 = 3;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ThreadArchiveManifest {
    format_version: u32,
    thread_id: ThreadId,
    created_at_ms: i64,
    segments: Vec<ArchiveSegment>,
    goal: Option<ArchiveGoal>,
    attachments: Vec<ArchiveAttachment>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ArchiveSegment {
    rollout_id: ThreadId,
    owner_thread_id: ThreadId,
    file_name: String,
    entry_name: String,
    stored_byte_len: u64,
    stored_sha256: String,
    content_sha256: String,
    is_current: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ArchiveGoal {
    goal_id: String,
    objective: String,
    status: String,
    token_budget: Option<i64>,
    tokens_used: i64,
    time_used_seconds: i64,
    created_at_ms: i64,
    updated_at_ms: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct ArchiveAttachment {
    attachment_type: String,
    identity_key: String,
    payload: serde_json::Value,
}

fn internal(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::Internal {
        message: message.into(),
    }
}

fn invalid(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::InvalidRequest {
        message: message.into(),
    }
}

fn conflict(message: impl Into<String>) -> ThreadStoreError {
    ThreadStoreError::Conflict {
        message: message.into(),
    }
}

fn io_internal(context: &str, err: std::io::Error) -> ThreadStoreError {
    internal(format!("{context}: {err}"))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn hash_reader(reader: &mut impl Read) -> std::io::Result<(u64, String)> {
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        total += read as u64;
    }
    Ok((total, format!("{:x}", hasher.finalize())))
}

/// SHA-256 over the bytes as stored on disk.
fn stored_hash(path: &Path) -> std::io::Result<(u64, String)> {
    hash_reader(&mut std::fs::File::open(path)?)
}

fn is_compressed_rollout(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".jsonl.zst"))
}

/// SHA-256 over the decompressed rollout content, so a plain file and its
/// compressed sibling compare equal when they carry the same lines.
pub(super) fn content_hash(path: &Path) -> std::io::Result<String> {
    if is_compressed_rollout(path) {
        let mut decoder = zstd::stream::read::Decoder::new(std::fs::File::open(path)?)?;
        Ok(hash_reader(&mut decoder)?.1)
    } else {
        Ok(stored_hash(path)?.1)
    }
}

fn validate_manifest(manifest: &ThreadArchiveManifest) -> ThreadStoreResult<()> {
    if manifest.format_version != ARCHIVE_FORMAT_VERSION {
        return Err(invalid(format!(
            "unsupported thread archive format version {}",
            manifest.format_version
        )));
    }
    if manifest.segments.is_empty() || manifest.segments.len() > MAX_SEGMENTS {
        return Err(invalid(format!(
            "thread archive has {} rollout segments",
            manifest.segments.len()
        )));
    }
    if manifest.attachments.len() > codex_state::MAX_THREAD_ATTACHMENTS_PER_THREAD {
        return Err(invalid(format!(
            "thread archive has {} attachments",
            manifest.attachments.len()
        )));
    }
    let mut entry_names = HashSet::new();
    let mut file_names = HashSet::new();
    let mut current_count = 0usize;
    for segment in &manifest.segments {
        if segment.file_name.contains('/')
            || segment.file_name.contains('\\')
            || segment.file_name.contains("..")
        {
            return Err(invalid(format!(
                "thread archive segment has unsafe file name `{}`",
                segment.file_name
            )));
        }
        if codex_rollout::rollout_id_from_path(Path::new(&segment.file_name))
            != Some(segment.rollout_id)
        {
            return Err(invalid(format!(
                "thread archive segment file name `{}` does not encode rollout id {}",
                segment.file_name, segment.rollout_id
            )));
        }
        if segment.entry_name != format!("rollouts/{}", segment.file_name) {
            return Err(invalid(format!(
                "thread archive segment has unexpected entry name `{}`",
                segment.entry_name
            )));
        }
        if !is_sha256_hex(&segment.stored_sha256) || !is_sha256_hex(&segment.content_sha256) {
            return Err(invalid(format!(
                "thread archive segment `{}` has a malformed digest",
                segment.file_name
            )));
        }
        if !entry_names.insert(segment.entry_name.clone())
            || !file_names.insert(segment.file_name.clone())
        {
            return Err(invalid(format!(
                "thread archive repeats segment `{}`",
                segment.file_name
            )));
        }
        if segment.is_current {
            current_count += 1;
            if segment.owner_thread_id != manifest.thread_id {
                return Err(invalid(
                    "thread archive current segment is not owned by the archived thread",
                ));
            }
        }
    }
    if current_count != 1 {
        return Err(invalid(format!(
            "thread archive has {current_count} current segments"
        )));
    }
    Ok(())
}

struct HashWriter<W: Write> {
    inner: W,
    hasher: Sha256,
    len: u64,
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.hasher.update(&buffer[..written]);
        self.len += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

struct VerifyingReader<R: Read> {
    inner: R,
    hasher: Sha256,
    len: u64,
}

impl<R: Read> Read for VerifyingReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.hasher.update(&buffer[..read]);
        self.len += read as u64;
        Ok(read)
    }
}

struct PackSegment {
    source_path: PathBuf,
    entry_name: String,
    stored_byte_len: u64,
    stored_sha256: String,
}

fn append_entry<W: Write, R: Read>(
    builder: &mut tar::Builder<W>,
    entry_name: &str,
    byte_len: u64,
    reader: R,
) -> ThreadStoreResult<()> {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(byte_len);
    header.set_mode(0o600);
    header.set_cksum();
    builder
        .append_data(&mut header, entry_name, reader)
        .map_err(|err| io_internal(&format!("failed to pack `{entry_name}`"), err))
}

/// Writes the archive to `temp_path`, returning the SHA-256 and length of
/// the finished (compressed) file. Runs on a blocking thread.
fn write_archive(
    temp_path: &Path,
    manifest_bytes: &[u8],
    segments: &[PackSegment],
) -> ThreadStoreResult<(String, u64)> {
    let file = std::fs::File::create(temp_path)
        .map_err(|err| io_internal("failed to create thread archive", err))?;
    let writer = HashWriter {
        inner: file,
        hasher: Sha256::new(),
        len: 0,
    };
    let encoder = zstd::stream::write::Encoder::new(writer, ZSTD_LEVEL)
        .map_err(|err| io_internal("failed to start thread archive compression", err))?;
    let mut builder = tar::Builder::new(encoder);
    append_entry(
        &mut builder,
        MANIFEST_ENTRY,
        manifest_bytes.len() as u64,
        std::io::Cursor::new(manifest_bytes),
    )?;
    for segment in segments {
        let source = std::fs::File::open(&segment.source_path).map_err(|err| {
            io_internal(
                &format!("failed to open rollout `{}`", segment.source_path.display()),
                err,
            )
        })?;
        let mut reader = VerifyingReader {
            inner: source,
            hasher: Sha256::new(),
            len: 0,
        };
        append_entry(
            &mut builder,
            &segment.entry_name,
            segment.stored_byte_len,
            &mut reader,
        )?;
        let digest = format!("{:x}", reader.hasher.finalize());
        if reader.len != segment.stored_byte_len || digest != segment.stored_sha256 {
            return Err(conflict(format!(
                "rollout `{}` changed while it was being archived",
                segment.source_path.display()
            )));
        }
    }
    let encoder = builder
        .into_inner()
        .map_err(|err| io_internal("failed to finish thread archive", err))?;
    let writer = encoder
        .finish()
        .map_err(|err| io_internal("failed to finish thread archive compression", err))?;
    writer
        .inner
        .sync_all()
        .map_err(|err| io_internal("failed to sync thread archive", err))?;
    Ok((format!("{:x}", writer.hasher.finalize()), writer.len))
}

/// Extracts an archive into `staging_dir` (created fresh), verifying the
/// manifest and every stored/content digest. Returns the manifest; staged
/// rollout files land at `staging_dir/<file_name>`. Runs on a blocking
/// thread and never writes outside `staging_dir`.
fn extract_archive(
    archive_path: &Path,
    staging_dir: &Path,
) -> ThreadStoreResult<ThreadArchiveManifest> {
    std::fs::create_dir_all(staging_dir)
        .map_err(|err| io_internal("failed to create thread archive staging directory", err))?;
    let file = std::fs::File::open(archive_path).map_err(|err| {
        io_internal(
            &format!("failed to open thread archive `{}`", archive_path.display()),
            err,
        )
    })?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .map_err(|err| io_internal("failed to decompress thread archive", err))?;
    let mut archive = tar::Archive::new(decoder);
    let mut entries = archive
        .entries()
        .map_err(|err| io_internal("failed to read thread archive entries", err))?;

    let mut manifest_entry = entries
        .next()
        .ok_or_else(|| invalid("thread archive is empty"))?
        .map_err(|err| io_internal("failed to read thread archive manifest entry", err))?;
    let manifest_path = manifest_entry
        .path()
        .map_err(|err| io_internal("failed to read thread archive manifest path", err))?
        .into_owned();
    if manifest_path != Path::new(MANIFEST_ENTRY) {
        return Err(invalid(format!(
            "thread archive first entry is `{}`, expected `{MANIFEST_ENTRY}`",
            manifest_path.display()
        )));
    }
    if manifest_entry.header().entry_type() != tar::EntryType::Regular {
        return Err(invalid("thread archive manifest is not a regular file"));
    }
    let manifest_size = manifest_entry.header().size().unwrap_or(0);
    if manifest_size > MAX_MANIFEST_BYTES {
        return Err(invalid(format!(
            "thread archive manifest is {manifest_size} bytes"
        )));
    }
    let mut manifest_bytes = Vec::with_capacity(manifest_size as usize);
    manifest_entry
        .read_to_end(&mut manifest_bytes)
        .map_err(|err| io_internal("failed to read thread archive manifest", err))?;
    let manifest: ThreadArchiveManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|err| invalid(format!("thread archive manifest is malformed: {err}")))?;
    validate_manifest(&manifest)?;

    for segment in &manifest.segments {
        let mut entry = entries
            .next()
            .ok_or_else(|| {
                invalid(format!(
                    "thread archive is missing segment `{}`",
                    segment.entry_name
                ))
            })?
            .map_err(|err| io_internal("failed to read thread archive segment entry", err))?;
        let entry_path = entry
            .path()
            .map_err(|err| io_internal("failed to read thread archive segment path", err))?
            .into_owned();
        if entry_path != Path::new(&segment.entry_name) {
            return Err(invalid(format!(
                "thread archive entry `{}` does not match manifest segment `{}`",
                entry_path.display(),
                segment.entry_name
            )));
        }
        if entry.header().entry_type() != tar::EntryType::Regular {
            return Err(invalid(format!(
                "thread archive segment `{}` is not a regular file",
                segment.entry_name
            )));
        }
        if entry.header().size().unwrap_or(0) != segment.stored_byte_len {
            return Err(invalid(format!(
                "thread archive segment `{}` size does not match its manifest",
                segment.entry_name
            )));
        }
        let staged_path = staging_dir.join(&segment.file_name);
        let staged = std::fs::File::create(&staged_path)
            .map_err(|err| io_internal("failed to stage thread archive segment", err))?;
        let mut writer = HashWriter {
            inner: staged,
            hasher: Sha256::new(),
            len: 0,
        };
        std::io::copy(&mut entry, &mut writer)
            .map_err(|err| io_internal("failed to stage thread archive segment", err))?;
        writer
            .flush()
            .map_err(|err| io_internal("failed to stage thread archive segment", err))?;
        let digest = format!("{:x}", writer.hasher.finalize());
        if writer.len != segment.stored_byte_len || digest != segment.stored_sha256 {
            return Err(invalid(format!(
                "thread archive segment `{}` does not match its stored digest",
                segment.entry_name
            )));
        }
    }
    if entries.next().is_some() {
        return Err(invalid("thread archive has entries beyond its manifest"));
    }

    for segment in &manifest.segments {
        let staged_path = staging_dir.join(&segment.file_name);
        let digest = content_hash(&staged_path)
            .map_err(|err| io_internal("failed to hash staged thread archive segment", err))?;
        if digest != segment.content_sha256 {
            return Err(invalid(format!(
                "thread archive segment `{}` does not match its content digest",
                segment.entry_name
            )));
        }
    }
    Ok(manifest)
}

struct SourceSegment {
    rollout_id: ThreadId,
    owner_thread_id: ThreadId,
    path: PathBuf,
    file_name: String,
    is_current: bool,
}

/// Resolves the thread's full rollout lineage to concrete files, oldest
/// first, with the currently selected segment marked.
async fn lineage_sources(
    store: &LocalThreadStore,
    thread_id: ThreadId,
) -> ThreadStoreResult<(
    Vec<SourceSegment>,
    super::thread_rollout_resolver::ResolvedThreadRollout,
)> {
    let selected =
        super::thread_rollout_resolver::resolve_current_including_archived(store, thread_id)
            .await?
            .ok_or(ThreadStoreError::ThreadNotFound { thread_id })?;
    // The lineage resolver only follows paginated history-base pointers; a
    // legacy thread's whole history lives in its one selected rollout.
    let selected_meta = codex_rollout::read_session_meta_line(&selected.path)
        .await
        .map_err(|err| {
            internal(format!(
                "failed to read rollout metadata {}: {err}",
                selected.path.display()
            ))
        })?;
    if selected_meta.meta.history_mode == codex_protocol::protocol::ThreadHistoryMode::Legacy {
        let file_name = selected
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .ok_or_else(|| {
                invalid(format!(
                    "rollout path `{}` has no usable file name",
                    selected.path.display()
                ))
            })?;
        return Ok((
            vec![SourceSegment {
                rollout_id: selected.rollout_id,
                owner_thread_id: selected_meta.meta.id,
                path: selected.path.clone(),
                file_name,
                is_current: true,
            }],
            selected,
        ));
    }
    let lineage = store.resolve_rollout_lineage(thread_id, None).await?;
    let mut seen_paths = HashSet::new();
    let mut sources = Vec::new();
    for segment in lineage.segments {
        if !seen_paths.insert(segment.rollout_path.clone()) {
            continue;
        }
        let meta = codex_rollout::read_session_meta_line(&segment.rollout_path)
            .await
            .map_err(|err| {
                internal(format!(
                    "failed to read rollout metadata {}: {err}",
                    segment.rollout_path.display()
                ))
            })?;
        let file_name = segment
            .rollout_path
            .file_name()
            .and_then(|name| name.to_str())
            .map(str::to_string)
            .ok_or_else(|| {
                invalid(format!(
                    "rollout path `{}` has no usable file name",
                    segment.rollout_path.display()
                ))
            })?;
        sources.push(SourceSegment {
            rollout_id: segment.rollout_id,
            owner_thread_id: meta.meta.id,
            is_current: segment.rollout_id == selected.rollout_id,
            path: segment.rollout_path,
            file_name,
        });
    }
    if sources.iter().filter(|source| source.is_current).count() != 1 {
        return Err(internal(format!(
            "lineage for thread {thread_id} does not contain its selected rollout"
        )));
    }
    Ok((sources, selected))
}

struct ColdLocks {
    _lifecycle: Vec<tokio::sync::OwnedRwLockWriteGuard<()>>,
    _writer: Vec<tokio::sync::OwnedMutexGuard<()>>,
    _file_locks: Vec<std::sync::Arc<codex_rollout::WriterLockGuard>>,
}

/// Locks every listed thread in archive/delete lock order and requires that
/// none of them has a live writer: archives only move cold threads.
async fn lock_cold(
    store: &LocalThreadStore,
    mut thread_ids: Vec<ThreadId>,
) -> ThreadStoreResult<ColdLocks> {
    thread_ids.sort_unstable_by_key(ToString::to_string);
    thread_ids.dedup();
    let mut lifecycle = Vec::with_capacity(thread_ids.len());
    for thread_id in &thread_ids {
        lifecycle.push(store.live_writer_locks.lock_lifecycle(*thread_id).await);
    }
    let mut writer = Vec::with_capacity(thread_ids.len());
    for thread_id in &thread_ids {
        writer.push(store.live_writer_locks.lock(*thread_id).await);
        if store.live_recorders.lock().await.contains_key(thread_id) {
            return Err(conflict(format!(
                "thread {thread_id} has a live writer; thread archives require a cold thread"
            )));
        }
    }
    let file_locks = store.acquire_writer_locks(&thread_ids).await?;
    Ok(ColdLocks {
        _lifecycle: lifecycle,
        _writer: writer,
        _file_locks: file_locks,
    })
}

async fn capture_goal(
    state_db: &codex_state::StateRuntime,
    thread_id: ThreadId,
) -> ThreadStoreResult<Option<ArchiveGoal>> {
    let goal = state_db
        .thread_goals()
        .get_thread_goal(thread_id)
        .await
        .map_err(|err| internal(format!("failed to read thread goal: {err}")))?;
    Ok(goal.map(|goal| ArchiveGoal {
        goal_id: goal.goal_id,
        objective: goal.objective,
        status: goal.status.as_str().to_string(),
        token_budget: goal.token_budget,
        tokens_used: goal.tokens_used,
        time_used_seconds: goal.time_used_seconds,
        created_at_ms: goal.created_at.timestamp_millis(),
        updated_at_ms: goal.updated_at.timestamp_millis(),
    }))
}

async fn capture_attachments(
    state_db: &codex_state::StateRuntime,
    thread_id: ThreadId,
) -> ThreadStoreResult<Vec<ArchiveAttachment>> {
    let mut attachments = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = state_db
            .list_thread_attachments(
                thread_id,
                cursor.as_deref(),
                codex_state::MAX_THREAD_ATTACHMENT_LIST_PAGE_SIZE,
            )
            .await
            .map_err(|err| internal(format!("failed to read thread attachments: {err}")))?;
        attachments.extend(
            page.attachments
                .into_iter()
                .map(|attachment| ArchiveAttachment {
                    attachment_type: attachment.attachment_type,
                    identity_key: attachment.identity_key,
                    payload: attachment.payload,
                }),
        );
        match page.next_cursor {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    Ok(attachments)
}

fn require_state_db(store: &LocalThreadStore) -> ThreadStoreResult<&codex_rollout::StateDbHandle> {
    store.state_db.as_ref().ok_or_else(|| {
        internal("thread archives require the state database so goal and attachment state cannot be lost silently")
    })
}

/// Entry point for `ThreadStore::export_thread_archive`.
pub(super) async fn export_thread_archive(
    store: &LocalThreadStore,
    params: ExportThreadArchiveParams,
) -> ThreadStoreResult<ExportedThreadArchive> {
    let thread_id = params.thread_id;
    let state_db = require_state_db(store)?.clone();

    // Relationship preflight runs before any fencing: a thread whose graph
    // cannot move whole must stay exactly as it was.
    let stored = super::read_thread::read_thread(
        store,
        ReadThreadParams {
            thread_id,
            include_archived: true,
            include_history: false,
        },
    )
    .await?;
    if let Some(parent) = stored.parent_thread_id {
        return Err(invalid(format!(
            "thread {thread_id} is a subagent of {parent}; move the root thread instead"
        )));
    }
    let descendants = state_db
        .list_thread_spawn_descendants(thread_id)
        .await
        .map_err(|err| internal(format!("failed to list spawned descendants: {err}")))?;
    if !descendants.is_empty() {
        return Err(invalid(format!(
            "thread {thread_id} has {} spawned descendant(s); thread archives move one thread at a time",
            descendants.len()
        )));
    }
    let reference_index =
        codex_rollout::RolloutReferenceIndex::scan(store.config.codex_home.as_path())
            .await
            .map_err(|err| internal(format!("failed to scan thread rollout files: {err}")))?;
    if let Some(child) = reference_index.attached_child(&[thread_id]) {
        return Err(invalid(format!(
            "thread {thread_id} has attached thread {child}; thread archives move one thread at a time"
        )));
    }
    let (_, selected) = lineage_sources(store, thread_id).await?;
    let selected_meta = codex_rollout::read_session_meta_line(&selected.path)
        .await
        .map_err(|err| {
            internal(format!(
                "failed to read rollout metadata {}: {err}",
                selected.path.display()
            ))
        })?;
    if selected_meta.meta.attached_thread {
        return Err(invalid(format!(
            "thread {thread_id} is attached to another thread; move its parent instead"
        )));
    }

    // Exact-move fencing: archive under the native writer locks first, so
    // the files packed below are the archived (final) source state.
    let mut source_archived = selected.location == RolloutLocation::Archived;
    if params.archive_source && !source_archived {
        super::archive_thread::archive_threads(
            store,
            ArchiveThreadsParams {
                thread_ids: vec![thread_id],
                writer_lock_thread_ids: Vec::new(),
            },
        )
        .await?;
        source_archived = true;
    }

    let (sources, _) = lineage_sources(store, thread_id).await?;
    let mut lock_ids: Vec<ThreadId> = sources
        .iter()
        .map(|source| source.owner_thread_id)
        .collect();
    lock_ids.push(thread_id);
    let _locks = lock_cold(store, lock_ids).await?;

    // Re-resolve under the locks; anything that moved since the preflight
    // aborts the export instead of packing a torn lineage.
    let (locked_sources, locked_selected) = lineage_sources(store, thread_id).await?;
    let before: Vec<&Path> = sources.iter().map(|source| source.path.as_path()).collect();
    let after: Vec<&Path> = locked_sources
        .iter()
        .map(|source| source.path.as_path())
        .collect();
    if before != after {
        return Err(conflict(format!(
            "thread {thread_id} changed while its archive export was starting; retry the export"
        )));
    }
    if params.archive_source && locked_selected.location != RolloutLocation::Archived {
        return Err(conflict(format!(
            "thread {thread_id} is no longer archived; retry the export"
        )));
    }

    let goal = capture_goal(&state_db, thread_id).await?;
    let attachments = capture_attachments(&state_db, thread_id).await?;

    let hash_paths: Vec<PathBuf> = locked_sources
        .iter()
        .map(|source| source.path.clone())
        .collect();
    let hashes =
        tokio::task::spawn_blocking(move || -> ThreadStoreResult<Vec<(u64, String, String)>> {
            hash_paths
                .iter()
                .map(|path| {
                    let (len, stored) = stored_hash(path).map_err(|err| {
                        io_internal(&format!("failed to hash rollout `{}`", path.display()), err)
                    })?;
                    let content = content_hash(path).map_err(|err| {
                        io_internal(&format!("failed to hash rollout `{}`", path.display()), err)
                    })?;
                    Ok((len, stored, content))
                })
                .collect()
        })
        .await
        .map_err(|err| internal(format!("thread archive hashing failed: {err}")))??;

    let segments: Vec<ArchiveSegment> = locked_sources
        .iter()
        .zip(hashes.iter())
        .map(|(source, (len, stored, content))| ArchiveSegment {
            rollout_id: source.rollout_id,
            owner_thread_id: source.owner_thread_id,
            entry_name: format!("rollouts/{}", source.file_name),
            file_name: source.file_name.clone(),
            stored_byte_len: *len,
            stored_sha256: stored.clone(),
            content_sha256: content.clone(),
            is_current: source.is_current,
        })
        .collect();
    let manifest = ThreadArchiveManifest {
        format_version: ARCHIVE_FORMAT_VERSION,
        thread_id,
        created_at_ms: Utc::now().timestamp_millis(),
        segments,
        goal,
        attachments,
    };
    validate_manifest(&manifest)?;
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).map_err(|err| {
        internal(format!(
            "failed to serialize thread archive manifest: {err}"
        ))
    })?;

    let archive_dir = store.config.codex_home.join(ARCHIVE_DIR);
    tokio::fs::create_dir_all(&archive_dir)
        .await
        .map_err(|err| io_internal("failed to create thread archive directory", err))?;
    let archive_id = ThreadId::new();
    let archive_path =
        archive_dir.join(format!("thread-{thread_id}-{archive_id}.codex-thread.zst"));
    let temp_path = archive_dir.join(format!(".thread-{thread_id}-{archive_id}.tmp"));
    let pack_segments: Vec<PackSegment> = locked_sources
        .iter()
        .zip(manifest.segments.iter())
        .map(|(source, segment)| PackSegment {
            source_path: source.path.clone(),
            entry_name: segment.entry_name.clone(),
            stored_byte_len: segment.stored_byte_len,
            stored_sha256: segment.stored_sha256.clone(),
        })
        .collect();
    let pack_temp = temp_path.clone();
    let packed = tokio::task::spawn_blocking(move || {
        write_archive(&pack_temp, &manifest_bytes, &pack_segments)
    })
    .await
    .map_err(|err| internal(format!("thread archive packing failed: {err}")))?;
    let (sha256, byte_len) = match packed {
        Ok(packed) => packed,
        Err(err) => {
            let _ = tokio::fs::remove_file(&temp_path).await;
            return Err(err);
        }
    };
    tokio::fs::rename(&temp_path, &archive_path)
        .await
        .map_err(|err| io_internal("failed to publish thread archive", err))?;

    Ok(ExportedThreadArchive {
        thread_id,
        archive_path,
        sha256,
        byte_len,
        source_archived,
    })
}

fn goal_from_archive(
    thread_id: ThreadId,
    goal: &ArchiveGoal,
) -> ThreadStoreResult<codex_state::ThreadGoal> {
    codex_protocol::protocol::validate_thread_goal_objective(&goal.objective)
        .map_err(|err| invalid(format!("thread archive goal is invalid: {err}")))?;
    let status = codex_state::ThreadGoalStatus::try_from(goal.status.as_str())
        .map_err(|err| invalid(format!("thread archive goal is invalid: {err}")))?;
    let created_at = DateTime::from_timestamp_millis(goal.created_at_ms)
        .ok_or_else(|| invalid("thread archive goal has an invalid creation timestamp"))?;
    let updated_at = DateTime::from_timestamp_millis(goal.updated_at_ms)
        .ok_or_else(|| invalid("thread archive goal has an invalid update timestamp"))?;
    Ok(codex_state::ThreadGoal {
        thread_id,
        goal_id: goal.goal_id.clone(),
        objective: goal.objective.clone(),
        status,
        token_budget: goal.token_budget,
        tokens_used: goal.tokens_used,
        time_used_seconds: goal.time_used_seconds,
        created_at,
        updated_at,
    })
}

fn destination_for(
    store: &LocalThreadStore,
    segment: &ArchiveSegment,
) -> ThreadStoreResult<PathBuf> {
    let (year, month, day) = codex_rollout::rollout_date_parts(std::ffi::OsStr::new(
        &segment.file_name,
    ))
    .ok_or_else(|| {
        invalid(format!(
            "thread archive segment `{}` has no rollout date",
            segment.file_name
        ))
    })?;
    Ok(store
        .config
        .codex_home
        .join(codex_rollout::SESSIONS_SUBDIR)
        .join(year)
        .join(month)
        .join(day)
        .join(&segment.file_name))
}

#[derive(Default)]
struct InstallProgress {
    installed_new: Vec<PathBuf>,
    state_touched: bool,
}

async fn rollback_import(
    store: &LocalThreadStore,
    thread_id: ThreadId,
    progress: &InstallProgress,
) -> Result<(), String> {
    let mut errors = Vec::new();
    if progress.state_touched
        && let Err(err) = super::delete_thread::delete_state_rows(store, &[thread_id]).await
    {
        errors.push(err.to_string());
    }
    for path in progress.installed_new.iter().rev() {
        match tokio::fs::remove_file(path).await {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => errors.push(format!("failed to remove `{}`: {err}", path.display())),
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

/// Entry point for `ThreadStore::import_thread_archive`.
pub(super) async fn import_thread_archive(
    store: &LocalThreadStore,
    params: ImportThreadArchiveParams,
) -> ThreadStoreResult<ImportedThreadArchive> {
    let state_db = require_state_db(store)?.clone();
    let staging_dir = store
        .config
        .codex_home
        .join("imports")
        .join(format!("thread-archive-{}", ThreadId::new()));
    let extract_path = params.archive_path.clone();
    let extract_dir = staging_dir.clone();
    let extracted =
        tokio::task::spawn_blocking(move || extract_archive(&extract_path, &extract_dir))
            .await
            .map_err(|err| internal(format!("thread archive extraction failed: {err}")))?;
    let manifest = match extracted {
        Ok(manifest) => manifest,
        Err(err) => {
            let _ = tokio::fs::remove_dir_all(&staging_dir).await;
            return Err(err);
        }
    };
    let result = import_staged(store, &state_db, &manifest, &staging_dir).await;
    let _ = tokio::fs::remove_dir_all(&staging_dir).await;
    result
}

async fn import_staged(
    store: &LocalThreadStore,
    state_db: &codex_rollout::StateDbHandle,
    manifest: &ThreadArchiveManifest,
    staging_dir: &Path,
) -> ThreadStoreResult<ImportedThreadArchive> {
    let thread_id = manifest.thread_id;

    // The staged files must agree with the manifest about who they are
    // before anything is installed or locked.
    for segment in &manifest.segments {
        let staged_path = staging_dir.join(&segment.file_name);
        let meta = codex_rollout::read_session_meta_line(&staged_path)
            .await
            .map_err(|err| {
                invalid(format!(
                    "thread archive segment `{}` has unreadable session metadata: {err}",
                    segment.file_name
                ))
            })?;
        if meta.meta.id != segment.owner_thread_id {
            return Err(invalid(format!(
                "thread archive segment `{}` belongs to thread {}, not {}",
                segment.file_name, meta.meta.id, segment.owner_thread_id
            )));
        }
        if segment.is_current && (meta.meta.attached_thread || meta.meta.parent_thread_id.is_some())
        {
            return Err(invalid(
                "thread archive carries a subagent or attached thread; only root threads move",
            ));
        }
    }
    let goal = manifest
        .goal
        .as_ref()
        .map(|goal| goal_from_archive(thread_id, goal))
        .transpose()?;

    let mut lock_ids: Vec<ThreadId> = manifest
        .segments
        .iter()
        .map(|segment| segment.owner_thread_id)
        .collect();
    lock_ids.push(thread_id);
    let _locks = lock_cold(store, lock_ids).await?;

    if state_db
        .get_thread(thread_id)
        .await
        .map_err(|err| internal(format!("failed to read thread metadata: {err}")))?
        .is_some()
    {
        return Err(conflict(format!(
            "thread {thread_id} already exists in this store; resume it by thread id instead of importing over it"
        )));
    }

    // Plan every destination before installing any of them. A pre-existing
    // file is kept only when its content digest matches exactly.
    let mut plan: Vec<(&ArchiveSegment, PathBuf, bool)> = Vec::new();
    for segment in &manifest.segments {
        let destination = destination_for(store, segment)?;
        match codex_rollout::existing_rollout_path(&destination).await {
            Some(existing) => {
                let existing_path = existing.clone();
                let digest = tokio::task::spawn_blocking(move || content_hash(&existing_path))
                    .await
                    .map_err(|err| internal(format!("thread archive hashing failed: {err}")))?
                    .map_err(|err| io_internal("failed to hash existing rollout", err))?;
                if digest != segment.content_sha256 {
                    return Err(conflict(format!(
                        "rollout `{}` already exists in this store with different content; refusing to guess which lineage wins",
                        existing.display()
                    )));
                }
                plan.push((segment, existing, true));
            }
            None => plan.push((segment, destination, false)),
        }
    }

    let mut progress = InstallProgress::default();
    let installed = install_segments(
        store,
        state_db,
        manifest,
        staging_dir,
        &plan,
        goal.as_ref(),
        &mut progress,
    )
    .await;
    match installed {
        Ok(imported) => Ok(imported),
        Err(err) => match rollback_import(store, thread_id, &progress).await {
            Ok(()) => Err(err),
            Err(rollback_err) => Err(internal(format!(
                "thread archive import failed: {err}; rollback also failed: {rollback_err}"
            ))),
        },
    }
}

async fn install_segments(
    store: &LocalThreadStore,
    state_db: &codex_rollout::StateDbHandle,
    manifest: &ThreadArchiveManifest,
    staging_dir: &Path,
    plan: &[(&ArchiveSegment, PathBuf, bool)],
    goal: Option<&codex_state::ThreadGoal>,
    progress: &mut InstallProgress,
) -> ThreadStoreResult<ImportedThreadArchive> {
    let thread_id = manifest.thread_id;
    let mut root_path: Option<PathBuf> = None;
    for (segment, destination, preexisting) in plan {
        if segment.is_current {
            root_path = Some(destination.clone());
        }
        if *preexisting {
            continue;
        }
        let parent = destination.parent().ok_or_else(|| {
            internal(format!(
                "rollout destination `{}` has no parent",
                destination.display()
            ))
        })?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|err| io_internal("failed to create rollout directory", err))?;
        // Copy to a sibling temp name and rename so a crash mid-copy cannot
        // leave a torn rollout at the canonical path.
        let staging_destination = destination.with_extension("importing");
        let staged_path = staging_dir.join(&segment.file_name);
        tokio::fs::copy(&staged_path, &staging_destination)
            .await
            .map_err(|err| io_internal("failed to install rollout from thread archive", err))?;
        if let Err(err) = tokio::fs::rename(&staging_destination, destination).await {
            let _ = tokio::fs::remove_file(&staging_destination).await;
            return Err(io_internal(
                "failed to publish rollout from thread archive",
                err,
            ));
        }
        progress.installed_new.push(destination.clone());
    }
    let root_path =
        root_path.ok_or_else(|| internal("thread archive plan has no current segment"))?;

    progress.state_touched = true;
    super::thread_history_materialization::materialize_to_sqlite(store, thread_id, &root_path)
        .await?;
    codex_rollout::state_db::read_repair_rollout_path(
        Some(state_db.as_ref()),
        Some(thread_id),
        Some(false),
        &root_path,
    )
    .await;
    if state_db
        .get_thread(thread_id)
        .await
        .map_err(|err| internal(format!("failed to read imported thread metadata: {err}")))?
        .is_none()
    {
        return Err(internal(format!(
            "imported thread {thread_id} was not registered in the state database"
        )));
    }

    for attachment in &manifest.attachments {
        let outcome = state_db
            .add_thread_attachment(
                thread_id,
                &attachment.attachment_type,
                &attachment.identity_key,
                &attachment.payload,
            )
            .await
            .map_err(|err| internal(format!("failed to restore thread attachment: {err}")))?;
        if let codex_state::AddThreadAttachmentOutcome::Existing(existing) = outcome
            && existing.payload != attachment.payload
        {
            return Err(conflict(format!(
                "thread {thread_id} already has attachment {}/{} with a different payload",
                attachment.attachment_type, attachment.identity_key
            )));
        }
    }

    if let Some(goal) = goal {
        state_db
            .thread_goals()
            .replace_thread_goal_snapshot(goal)
            .await
            .map_err(|err| internal(format!("failed to restore thread goal: {err}")))?;
    }

    let read = super::read_thread::read_thread(
        store,
        ReadThreadParams {
            thread_id,
            include_archived: false,
            include_history: false,
        },
    )
    .await?;
    if read.thread_id != thread_id || read.archived_at.is_some() {
        return Err(internal(format!(
            "imported thread {thread_id} did not come back as an active thread"
        )));
    }

    Ok(ImportedThreadArchive {
        thread_id,
        rollout_path: root_path,
    })
}
