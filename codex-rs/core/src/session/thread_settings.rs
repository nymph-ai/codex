//! Handles persistent thread-settings updates and serializes their persistence
//! with checkpoints written directly to storage.

use super::session::Session;
use super::session::SessionSettingsCommit;
use super::session::SessionSettingsUpdate;
use super::step_settings::StepSettingsUpdate;
use crate::ThreadConfigSnapshot;
use crate::WithTurnExtensionData;
use crate::config::ConstraintResult;
use codex_history::InitialHistory;
use codex_history::RolloutItem;
use codex_protocol::ThreadId;
use codex_protocol::capabilities::SelectedCapabilityRoot;
use codex_protocol::config_types::CollaborationMode;
use codex_protocol::config_types::ThreadResumeSetupOption;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result as CodexResult;
use codex_protocol::openai_models::ModelInfo;
use codex_protocol::openai_models::ReasoningEffort;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ThreadSettingsAppliedEvent;
use codex_protocol::protocol::ThreadSettingsOverrides;
use codex_protocol::protocol::ThreadSettingsSnapshot;
use codex_thread_store::ThreadStoreError;
use codex_thread_store::ThreadStoreResult;
use std::sync::Arc;
use tokio::sync::SemaphorePermit;

/// Typed startup data consumed before the session is exposed to hooks or inference.
pub(crate) struct InitialThreadSetup(pub(crate) Vec<ThreadResumeSetupOption>);

pub(crate) fn validate_initial_setup_options(
    options: &[ThreadResumeSetupOption],
) -> CodexResult<()> {
    let mut mode_seen = false;
    let mut effort_seen = false;
    for option in options {
        match option {
            ThreadResumeSetupOption::CollaborationMode(_) => {
                if std::mem::replace(&mut mode_seen, true) {
                    return Err(CodexErr::InvalidRequest(
                        "duplicate collaborationMode in initialSettings".into(),
                    ));
                }
            }
            ThreadResumeSetupOption::ReasoningEffort(effort) => {
                if std::mem::replace(&mut effort_seen, true) {
                    return Err(CodexErr::InvalidRequest(
                        "duplicate reasoningEffort in initialSettings".into(),
                    ));
                }
                if matches!(effort, ReasoningEffort::Custom(_)) {
                    return Err(CodexErr::InvalidRequest(
                        "initialSettings requires a supported native reasoningEffort".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn validate_retained_initial_setup(history: &InitialHistory) -> CodexResult<()> {
    let InitialHistory::Resumed(resumed) = history else {
        return Err(CodexErr::InvalidRequest(
            "initialSettings requires a retained root thread for cold resume".into(),
        ));
    };
    if resumed.history_revision.is_none() {
        return Err(CodexErr::InvalidRequest(
            "native history changed while preparing initialSettings; retry cold resume".into(),
        ));
    }
    if !has_original_root_metadata(resumed.conversation_id, &resumed.history) {
        return Err(CodexErr::InvalidRequest(
            "initialSettings requires an original retained root thread for cold resume".into(),
        ));
    }

    if !is_settled_setup_boundary(resumed.conversation_id, &resumed.history) {
        return Err(CodexErr::InvalidRequest(
            "initialSettings requires a settled retained turn or an unprompted setup checkpoint"
                .into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_fork_initial_setup(
    source_id: ThreadId,
    history: &[RolloutItem],
) -> CodexResult<()> {
    if !has_original_root_metadata(source_id, history) {
        return Err(CodexErr::InvalidRequest(
            "fork initialSettings requires an original retained root source".into(),
        ));
    }
    if !is_settled_setup_boundary(source_id, history) {
        return Err(CodexErr::InvalidRequest(
            "fork initialSettings requires a settled retained source boundary".into(),
        ));
    }
    Ok(())
}

fn has_original_root_metadata(thread_id: ThreadId, history: &[RolloutItem]) -> bool {
    matches!(
        history.first(),
        Some(RolloutItem::SessionMeta(meta))
            if meta.meta.id == thread_id
                && meta.meta.parent_thread_id.is_none()
                && meta.meta.forked_from_id.is_none()
                && !meta.meta.source.is_non_root_agent()
    )
}

fn is_settled_setup_boundary(thread_id: ThreadId, history: &[RolloutItem]) -> bool {
    // A final matching native completion proves a suspension cannot be awaiting continuation.
    // An unprompted root with its own durable setup checkpoint is also a complete boundary.
    let mut boundary = history.iter().rev().filter_map(|item| match item {
        RolloutItem::EventMsg(
            event @ (EventMsg::TurnStarted(_)
            | EventMsg::TurnComplete(_)
            | EventMsg::TurnAborted(_)),
        ) => Some(event),
        _ => None,
    });
    match boundary.next() {
        Some(EventMsg::TurnComplete(completed)) => matches!(
            boundary.next(),
            Some(EventMsg::TurnStarted(started)) if started.turn_id == completed.turn_id
        ),
        None => {
            let mut has_checkpoint = false;
            let pristine = history.iter().all(|item| match item {
                RolloutItem::SessionMeta(meta) => meta.meta.id == thread_id,
                RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(event))
                    if event.thread_id == Some(thread_id) =>
                {
                    has_checkpoint = true;
                    true
                }
                _ => false,
            });
            pristine && has_checkpoint
        }
        _ => false,
    }
}

pub(crate) fn resolve_initial_setup_options(
    mut selection: CollaborationMode,
    options: &[ThreadResumeSetupOption],
    model: Option<&ModelInfo>,
) -> CodexResult<CollaborationMode> {
    validate_initial_setup_options(options)?;
    for option in options {
        match option {
            ThreadResumeSetupOption::CollaborationMode(mode) => {
                selection.mode = *mode;
                selection.settings.developer_instructions = None;
            }
            ThreadResumeSetupOption::ReasoningEffort(effort) => {
                if model.is_some_and(|model| {
                    let allowed = &model.supported_reasoning_levels;
                    !allowed.is_empty() && !allowed.iter().any(|entry| &entry.effort == effort)
                }) {
                    return Err(CodexErr::InvalidRequest(
                        "initialSettings reasoningEffort is unavailable on the selected model"
                            .into(),
                    ));
                }
                selection.settings.reasoning_effort = Some(effort.clone());
            }
        }
    }
    Ok(selection)
}

impl Session {
    /// The persistence and turn-reservation locks fence both admitted user turns and
    /// subsequent settings updates while one validated candidate is committed.
    #[expect(
        clippy::await_holding_invalid_type,
        reason = "a new turn must not observe setup before its checkpoint is published"
    )]
    pub(crate) async fn apply_resume_setup_options(
        &self,
        options: &[ThreadResumeSetupOption],
    ) -> CodexResult<(ThreadConfigSnapshot, Option<String>, bool)> {
        let _persistence = acquire_persistence_lock(self).await;
        let (configuration, changed) = {
            let active_turn = self.active_turn.lock().await;
            if active_turn.is_some() || self.input_queue.has_trigger_turn_mailbox_items().await {
                return Err(CodexErr::InvalidRequest(
                    "initialSettings requires an idle thread with no reserved turn".into(),
                ));
            }
            let Some(live_thread) = self.live_thread() else {
                return Err(CodexErr::InvalidRequest(
                    "initialSettings requires a durable thread".into(),
                ));
            };
            let mut state = self.state.lock().await;
            let previous = &state.session_configuration;
            if previous.parent_thread_id.is_some() {
                return Err(CodexErr::InvalidRequest(
                    "initialSettings requires a root thread".into(),
                ));
            }

            let catalog = self.services.models_manager.refreshed_model_catalog();
            let model = catalog.as_deref().and_then(|models| {
                models
                    .iter()
                    .find(|model| model.slug == previous.step_settings.collaboration_mode.model())
            });
            let selection = resolve_initial_setup_options(
                previous.step_settings.collaboration_mode.clone(),
                options,
                model,
            )?;
            let next = self
                .apply_session_settings(
                    previous,
                    &SessionSettingsUpdate {
                        step_settings: StepSettingsUpdate {
                            collaboration_mode: Some(selection),
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .map_err(|err| CodexErr::InvalidRequest(err.to_string()))?;

            let changed = next.step_settings != previous.step_settings;
            if changed {
                let snapshot = next.thread_settings_snapshot(&next.environments);
                let persisted = |err| {
                    CodexErr::Fatal(format!(
                        "initialSettings checkpoint could not be confirmed: {err}"
                    ))
                };
                live_thread
                    .append_items(&[RolloutItem::EventMsg(EventMsg::ThreadSettingsApplied(
                        ThreadSettingsAppliedEvent {
                            thread_id: Some(self.thread_id()),
                            thread_settings: snapshot,
                        },
                    ))])
                    .await
                    .map_err(persisted)?;
                live_thread.flush().await.map_err(persisted)?;
                let previous_config = self.build_effective_session_config(previous);
                let next_config = self.build_effective_session_config(&next);
                state.session_configuration = next.clone();
                state.last_started_turn_id = None;
                drop(state);
                drop(active_turn);
                self.emit_config_changed_contributors(Some(&previous_config), Some(&next_config));
            }
            (next, changed)
        };
        let selected_service_tier = configuration.step_settings.service_tier.clone();
        let snapshot = self
            .effective_thread_config_snapshot(&configuration, configuration.environments.clone())
            .await;
        Ok((snapshot, selected_service_tier, changed))
    }

    /// Persist before publishing so a failed write cannot change the running catalog.
    pub(crate) async fn replace_dynamic_tools(
        &self,
        tools: Vec<codex_protocol::dynamic_tools::DynamicToolSpec>,
    ) -> CodexResult<()> {
        let _settings_guard = acquire_persistence_lock(self).await;
        if let Some(live_thread) = self.live_thread() {
            let config = self.get_config().await;
            if !config.allow_persistent_dynamic_tool_updates {
                return Err(CodexErr::UnsupportedOperation(
                    "The host has disabled dynamic tool updates for persistent threads.".into(),
                ));
            }
            let persistence_error = |error: ThreadStoreError| match error {
                ThreadStoreError::Unsupported { .. } => {
                    CodexErr::UnsupportedOperation(error.to_string())
                }
                _ => CodexErr::Fatal(format!("failed to replace dynamic tools: {error}")),
            };
            live_thread
                .append_items(&[RolloutItem::DynamicToolsUpdated {
                    thread_id: self.thread_id(),
                    tools: tools.clone(),
                }])
                .await
                .map_err(persistence_error)?;
            live_thread.flush().await.map_err(persistence_error)?;
        }
        let tools = Arc::from(tools);
        let mut state = self.state.lock().await;
        state.persisted_dynamic_tools = Arc::clone(&tools);
        state.session_configuration.dynamic_tools = tools;
        Ok(())
    }

    /// Captures and flushes current settings under the shared persistence permit.
    pub(crate) async fn checkpoint_thread_settings(&self) -> ThreadStoreResult<()> {
        let _settings_guard = acquire_persistence_lock(self).await;
        if let Some(live_thread) = self.live_thread() {
            live_thread
                .append_items(&[RolloutItem::EventMsg(applied_event(self).await)])
                .await?;
            live_thread.flush().await?;
        }
        Ok(())
    }
}

/// Applies standalone thread settings. The caller holds the persistence permit through notification.
#[expect(
    clippy::await_holding_invalid_type,
    reason = "the active-turn guard makes continuation invalidation atomic with turn admission"
)]
pub(super) async fn update(
    session: &Session,
    overrides: impl Into<WithTurnExtensionData<ThreadSettingsOverrides>>,
    reply_requested: bool,
) -> ConstraintResult<SessionSettingsCommit> {
    let mut updates = prepare_update(overrides, &session.services.selected_capability_roots);
    updates.host_acceptance_in_reply = reply_requested;
    let commit = session.update_settings(updates).await?;
    let active_turn = session.active_turn.lock().await;
    // A registered running task retains its lineage so it can be recovered after a daemon
    // restart. Reservations and finishing turns have no interrupted work to continue.
    if active_turn
        .as_ref()
        .is_none_or(|active_turn| active_turn.task.is_none())
    {
        session.state.lock().await.last_started_turn_id = None;
    }
    Ok(commit)
}

/// Converts protocol overrides into the internal settings update shape.
pub(super) fn prepare_update(
    overrides: impl Into<WithTurnExtensionData<ThreadSettingsOverrides>>,
    roots: &[SelectedCapabilityRoot],
) -> SessionSettingsUpdate {
    let WithTurnExtensionData {
        request: overrides,
        turn_extension_init,
    } = overrides.into();
    let ThreadSettingsOverrides {
        request_country,
        environments: environment_requests,
        runtime_workspace_roots,
        profile_workspace_roots,
        approval_policy,
        approvals_reviewer,
        sandbox_policy,
        permission_profile,
        active_permission_profile,
        windows_sandbox_level,
        model,
        effort,
        summary,
        service_tier,
        collaboration_mode,
        personality,
        disabled_plugin_ids,
    } = overrides;
    SessionSettingsUpdate {
        turn_extension_init,
        step_settings: StepSettingsUpdate {
            request_country,
            model,
            effort,
            collaboration_mode,
            reasoning_summary: summary,
            service_tier,
            personality,
            approval_policy,
            approvals_reviewer,
        },
        environments: environment_requests.map(|requests| requests.select(roots)),
        runtime_workspace_roots,
        profile_workspace_roots,
        sandbox_policy,
        permission_profile,
        active_permission_profile,
        windows_sandbox_level,
        disabled_plugin_ids,
        ..Default::default()
    }
}

/// Acquires the shared permit before capturing or changing persistent settings.
pub(super) async fn acquire_persistence_lock(session: &Session) -> SemaphorePermit<'_> {
    session
        .thread_settings_persistence
        .acquire()
        .await
        .unwrap_or_else(|_| unreachable!("thread settings persistence semaphore is never closed"))
}

/// Applies persistent settings and emits the resulting thread-owned snapshot.
pub(super) async fn apply_update(
    session: &Session,
    submission_id: String,
    updates: SessionSettingsUpdate,
) -> ConstraintResult<()> {
    let _settings_guard = acquire_persistence_lock(session).await;
    let commit = session.update_settings(updates).await?;
    emit_applied(session, submission_id, commit.snapshot).await;
    Ok(())
}

/// Emits the snapshot published by one successful settings update.
pub(super) async fn emit_applied(
    session: &Session,
    submission_id: String,
    snapshot: ThreadSettingsSnapshot,
) {
    let msg = EventMsg::ThreadSettingsApplied(ThreadSettingsAppliedEvent {
        thread_id: Some(session.thread_id()),
        thread_settings: snapshot,
    });
    session
        .send_event_raw_without_materializing_rollout(Event {
            id: submission_id,
            msg,
        })
        .await;
}

/// Builds a current thread-owned snapshot for storage checkpoints.
pub(super) async fn applied_event(session: &Session) -> EventMsg {
    EventMsg::ThreadSettingsApplied(ThreadSettingsAppliedEvent {
        thread_id: Some(session.thread_id()),
        thread_settings: session.thread_settings_snapshot().await,
    })
}
