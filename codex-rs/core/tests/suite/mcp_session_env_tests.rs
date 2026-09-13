use super::*;

#[test_case("CODEX_THREAD_ID", vec!["CODEX_THREAD_ID", "CODEX_HOME"] ; "thread_is_session_owned")]
#[test_case("CODEX_HOME", vec!["CODEX_THREAD_ID", "CODEX_HOME"] ; "home_is_session_owned")]
#[test_case("CODEX_THREAD_ID", vec![] ; "unrequested_thread_is_unchanged")]
#[test_case("CODEX_HOME", vec!["CODEX_THREAD_ID"] ; "unrequested_home_is_unchanged")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_stdio_session_environment(
    variable: &str,
    requested: Vec<&str>,
) -> anyhow::Result<()> {
    skip_if_wine_exec!(
        Ok(()),
        "requires a Windows test_stdio_server in the Wine-exec environment"
    );
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let command = stdio_server_bin()?;
    let opted_in = requested.contains(&variable);
    let env_vars = requested.into_iter().map(McpServerEnvVar::from).collect();
    let fixture = test_codex()
        .with_config(move |config| {
            insert_mcp_server(
                config,
                "session_env",
                stdio_transport(
                    command,
                    Some(HashMap::from([
                        ("CODEX_THREAD_ID".to_string(), "stale".to_string()),
                        ("CODEX_HOME".to_string(), "stale".to_string()),
                    ])),
                    env_vars,
                ),
                // This connector intentionally runs on the app host, including
                // when the thread's execution environment is remote.
                TestMcpServerOptions::default(),
            );
        })
        .build_with_auto_env(&server)
        .await?;
    wait_for_mcp_server(&fixture.codex, "session_env").await?;

    let call_id = "read-session-env";
    mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_response_created("env-1"),
            responses::ev_function_call_with_namespace(
                call_id,
                "mcp__session_env",
                "echo",
                &json!({"message": "binding", "env_var": variable}).to_string(),
            ),
            responses::ev_completed("env-1"),
        ]),
    )
    .await;
    let response = mount_sse_once(
        &server,
        responses::sse(vec![
            responses::ev_assistant_message("env-done", "done"),
            responses::ev_completed("env-2"),
        ]),
    )
    .await;
    fixture
        .codex
        .start_or_steer_turn(read_only_user_turn(&fixture, "Read the connector binding."))
        .await?;
    let event = wait_for_event(&fixture.codex, |event| {
        matches!(event, EventMsg::McpToolCallEnd(_))
    })
    .await;
    let EventMsg::McpToolCallEnd(end) = event else {
        unreachable!("event guard guarantees McpToolCallEnd");
    };
    let expected = if !opted_in {
        "stale".to_string()
    } else if variable == "CODEX_THREAD_ID" {
        fixture.session_configured.thread_id.to_string()
    } else {
        fixture.config.codex_home.display().to_string()
    };
    assert_eq!(
        end.result.map_err(anyhow::Error::msg)?.structured_content,
        Some(json!({"echo": "ECHOING: binding", "env": expected}))
    );
    wait_for_event(&fixture.codex, |event| {
        matches!(event, EventMsg::TurnComplete(_))
    })
    .await;
    assert!(
        response
            .single_request()
            .function_call_output(call_id)
            .is_object()
    );
    Ok(())
}
