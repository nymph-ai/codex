use super::*;
use pretty_assertions::assert_eq;
use serde_json::json;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;

/// A real loopback HTTP server. Notices arrive in a POST SSE response, through
/// the production HTTP transport and notification service, never an injected
/// generation or a direct call to the catalog refresh implementation.
struct CatalogHttpServer {
    url: String,
    tools: Arc<RwLock<Vec<Tool>>>,
    lists: Arc<AtomicUsize>,
    initializes: Arc<AtomicUsize>,
    fail_lists: Arc<AtomicBool>,
    notify_during_list: Arc<AtomicBool>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CatalogHttpServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl CatalogHttpServer {
    async fn start(tools: Vec<Tool>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let tools = Arc::new(RwLock::new(tools));
        let lists = Arc::new(AtomicUsize::new(0));
        let initializes = Arc::new(AtomicUsize::new(0));
        let fail_lists = Arc::new(AtomicBool::new(false));
        let notify_during_list = Arc::new(AtomicBool::new(false));
        let task = tokio::spawn({
            let tools = Arc::clone(&tools);
            let lists = Arc::clone(&lists);
            let initializes = Arc::clone(&initializes);
            let fail_lists = Arc::clone(&fail_lists);
            let notify_during_list = Arc::clone(&notify_during_list);
            async move {
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut bytes = Vec::new();
                    let header_end = loop {
                        let mut chunk = [0; 4096];
                        let count = socket.read(&mut chunk).await.unwrap();
                        assert!(count > 0);
                        bytes.extend_from_slice(&chunk[..count]);
                        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                            break end + 4;
                        }
                        assert!(bytes.len() < 64 * 1024);
                    };
                    let headers = String::from_utf8_lossy(&bytes[..header_end]).to_lowercase();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.strip_prefix("content-length:")
                                .map(|value| value.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    while bytes.len() < header_end + length {
                        let mut chunk = [0; 4096];
                        let count = socket.read(&mut chunk).await.unwrap();
                        assert!(count > 0);
                        bytes.extend_from_slice(&chunk[..count]);
                    }
                    assert!(headers.contains("authorization: bearer catalog-test"));
                    assert!(headers.contains("x-catalog-identity: original"));
                    let (status, content_type, body) = if !headers.starts_with("post ") {
                        (405, "application/json", String::new())
                    } else {
                        let request: serde_json::Value =
                            serde_json::from_slice(&bytes[header_end..header_end + length])
                                .unwrap();
                        let id = request["id"].clone();
                        match request["method"].as_str().unwrap() {
                            "initialize" => {
                                initializes.fetch_add(1, Ordering::SeqCst);
                                (200, "application/json", json!({"jsonrpc":"2.0", "id":id, "result":{
                                    "protocolVersion":"2025-06-18", "capabilities":{"tools":{"listChanged":true}},
                                    "serverInfo":{"name":"catalog-test", "version":"1"}
                                }}).to_string())
                            }
                            "notifications/initialized" => (202, "application/json", String::new()),
                            "tools/list" => {
                                lists.fetch_add(1, Ordering::SeqCst);
                                let result = if fail_lists.load(Ordering::Acquire) {
                                    json!({"jsonrpc":"2.0", "id":id, "error":{"code":-32603, "message":"temporary list failure"}})
                                } else {
                                    json!({"jsonrpc":"2.0", "id":id, "result":{"tools":*tools.read().await}})
                                };
                                if notify_during_list.swap(false, Ordering::AcqRel) {
                                    let notice = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n";
                                    (
                                        200,
                                        "text/event-stream",
                                        format!("{notice}event: message\ndata: {result}\n\n"),
                                    )
                                } else {
                                    (200, "application/json", result.to_string())
                                }
                            }
                            "tools/call" => {
                                let notice = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/tools/list_changed\"}\n\n";
                                let result =
                                    json!({"jsonrpc":"2.0", "id":id, "result":{"content":[]}});
                                (
                                    200,
                                    "text/event-stream",
                                    format!(
                                        "{}event: message\ndata: {result}\n\n",
                                        notice.repeat(20)
                                    ),
                                )
                            }
                            method => panic!("unexpected method {method}"),
                        }
                    };
                    let response = format!(
                        "HTTP/1.1 {status} OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.shutdown().await.unwrap();
                }
            }
        });
        Self {
            url,
            tools,
            lists,
            initializes,
            fail_lists,
            notify_during_list,
            task,
        }
    }

    async fn client(&self, name: &str) -> AsyncManagedClient {
        let client = Arc::new(
            RmcpClient::new_streamable_http_client(
                name,
                &self.url,
                Some("catalog-test".to_string()),
                Some(HashMap::from([(
                    "X-Catalog-Identity".to_string(),
                    "original".to_string(),
                )])),
                /*env_http_headers*/ None,
                OAuthCredentialsStoreMode::File,
                AuthKeyringBackendKind::default(),
                codex_exec_server::Environment::default_for_tests().get_http_client(),
                /*auth_provider*/ None,
            )
            .await
            .unwrap(),
        );
        client
            .initialize(
                InitializeRequestParams::new(
                    ClientCapabilities::default(),
                    Implementation::new("test", "1"),
                )
                .with_protocol_version(ProtocolVersion::V_2025_06_18),
                Some(Duration::from_secs(5)),
                Box::new(|_, _| async { Err(anyhow!("unexpected elicitation")) }.boxed()),
            )
            .await
            .unwrap();
        let tools = list_tools_for_client_uncached(
            name,
            /*is_codex_apps_mcp_server*/ false,
            "initial",
            &client,
            Some(Duration::from_secs(5)),
            100,
            /*server_instructions*/ None,
        )
        .await
        .unwrap();
        let managed = ManagedClient {
            client,
            server_info: create_test_server_info(name),
            tools,
            tool_timeout: Some(Duration::from_secs(5)),
            server_instructions: None,
            server_supports_sandbox_state_meta_capability: false,
            codex_apps_tools_cache_context: None,
        };
        AsyncManagedClient {
            client: futures::future::ready(Ok(managed)).boxed().shared(),
            is_codex_apps_mcp_server: false,
            cached_server_info: None,
            codex_apps_tools_cache_context: None,
            tool_catalog_cache_context: None,
            startup_complete: Arc::new(AtomicBool::new(true)),
            startup_reconnect: None,
            cancel_token: CancellationToken::new(),
        }
    }
}

#[tokio::test]
async fn http_notifications_refresh_model_bindings_and_preserve_server_scope() {
    let old = create_test_tool("fabric", "old").tool;
    let same = create_test_tool("fabric", "same").tool;
    let server = CatalogHttpServer::start(vec![old, same.clone()]).await;
    let other = CatalogHttpServer::start(vec![create_test_tool("other", "untouched").tool]).await;
    let mut manager = McpConnectionSet::empty(/*prefix_mcp_tool_names*/ true);
    manager.insert_test_client("fabric", server.client("fabric").await);
    manager.insert_test_client("other", other.client("other").await);
    manager
        .servers
        .get_mut("fabric")
        .unwrap()
        .tool_filter
        .disabled
        .insert("blocked".to_string());
    let manager = Arc::new(manager);
    let before = capture_binding(&manager).await;
    let before_tools = before.tools().to_vec();
    let transport = manager.test_client("fabric").ready_transport().unwrap();
    let mut changed = same;
    changed.description = Some("new descriptor".into());
    changed.input_schema = Arc::new(
        serde_json::from_value(json!({"type":"object", "properties":{"new":{"type":"string"}}}))
            .unwrap(),
    );
    changed.annotations = Some(rmcp::model::ToolAnnotations::new().read_only(true));
    *server.tools.write().await = vec![
        changed.clone(),
        create_test_tool("fabric", "added").tool,
        create_test_tool("fabric", "blocked").tool,
    ];
    transport
        .call_tool(
            "notify".to_string(),
            /*arguments*/ None,
            /*meta*/ None,
            Some(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while transport.tool_list_generation() != 20 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(manager.stable_catalog_revision().await, Some(1));
    let (after, concurrent) = tokio::join!(capture_binding(&manager), manager.list_all_tools());
    assert_eq!(after.tool_info("fabric", "same").unwrap().tool, changed);
    assert!(after.prepare_call("fabric", "added").is_some());
    assert!(after.prepare_call("fabric", "old").is_none());
    assert!(after.prepare_call("fabric", "blocked").is_none());
    assert!(
        concurrent
            .iter()
            .any(|tool| tool.server_name == "fabric" && tool.tool.name == "added")
    );
    assert_eq!(before.tools(), before_tools);
    assert_eq!(server.lists.load(Ordering::SeqCst), 2);
    assert_eq!(other.lists.load(Ordering::SeqCst), 1);
    assert_eq!(server.initializes.load(Ordering::SeqCst), 1);
    assert_eq!(other.initializes.load(Ordering::SeqCst), 1);
    assert!(
        before
            .prepare_call("fabric", "old")
            .unwrap()
            .call_with_preparation(
                /*requested_timeout*/ None,
                || async { Ok((None, None)) },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("catalog changed")
    );

    // A failed refresh must neither publish a partial catalog nor reuse old
    // read-only annotations. Retry is bounded and needs no additional notice.
    server.fail_lists.store(true, Ordering::Release);
    *server.tools.write().await = vec![create_test_tool("fabric", "recovered").tool];
    transport
        .call_tool(
            "notify".to_string(),
            /*arguments*/ None,
            /*meta*/ None,
            Some(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while transport.tool_list_generation() != 40 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(manager.stable_catalog_revision().await, None);
    let unavailable = capture_binding(&manager).await;
    assert!(
        unavailable
            .tools()
            .iter()
            .all(|tool| tool.server_name != "fabric")
    );
    assert_eq!(server.lists.load(Ordering::SeqCst), 3);
    assert!(
        after
            .prepare_call("fabric", "same")
            .unwrap()
            .call_with_preparation(
                /*requested_timeout*/ None,
                || async { Ok((None, None)) },
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("catalog changed")
    );
    server.fail_lists.store(false, Ordering::Release);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let recovered = capture_binding(&manager).await;
    assert!(recovered.prepare_call("fabric", "recovered").is_some());
    assert_eq!(server.lists.load(Ordering::SeqCst), 4);

    // Identical descriptors still advance the captured generation, so the
    // runtime cannot keep returning a cached binding whose calls are stale.
    transport
        .call_tool(
            "notify".to_string(),
            /*arguments*/ None,
            /*meta*/ None,
            Some(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while transport.tool_list_generation() != 60 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(manager.stable_catalog_revision().await, Some(3));
    let identical = capture_binding(&manager).await;
    assert_eq!(identical.tools(), recovered.tools());
    assert_eq!(server.lists.load(Ordering::SeqCst), 5);
    assert_eq!(other.lists.load(Ordering::SeqCst), 1);
    assert_eq!(server.initializes.load(Ordering::SeqCst), 1);
    assert_eq!(other.initializes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_notice_during_list_is_not_acknowledged_by_that_list() {
    let server = CatalogHttpServer::start(vec![create_test_tool("fabric", "same").tool]).await;
    let mut manager = McpConnectionSet::empty(/*prefix_mcp_tool_names*/ true);
    manager.insert_test_client("fabric", server.client("fabric").await);
    let mut config = crate::mcp::tests::test_mcp_config(std::env::temp_dir());
    let mut catalog = crate::ResolvedMcpCatalog::builder();
    catalog.register(crate::McpServerRegistration::from_plugin(
        "fabric".to_string(),
        crate::McpPluginAttribution::new("fabric@test".to_string(), "Fabric".to_string()),
        /*plugin_order*/ 0,
        serde_json::from_value(json!({"url": server.url})).unwrap(),
    ));
    config.mcp_server_catalog = catalog.build();
    manager.tool_plugin_provenance = Arc::new(crate::tool_plugin_provenance(&config));
    let manager = Arc::new(manager);
    let before = capture_binding(&manager).await;
    let expected = before.tools().to_vec();
    assert_eq!(expected[0].plugin_display_names, vec!["Fabric".to_string()]);
    let transport = manager.test_client("fabric").ready_transport().unwrap();
    server.notify_during_list.store(true, Ordering::Release);
    transport
        .call_tool(
            "notify".to_string(),
            /*arguments*/ None,
            /*meta*/ None,
            Some(Duration::from_secs(5)),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while transport.tool_list_generation() != 20 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager.stable_catalog_revision().await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while transport.tool_list_generation() != 21 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(manager.stable_catalog_revision().await, Some(2));
    let after = capture_binding(&manager).await;
    assert_eq!(after.tools(), expected);
    assert_eq!(server.lists.load(Ordering::SeqCst), 3);
    assert_eq!(server.initializes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_catalog_publication_waits_for_inflight_prepared_calls() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let server = CatalogHttpServer::start(vec![create_test_tool("fabric", "old").tool]).await;
        let mut manager = McpConnectionSet::empty(/*prefix_mcp_tool_names*/ true);
        manager.insert_test_client("fabric", server.client("fabric").await);
        let manager = Arc::new(manager);
        let before = capture_binding(&manager).await;
        let frozen = before.tools().to_vec();
        let prepared = before.prepare_call("fabric", "old").unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let call = tokio::spawn(async move {
            prepared
                .call_with_preparation(/*requested_timeout*/ None, || async {
                    entered_tx.send(()).unwrap();
                    release_rx.await.unwrap();
                    Ok((None, None))
                })
                .await
        });
        entered_rx.await.unwrap();
        *server.tools.write().await = vec![create_test_tool("fabric", "new").tool];
        let transport = manager.test_client("fabric").ready_transport().unwrap();
        transport
            .call_tool(
                "notify".to_string(),
                /*arguments*/ None,
                /*meta*/ None,
                Some(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        while transport.tool_list_generation() != 20 {
            tokio::task::yield_now().await;
        }
        let refresh = tokio::spawn({
            let manager = Arc::clone(&manager);
            async move { capture_binding(&manager).await }
        });
        while server.lists.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
        assert!(!refresh.is_finished());
        release_tx.send(()).unwrap();
        call.await.unwrap().unwrap();
        refresh.await.unwrap();
        while transport.tool_list_generation() != 40 {
            tokio::task::yield_now().await;
        }
        let after = capture_binding(&manager).await;
        assert!(after.prepare_call("fabric", "new").is_some());
        assert!(after.prepare_call("fabric", "old").is_none());
        assert_eq!(before.tools(), frozen);
        assert_eq!(server.initializes.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}
