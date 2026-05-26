//! Dynamic tool-call history cells.

use super::*;
use codex_app_server_protocol::DynamicToolCallOutputContentItem;

#[derive(Debug)]
pub(crate) struct DynamicToolCallCell {
    call_id: String,
    namespace: Option<String>,
    tool: String,
    arguments: serde_json::Value,
    start_time: Instant,
    duration: Option<Duration>,
    success: Option<bool>,
    content_items: Option<Vec<DynamicToolCallOutputContentItem>>,
    animations_enabled: bool,
}

impl DynamicToolCallCell {
    pub(crate) fn new(
        call_id: String,
        namespace: Option<String>,
        tool: String,
        arguments: serde_json::Value,
        animations_enabled: bool,
    ) -> Self {
        Self {
            call_id,
            namespace,
            tool,
            arguments,
            start_time: Instant::now(),
            duration: None,
            success: None,
            content_items: None,
            animations_enabled,
        }
    }

    pub(crate) fn call_id(&self) -> &str {
        &self.call_id
    }

    pub(crate) fn complete(
        &mut self,
        duration: Duration,
        success: bool,
        content_items: Vec<DynamicToolCallOutputContentItem>,
    ) {
        self.duration = Some(duration);
        self.success = Some(success);
        self.content_items = Some(content_items);
    }

    fn render_content_item(item: &DynamicToolCallOutputContentItem, width: usize) -> String {
        match item {
            DynamicToolCallOutputContentItem::InputText { text } => {
                format_and_truncate_tool_result(text, TOOL_CALL_MAX_LINES, width)
            }
            DynamicToolCallOutputContentItem::InputImage { image_url } => {
                format_and_truncate_tool_result(
                    &format!("image: {image_url}"),
                    TOOL_CALL_MAX_LINES,
                    width,
                )
            }
        }
    }
}

impl HistoryCell for DynamicToolCallCell {
    fn display_lines(&self, width: u16) -> Vec<Line<'static>> {
        let mut lines: Vec<Line<'static>> = Vec::new();
        let status = self.success;
        let bullet = match status {
            Some(true) => "•".green().bold(),
            Some(false) => "•".red().bold(),
            None => activity_indicator(
                Some(self.start_time),
                MotionMode::from_animations_enabled(self.animations_enabled),
                ReducedMotionIndicator::StaticBullet,
            )
            .unwrap_or_else(|| "•".dim()),
        };
        let header_text = if status.is_some() {
            "Called"
        } else {
            "Calling"
        };

        let invocation_line = line_to_static(&format_dynamic_tool_invocation(
            self.namespace.clone(),
            self.tool.clone(),
            &self.arguments,
        ));
        let status_suffix = status.map(|success| {
            if success {
                Line::from(vec![" -> ".dim(), "success".green()])
            } else {
                Line::from(vec![" -> ".dim(), "failed".red()])
            }
        });

        let mut compact_spans = vec![bullet.clone(), " ".into(), header_text.bold(), " ".into()];
        let mut compact_header = Line::from(compact_spans.clone());
        let reserved = compact_header.width()
            + status_suffix
                .as_ref()
                .map(|suffix| suffix.width())
                .unwrap_or_default();
        let inline_invocation =
            invocation_line.width() <= (width as usize).saturating_sub(reserved);

        if inline_invocation {
            compact_header.extend(invocation_line.spans.clone());
            if let Some(suffix) = &status_suffix {
                compact_header.extend(suffix.spans.clone());
            }
            lines.push(compact_header);
        } else {
            compact_spans.pop(); // drop trailing space for standalone header
            if let Some(suffix) = &status_suffix {
                compact_spans.extend(suffix.spans.clone());
            }
            lines.push(Line::from(compact_spans));

            let opts = RtOptions::new((width as usize).saturating_sub(4))
                .initial_indent("".into())
                .subsequent_indent("    ".into());
            let wrapped = adaptive_wrap_line(&invocation_line, opts);
            let body_lines: Vec<Line<'static>> = wrapped.iter().map(line_to_static).collect();
            lines.extend(prefix_lines(body_lines, "  └ ".dim(), "    ".into()));
        }

        let mut detail_lines: Vec<Line<'static>> = Vec::new();
        let detail_wrap_width = (width as usize).saturating_sub(4).max(1);

        if let Some(items) = &self.content_items {
            for item in items {
                let text = Self::render_content_item(item, detail_wrap_width);
                for segment in text.split('\n') {
                    let line = Line::from(segment.to_string().dim());
                    let wrapped = adaptive_wrap_line(
                        &line,
                        RtOptions::new(detail_wrap_width)
                            .initial_indent("".into())
                            .subsequent_indent("    ".into()),
                    );
                    detail_lines.extend(wrapped.iter().map(line_to_static));
                }
            }
        }

        if !detail_lines.is_empty() {
            let initial_prefix: Span<'static> = if inline_invocation {
                "  └ ".dim()
            } else {
                "    ".into()
            };
            lines.extend(prefix_lines(detail_lines, initial_prefix, "    ".into()));
        }

        lines
    }

    fn raw_lines(&self) -> Vec<Line<'static>> {
        let header_text = if self.success.is_some() {
            "Called"
        } else {
            "Calling"
        };
        let mut header = format!(
            "{header_text} {}",
            plain_dynamic_tool_invocation(self.namespace.as_deref(), &self.tool, &self.arguments)
        );
        if let Some(success) = self.success {
            header.push_str(if success { " -> success" } else { " -> failed" });
        }

        let mut lines = vec![Line::from(header)];
        if let Some(items) = &self.content_items {
            for item in items {
                let text = Self::render_content_item(item, RAW_TOOL_OUTPUT_WIDTH);
                lines.extend(raw_lines_from_source(&text));
            }
        }

        lines
    }

    fn transcript_animation_tick(&self) -> Option<u64> {
        if !self.animations_enabled || self.success.is_some() {
            return None;
        }
        Some((self.start_time.elapsed().as_millis() / 50) as u64)
    }
}

pub(crate) fn new_active_dynamic_tool_call(
    call_id: String,
    namespace: Option<String>,
    tool: String,
    arguments: serde_json::Value,
    animations_enabled: bool,
) -> DynamicToolCallCell {
    DynamicToolCallCell::new(call_id, namespace, tool, arguments, animations_enabled)
}

fn format_dynamic_tool_invocation<'a>(
    namespace: Option<String>,
    tool: String,
    arguments: &serde_json::Value,
) -> Line<'a> {
    let invocation = plain_dynamic_tool_invocation(namespace.as_deref(), &tool, arguments);
    let args_start = invocation.find('(').unwrap_or(invocation.len());
    let args_end = invocation.len().saturating_sub(1);
    let mut invocation_spans: Vec<Span<'a>> = Vec::new();
    invocation_spans.push(invocation[..args_start].to_string().cyan());
    invocation_spans.push("(".into());
    invocation_spans.push(invocation[args_start + 1..args_end].to_string().dim());
    invocation_spans.push(")".into());
    invocation_spans.into()
}

fn plain_dynamic_tool_invocation(
    namespace: Option<&str>,
    tool: &str,
    arguments: &serde_json::Value,
) -> String {
    let args_str = serde_json::to_string(arguments).unwrap_or_else(|_| arguments.to_string());
    match namespace.filter(|namespace| !namespace.is_empty()) {
        Some(namespace) => format!("{namespace}.{tool}({args_str})"),
        None => format!("{tool}({args_str})"),
    }
}
