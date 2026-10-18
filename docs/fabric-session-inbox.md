# Session-owned Fabric inbox

`features.fabric_session_inbox = true` enables an experimental Fabric receiver
inside each root Codex session. It is disabled by default. Existing sessions
using the external native notifier keep their current delivery path.

Enable only with a Fabric kernel that advertises experimental capability
`io.nymphai/fabric-session-inbox` version 1 and acknowledges
`_meta.fabricDelivery = "session"` in its `proc_self` response. Deploy kernel
support first. Capability negotiation happens before any tool call: probing
an old kernel with tools could consume its legacy piggyback inbox. A kernel
without this support cannot activate the receiver. This feature does
not change credentials, authorize additional tools, or enable Fabric in child
agents.

The first rollout supports a configured, enabled HTTP `fabric` mount without
`http_headers_helper`. Built-in OAuth, bearer-token environment variables,
and other non-identity headers remain available. A dynamic header helper can
overwrite thread identity after connection setup, so such mounts retain the
legacy path until the runtime can fence those headers at the final transport
boundary. Identity header environment overrides are removed from the derived
session configuration; other environment headers are preserved.

The owning runtime materializes its thread ID into the Fabric connection's
transcript, incarnation, and name headers before connection creation and
caching. Concurrent threads therefore cannot share a Fabric identity, and
reconnecting the same thread can reattach its existing proc. The runtime calls
`proc_self` itself; it does not require a model turn or a native socket
verification challenge.

The receiver holds `msg_inbox` open through the session's own MCP connection.
The kernel wakes the read when a message arrives. Empty deadline responses
renew the held read; transport failures trigger bounded exponential reconnect
backoff. There is no separate inbox polling process or native turn injection.

Messages retain their IDs, sender, reply correlation, and reply expectation in
the receiving turn. They are external input, with `fabric_inbox` as the turn
trigger. Input uses Core's atomic `StartIfIdle` admission: it queues locally
during active turns and never steers them. Ordinary thread settings and
automatic-start restrictions continue to apply.

Fabric remains the durable queue. Reading, scheduling a turn, and subsequent
background tool calls do not acknowledge a message. The runtime acknowledges
only after the exact receiving turn completes without error and its terminal
event passes the rollout durability barrier. Receipt failures retry without
starting another receiving turn. A replacement proc identity cannot receive
the previous proc's receipts.

Interrupted or failed turns leave messages undelivered. They do not immediately
restart automatically; a later user turn allows queued work to resume. Process
loss also leaves the kernel's unacknowledged letters available for redelivery.
Delivery is at least once: a crash between completing a turn and acknowledging
its letters can present a letter again. Message IDs let consumers correlate
such retries.

Shutdown cancels the held read before the session tears down MCP. Root
session lifecycle events publish busy/idle state through the same connection.
The feature does not retire the external notifier or remove legacy hooks:
those remain necessary for sessions with the flag disabled. Rollout requires
idle wake-up, concurrent-session isolation, interruption, reconnect, and
receipt-ordering validation before changing the operator package or defaults.
