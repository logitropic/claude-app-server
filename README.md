# claude-app-server

`claude-app-server` is a Codex-style Rust app-server for the local Claude Code CLI.

It keeps the project structure and runtime flow close to Codex app-server, while the agent behavior follows the TypeScript Claude app-server reference: each turn launches `claude --print --output-format stream-json`, parses Claude Code stream events, and forwards JSON-RPC notifications to the client.

## Status

This is a standalone v1 implementation.

Implemented:

- Codex-style Rust workspace layout.
- `stdio://`, `ws://IP:PORT`, `unix://`, `unix://PATH`, and `off` transports.
- Codex-style WebSocket bearer authentication.
- TypeScript-compatible thread and turn behavior.
- In-memory thread state.
- Claude Code stream-json event mapping.

Not implemented yet:

- Persistent thread storage.
- MCP/app/plugin marketplace systems.
- Codex config manager, device key, analytics, remote control enrollment.
- Codex-specific command execution APIs.

## Requirements

- Rust stable with Cargo.
- Claude Code CLI installed and authenticated.

Check Claude Code:

```bash
claude --version
```

If needed, log in with Claude Code before running the server.

## Workspace Layout

```text
.
├── app-server
│   └── src
│       ├── main.rs
│       ├── lib.rs
│       ├── message_processor.rs
│       ├── claude_runner.rs
│       ├── outgoing_message.rs
│       ├── thread_state.rs
│       └── request_processors/
├── app-server-protocol
│   └── src
│       ├── jsonrpc_lite.rs
│       └── protocol/
└── app-server-transport
    └── src
        ├── outgoing_message.rs
        └── transport/
```

Crates:

- `claude-app-server-protocol`: JSON-RPC envelopes, request/response structs, notifications, thread/turn/item types.
- `claude-app-server-transport`: transport parsing, stdio, WebSocket, unix socket, connection events, outbound queue, WebSocket auth.
- `claude-app-server`: binary/runtime, message processor, request dispatch, in-memory state, Claude subprocess runner.

## Build

```bash
cargo build -p claude-app-server
```

The binary is written to:

```text
target/debug/claude-app-server
```

## Running

The CLI follows Codex app-server’s `--listen` style.

### stdio

```bash
cargo run -p claude-app-server -- --listen stdio://
```

`stdio://` is the default:

```bash
cargo run -p claude-app-server
```

### WebSocket

```bash
cargo run -p claude-app-server -- --listen ws://127.0.0.1:3284
```

The WebSocket transport is plain `ws://`. There is no QR code, pair key, or default TLS.

When binding to a non-loopback address without auth, the server logs a warning:

```bash
cargo run -p claude-app-server -- --listen ws://0.0.0.0:3284
```

### Unix Socket

Use the default app-server control socket path:

```bash
cargo run -p claude-app-server -- --listen unix://
```

Use an explicit socket path:

```bash
cargo run -p claude-app-server -- --listen unix:///tmp/claude-app-server.sock
```

### Off

Parse config and exit without starting a listener:

```bash
cargo run -p claude-app-server -- --listen off
```

### Claude Path

By default, the server resolves `claude` from `PATH`. You can pass an explicit binary:

```bash
cargo run -p claude-app-server -- \
  --claude-path /absolute/path/to/claude
```

## WebSocket Auth

WebSocket auth uses `Authorization: Bearer <token>`, matching Codex-style transport auth.

### Capability Token From File

```bash
printf 'super-secret-token\n' > /tmp/claude-app-server-token

cargo run -p claude-app-server -- \
  --listen ws://127.0.0.1:3284 \
  --ws-auth capability-token \
  --ws-token-file /tmp/claude-app-server-token
```

Clients must send:

```text
Authorization: Bearer super-secret-token
```

### Capability Token SHA-256

```bash
TOKEN='super-secret-token'
HASH="$(printf '%s' "$TOKEN" | shasum -a 256 | awk '{print $1}')"

cargo run -p claude-app-server -- \
  --listen ws://127.0.0.1:3284 \
  --ws-auth capability-token \
  --ws-token-sha256 "$HASH"
```

### Signed Bearer Token

```bash
printf 'at-least-32-bytes-shared-secret-here\n' > /tmp/claude-app-server-jwt-secret

cargo run -p claude-app-server -- \
  --listen ws://127.0.0.1:3284 \
  --ws-auth signed-bearer-token \
  --ws-shared-secret-file /tmp/claude-app-server-jwt-secret
```

Optional JWT validation flags:

- `--ws-issuer <issuer>`
- `--ws-audience <audience>`
- `--ws-max-clock-skew-seconds <seconds>`

## Protocol

Messages are JSON-RPC 2.0 objects. stdio and unix socket messages are newline-delimited JSON. WebSocket messages are JSON text frames.

### Initialize

Request:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"client":{"name":"demo","version":"1.0.0"},"cwd":"/tmp"}}
```

Response:

```json
{"jsonrpc":"2.0","id":1,"result":{"server":{"name":"claude-app-server","version":"0.1.1"},"capabilities":{"threads":["start","resume","fork"],"turns":["start","steer","interrupt"],"models":["claude-opus-4-6","claude-sonnet-4-6","claude-haiku-4-5"],"skills":["Read","Write","Edit","Bash","Glob","Grep","WebFetch","WebSearch","Task"]}}}
```

Notification:

```json
{"jsonrpc":"2.0","method":"initialized","params":{"server":"claude-app-server"}}
```

## Methods

Implemented methods:

| Method | Description |
| --- | --- |
| `initialize` | Initializes a client connection. |
| `thread/start` | Creates an in-memory thread. |
| `thread/resume` | Reads an in-memory thread created in this process. |
| `thread/fork` | Forks an existing Claude session after at least one completed turn. |
| `turn/start` | Starts a Claude Code turn. |
| `turn/steer` | Queues steer content for the active turn. |
| `turn/interrupt` | Cancels the active Claude process. |
| `approval/respond` | Updates the thread permission mode for later turns. |
| `model/list` | Lists Claude model ids advertised by the server. |
| `skills/list` | Lists Claude Code built-in tools. |
| `app/list` | Returns an empty app list. |

Both `snake_case` and `camelCase` params are accepted where the TypeScript reference accepts them, for example `thread_id` and `threadId`, `permission_mode` and `permissionMode`.

## Turns

`turn/start` accepts either plain content:

```json
{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"thread_id":"THREAD_ID","content":"Say hello"}}
```

or Codex-style text input:

```json
{"jsonrpc":"2.0","id":3,"method":"turn/start","params":{"threadId":"THREAD_ID","input":[{"type":"text","text":"Say hello"}]}}
```

The server returns immediately:

```json
{"jsonrpc":"2.0","id":3,"result":{"turn":{"id":"TURN_ID"}}}
```

Then it streams notifications until the turn completes or fails.

## Notifications

During a turn, the server may emit:

| Notification | Description |
| --- | --- |
| `turn/started` | The turn has started. |
| `item/progress` | Streaming text or thinking delta. |
| `item/created` | Finalized text, thinking, tool call, or tool result item. |
| `usage/update` | Token usage update from Claude stream events. |
| `turn/permission_denied` | Claude reported permission denials. |
| `turn/completed` | The turn completed or was interrupted. |
| `turn/failed` | The turn failed before completion. |

## Claude Invocation

Each turn spawns Claude Code with:

```text
claude --print --output-format stream-json --verbose --include-partial-messages \
  --permission-mode <mode> \
  --session-id <thread_id>
```

Later turns use:

```text
--resume <claude_session_id>
```

Forked first turns use:

```text
--resume <source_session_id> --fork-session
```

Supported permission modes:

- `default`
- `acceptEdits`
- `bypassPermissions`
- `dontAsk`

## Smoke Test

Basic stdio handshake:

```bash
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"client":{"name":"smoke","version":"1"}}}' \
  '{"jsonrpc":"2.0","id":2,"method":"model/list"}' \
  | cargo run -q -p claude-app-server -- --listen stdio://
```

Real Claude turn:

```bash
node <<'EOF'
const { spawn } = require('child_process');
const readline = require('readline');

const child = spawn('cargo', ['run', '-q', '-p', 'claude-app-server', '--', '--listen', 'stdio://'], {
  stdio: ['pipe', 'pipe', 'inherit']
});
const rl = readline.createInterface({ input: child.stdout });
let id = 0;
let threadId;

function send(method, params) {
  child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: ++id, method, params }) + '\n');
}

rl.on('line', line => {
  const msg = JSON.parse(line);
  console.log(msg);
  if (msg.id === 1) send('thread/start', { cwd: '/tmp', permission_mode: 'dontAsk' });
  if (msg.id === 2) {
    threadId = msg.result.thread.id;
    send('turn/start', {
      thread_id: threadId,
      content: 'Respond with exactly CLAUDE_APP_SERVER_SMOKE_OK and nothing else.'
    });
  }
  if (msg.method === 'turn/completed' || msg.method === 'turn/failed') {
    child.kill('SIGTERM');
  }
});

send('initialize', { client: { name: 'smoke', version: '1' } });
EOF
```

## Development

Run checks:

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Build:

```bash
cargo build -p claude-app-server
```

## npm Distribution

The npm package follows Codex-style native distribution:

- `@logitropic/claude-app-server` is a lightweight Node launcher.
- Platform binaries are optional dependency aliases published as platform-tagged versions of the same npm package.
- Native payloads live under `vendor/<target-triple>/claude-app-server/claude-app-server(.exe)`.

Stage packages from a populated vendor tree:

```bash
python3 scripts/stage_npm_packages.py \
  --release-version 0.1.1 \
  --package claude-app-server \
  --vendor-src dist/native/vendor \
  --output-dir dist/npm
```

For local testing on the current machine:

```bash
cargo build --release -p claude-app-server
TARGET_TRIPLE=aarch64-apple-darwin
PLATFORM_PACKAGE=claude-app-server-darwin-arm64
mkdir -p "dist/native/vendor/${TARGET_TRIPLE}/claude-app-server"
cp target/release/claude-app-server \
  "dist/native/vendor/${TARGET_TRIPLE}/claude-app-server/claude-app-server"
python3 scripts/build_npm_package.py \
  --package "${PLATFORM_PACKAGE}" \
  --release-version 0.1.1 \
  --vendor-src dist/native/vendor \
  --pack-output "dist/npm/${PLATFORM_PACKAGE}-0.1.1.tgz"
```

## Compatibility Notes

This Rust implementation was smoke-tested against the TypeScript reference for:

- handshake and discovery methods,
- thread lifecycle methods,
- fork-before-turn error behavior,
- real Claude turn streaming,
- notification sequence,
- text item output.

Known intentional differences:

- Server name is `claude-app-server` instead of `symphony-claude`.
- CLI uses Codex-style `--listen`; there is no TypeScript-style `start` subcommand.
- WebSocket auth uses bearer tokens; there is no QR code or `?key=` pair key.
