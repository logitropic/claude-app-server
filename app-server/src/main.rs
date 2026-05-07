use clap::Parser;
use claude_app_server::AppServerRuntimeOptions;
use claude_app_server::AppServerTransport;
use claude_app_server::AppServerWebsocketAuthArgs;
use claude_app_server::run_main_with_transport_options;

#[derive(Debug, Parser)]
#[command(version, about = "Codex-style app-server wrapper for Claude Code CLI")]
struct AppServerArgs {
    /// Transport endpoint URL. Supported values: `stdio://` (default), `unix://`,
    /// `unix://PATH`, `ws://IP:PORT`, or `off`.
    #[arg(
        long = "listen",
        value_name = "URL",
        default_value = AppServerTransport::DEFAULT_LISTEN_URL
    )]
    listen: AppServerTransport,

    /// Path to the Claude Code CLI binary. Defaults to resolving `claude` from PATH.
    #[arg(long = "claude-path", value_name = "PATH")]
    claude_path: Option<std::path::PathBuf>,

    /// Enable debug logging and Claude subprocess tracing.
    #[arg(long = "debug")]
    debug: bool,

    #[command(flatten)]
    auth: AppServerWebsocketAuthArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = AppServerArgs::parse();
    let auth = args.auth.try_into_settings()?;
    let runtime_options = AppServerRuntimeOptions {
        claude_path: args.claude_path,
        debug: args.debug,
    };
    run_main_with_transport_options(args.listen, auth, runtime_options).await
}
