use clap::{Parser, Subcommand};

mod client;
mod config;
mod server;
use config::Config;

#[derive(Parser)]
#[command()]
struct Cli {
    #[arg(short, long = "socket", default_value = "list-proc.sock")]
    socket_path: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Serve a list of items
    Serve {
        /// Path to the state file
        #[arg(long = "state")]
        state_file: Option<String>,
        /// Name of the envvar set to the current item
        #[arg(long, default_value = "item")]
        env: String,
        /// File used as a list
        list_file: String,
        /// Command to run
        cmd: String,
        /// Args to the command to run
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// Process a list of items distributed by a list-proc server
    Process {
        /// Number of items to process (default: unlimited)
        #[arg(short)]
        n_items: Option<u32>,
        /// Exit on error
        #[arg(short = 'e', default_value = "false")]
        exit_on_error: bool,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    if let None = std::env::var_os("RUST_LOG") {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let cli = Cli::parse();

    let socket_path = cli.socket_path;

    match cli.command {
        Commands::Serve {
            state_file,
            env,
            list_file,
            cmd,
            args,
        } => {
            let cfg = Config { env, cmd, args };
            server::serve(socket_path, list_file, state_file, cfg).await
        }
        Commands::Process {
            n_items,
            exit_on_error,
        } => client::process(socket_path, n_items, exit_on_error).await,
    }
}
