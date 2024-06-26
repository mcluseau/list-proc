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
        #[arg(long = "state")]
        state_file: Option<String>,
        #[arg(long, default_value = "item")]
        env: String,
        list_file: String,
        cmd: String,
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
    /// Process a list of items distributed by a list-proc server
    Process {
        #[arg(short)]
        n_items: Option<u32>,
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
        Commands::Process { n_items } => client::process(socket_path, n_items).await,
    }
}
