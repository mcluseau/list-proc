use clap::{Parser, Subcommand};

mod client;
mod server;

#[derive(Parser)]
#[command()]
struct Cli {
    #[arg(short, long, default_value = "list-proc.sock")]
    socket_path: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Serve a list of items
    Serve {
        list_file: String,
        state_file: Option<String>,
    },
    /// Process a list of items distributed by a list-proc server
    Process {
        #[arg(long, default_value = "item")]
        env: String,
        cmd: String,
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
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
            list_file,
            state_file,
        } => server::serve(socket_path, list_file, state_file).await,
        Commands::Process { env, cmd, args } => client::process(socket_path, env, cmd, args).await,
    }
}
