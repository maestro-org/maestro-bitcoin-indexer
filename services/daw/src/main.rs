use clap::Parser;

mod args;
mod commands;

use args::{Cli, Commands::Delete, Commands::Write, DeleteArgs};
use commands::{handle_delete_cmd, handle_write_cmd};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Delete(delete_cmd) => handle_delete_cmd(cli.pd, cli.redis, delete_cmd).await?,
        Write(write_cmd) => handle_write_cmd(cli.pd, write_cmd).await?,
    }

    Ok(())
}
