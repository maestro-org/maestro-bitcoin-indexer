use clap::Parser;
use std::process;

mod console;
mod daemon;

#[derive(Parser)]
#[clap(name = "polyphony-xbt")]
#[clap(bin_name = "polyphony-xbt")]
#[clap(author, version, about, long_about = None)]
enum Polyphony {
    Daemon(daemon::Args),
}

#[tokio::main(flavor = "multi_thread", worker_threads = 3)]
async fn main() {
    let args = Polyphony::parse();

    let result = match args {
        Polyphony::Daemon(x) => daemon::run(&x).await,
    };

    if let Err(err) = &result {
        eprintln!("ERROR: {:#?}", err);
        process::exit(1);
    }

    process::exit(0);
}
