use clap::Parser;
use cmd::bins_run;
use cmd::bins_process_run;
use log::LevelFilter;
use tribbler::config::DEFAULT_CONFIG_LOCATION;
use tribbler::err::TribResult;

/// starts a number of backend servers using a given bin config file
#[derive(Parser, Debug)]
#[clap(name = "bins-back")]
struct Args {
    /// log level to use when starting the backends
    #[clap(short, long, default_value = "INFO")]
    log_level: LevelFilter,
    /// bin configuration file
    #[clap(short, long, default_value = DEFAULT_CONFIG_LOCATION)]
    cfg: String,
    /// addresses to send ready notifications to
    #[clap(short, long)]
    ready_addrs: Vec<String>,

    #[clap(long, default_value = "10")]
    recv_timeout: u64,
    #[clap(long, default_value = "0")]

    n: usize
}

#[tokio::main]
async fn main() -> TribResult<()> {
    let args = Args::parse();
    println!("running process");
    let pt = bins_process_run::ProcessType::Back;

    bins_process_run::main(
        pt,
        args.log_level,
        args.cfg,
        args.ready_addrs,
        args.n,
    )
    .await

}
