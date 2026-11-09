use anyhow::{Context, ensure};
use clap::{Args, Parser, Subcommand, ValueEnum};
use engineering_challenge::{
    auth::Credentials,
    collector, discovery,
    exchange::{kalshi, polymarket},
    latency, pilot, replay,
    transport::ConnectOptions,
};
use std::{net::IpAddr, path::PathBuf, sync::Arc, time::Duration};

#[derive(Parser)]
#[command(about = "Orderbook collector: binary SBE on stdout, diagnostics on stderr")]
struct Cli {
    #[command(flatten)]
    collect: Collect,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Also the default without a subcommand.
    Collect(Box<Collect>),
    /// Ramp one read-only Kalshi subscription through bounded market-count steps.
    Capacity(Capacity),
    /// Compare identical Kalshi events on a reference and candidate socket.
    Latency(Latency),
    /// Replay a capture; audit enables strict periodic checkpoint checks.
    #[command(alias = "replay")]
    Verify {
        #[arg(long)]
        input: PathBuf,
        #[arg(long)]
        audit: Option<PathBuf>,
        #[arg(long, default_value_t = 250_000)]
        max_symbols: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Exchange {
    Kalshi,
    Polymarket,
}
impl Exchange {
    fn rest_url(self) -> &'static str {
        match self {
            Self::Kalshi => kalshi::REST_URL,
            Self::Polymarket => polymarket::REST_URL,
        }
    }
    fn ws_url(self) -> &'static str {
        match self {
            Self::Kalshi => kalshi::WS_URL,
            Self::Polymarket => polymarket::WS_URL,
        }
    }
    fn symbols_per_connection(self) -> usize {
        match self {
            Self::Kalshi => kalshi::DEFAULT_SYMBOLS_PER_CONNECTION,
            // Conservative default below the observed eight-asset dump pilot;
            // book depth varies and this is not an exchange capacity limit.
            Self::Polymarket => 1,
        }
    }
}

#[derive(Args)]
struct Auth {
    #[arg(long, env = "KALSHI_KEY_ID", hide_env_values = true)]
    key_id: Option<String>,
    #[arg(long, env = "KALSHI_KEY_ID_FILE")]
    key_id_file: Option<PathBuf>,
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    private_key: Option<PathBuf>,
}
impl Auth {
    fn load(self) -> anyhow::Result<Arc<Credentials>> {
        let id = match (self.key_id, self.key_id_file) {
            (Some(id), _) => id,
            (None, Some(path)) => std::fs::read_to_string(path)
                .context("cannot read Key ID file")?
                .trim()
                .to_owned(),
            _ => anyhow::bail!(
                "set KALSHI_KEY_ID or KALSHI_KEY_ID_FILE, and KALSHI_PRIVATE_KEY_PATH"
            ),
        };
        Ok(Arc::new(Credentials::load(
            id,
            &self.private_key.context("set KALSHI_PRIVATE_KEY_PATH")?,
        )?))
    }
}

#[derive(Args)]
struct Access {
    #[command(flatten)]
    auth: Auth,
    #[arg(long,default_value=kalshi::REST_URL)]
    rest_url: String,
    #[arg(long,default_value=kalshi::WS_URL)]
    ws_url: String,
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    #[arg(long)]
    symbols_file: Option<PathBuf>,
    #[arg(long)]
    remote_ip: Option<IpAddr>,
    #[arg(long,default_value="10m",value_parser=parse_duration)]
    max_runtime: Duration,
}
#[derive(Args)]
struct Capacity {
    #[command(flatten)]
    access: Access,
    #[arg(long, value_delimiter = ',', default_value = "100,500,1000,2000")]
    steps: Vec<usize>,
    #[arg(long, default_value_t = 5)]
    dwell_seconds: u64,
    #[arg(long, default_value_t = 45)]
    step_timeout_seconds: u64,
    #[arg(long, default_value_t = 1000)]
    command_gap_ms: u64,
    #[arg(long)]
    report: PathBuf,
    #[arg(long)]
    inventory_out: Option<PathBuf>,
}
#[derive(Args)]
struct Latency {
    #[command(flatten)]
    access: Access,
    #[arg(long, default_value_t = 3)]
    trials: usize,
    #[arg(long, default_value_t = 2)]
    warmup_seconds: u64,
    #[arg(long, default_value_t = 30)]
    trial_seconds: u64,
    #[arg(long, value_delimiter = ',')]
    candidate_ports: Vec<u16>,
    /// Open a fresh reference every trial instead of keeping reference A.
    #[arg(long)]
    fresh_reference: bool,
    #[arg(long, default_value_t = 50_000)]
    sample_cap: usize,
    #[arg(long)]
    report: PathBuf,
    #[arg(long)]
    samples: PathBuf,
}

#[derive(Args)]
struct Collect {
    #[command(flatten)]
    health: HealthArgs,
    #[command(flatten)]
    auth: Auth,
    /// Public Polymarket collection uses the CLOB/CTF v1 profile without credentials.
    #[arg(long, value_enum, default_value_t = Exchange::Kalshi)]
    exchange: Exchange,
    /// Override the selected exchange's default REST endpoint.
    #[arg(long)]
    rest_url: Option<String>,
    /// Override the selected exchange's default WebSocket endpoint.
    #[arg(long)]
    ws_url: Option<String>,
    /// Exact Kalshi tickers or v1 CLOB asset IDs; omitted follows the live registry.
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// Symbols per socket: Kalshi defaults to 2250, Polymarket provisionally to 1.
    #[arg(long)]
    symbols_per_connection: Option<usize>,
    /// Independent feeds per shard; 1 disables redundancy.
    #[arg(long, default_value_t = 2)]
    replicas: usize,
    #[arg(long, default_value_t = 2000)]
    replica_lag_ms: u64,
    /// Fault pilot: close one of this collector's active connections once.
    #[arg(long,value_parser=parse_duration)]
    disconnect_active_after: Option<Duration>,
    #[arg(long, default_value_t = 196)]
    max_connections: usize,
    #[arg(long, default_value_t = 220_500)]
    max_symbols: usize,
    #[arg(long, default_value = "-")]
    output: PathBuf,
    /// New JSONL file with snapshot reasons, without credentials.
    #[arg(long)]
    audit: Option<PathBuf>,
    #[arg(long, default_value_t = 60)]
    snapshot_seconds: u64,
    #[arg(long, default_value_t = 60)]
    flush_seconds: u64,
    #[arg(long,value_parser=parse_duration)]
    max_runtime: Option<Duration>,
    /// Write a new final membership/progress summary.
    #[arg(long)]
    summary: Option<PathBuf>,
    #[arg(long, default_value_t = 15)]
    discovery_seconds: u64,
    #[arg(long, default_value_t = 1800)]
    full_reconcile_seconds: u64,
    #[arg(long, default_value_t = 100)]
    command_interval_ms: u64,
    #[arg(long, default_value_t = 250)]
    command_batch: usize,
    #[arg(long)]
    source_port: Option<u16>,
    #[arg(long)]
    remote_ip: Option<IpAddr>,
}

#[derive(Args)]
struct HealthArgs {
    #[arg(long, default_value_t = 60)]
    health_window_seconds: u64,
    #[arg(long, default_value_t = 120)]
    health_warmup_seconds: u64,
    #[arg(long, default_value_t = 120)]
    health_cooldown_seconds: u64,
    /// Expected incremental data points per symbol/window; zero allows quiet markets.
    #[arg(long, default_value_t = 0)]
    health_expected_points: u64,
    #[arg(long, default_value_t = 5)]
    health_min_peer_points: u64,
    #[arg(long, default_value_t = 20)]
    health_min_peer_percent: u8,
    #[arg(long, default_value_t = 3)]
    health_max_resubscriptions: u32,
    /// Base interval between staggered shard renewals; replicas alternate. Zero disables.
    #[arg(long, default_value_t = 1800)]
    rotate_seconds: u64,
    #[arg(long, default_value_t = 30)]
    rotation_timeout_seconds: u64,
    #[arg(long, default_value_t = 30)]
    ack_timeout_seconds: u64,
}
impl HealthArgs {
    fn options(self) -> engineering_challenge::health::Options {
        engineering_challenge::health::Options {
            window: Duration::from_secs(self.health_window_seconds),
            warmup: Duration::from_secs(self.health_warmup_seconds),
            cooldown: Duration::from_secs(self.health_cooldown_seconds),
            expected_points: self.health_expected_points,
            minimum_peer_points: self.health_min_peer_points,
            minimum_peer_percent: self.health_min_peer_percent,
            max_resubscriptions: self.health_max_resubscriptions,
            rotation_interval: Duration::from_secs(self.rotate_seconds),
            rotation_timeout: Duration::from_secs(self.rotation_timeout_seconds),
            ack_timeout: Duration::from_secs(self.ack_timeout_seconds),
        }
    }
}

fn parse_duration(text: &str) -> Result<Duration, String> {
    let (digits, multiplier) = match text.as_bytes().last() {
        Some(b's') => (&text[..text.len() - 1], 1),
        Some(b'm') => (&text[..text.len() - 1], 60),
        Some(b'h') => (&text[..text.len() - 1], 3600),
        _ => (text, 1),
    };
    let seconds = digits
        .parse::<u64>()
        .ok()
        .and_then(|n| n.checked_mul(multiplier))
        .filter(|&n| n > 0)
        .ok_or("expected positive duration, e.g. 30s, 2m, 1h")?;
    Ok(Duration::from_secs(seconds))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Some(Command::Verify {
            input,
            audit,
            max_symbols,
        }) => {
            let report = replay::verify(&input, audit.as_deref(), max_symbols)?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            if audit.is_none() {
                eprintln!(
                    "without --audit, snapshot resets are unclassified; periodic checkpoints are not strictly verified"
                );
            }
            Ok(())
        }
        Some(Command::Collect(options)) => collect(*options).await,
        Some(Command::Capacity(options)) => capacity(options).await,
        Some(Command::Latency(options)) => latency_probe(options).await,
        None => collect(cli.collect).await,
    }
}

async fn collect(args: Collect) -> anyhow::Result<()> {
    ensure!(
        args.snapshot_seconds > 0 && args.flush_seconds > 0,
        "timer intervals must be positive"
    );
    let health = args.health.options();
    health.validate()?;
    let rest_url = args
        .rest_url
        .unwrap_or_else(|| args.exchange.rest_url().to_owned());
    let ws_url = args
        .ws_url
        .unwrap_or_else(|| args.exchange.ws_url().to_owned());
    let exchange = match args.exchange {
        Exchange::Kalshi => {
            ensure!(
                args.max_connections <= 196,
                "Kalshi socket budget leaves quota headroom at 196"
            );
            kalshi::configure(args.auth.load()?, &rest_url, args.max_symbols)?
        }
        Exchange::Polymarket => {
            polymarket::configure(&rest_url, args.max_symbols, args.symbols.clone())?
        }
    };
    let options = collector::Options {
        health,
        replicas: args.replicas,
        replica_lag: Duration::from_millis(args.replica_lag_ms),
        failover_test_after: args.disconnect_active_after,
        symbols: args.symbols,
        rest_url,
        ws_url,
        symbols_per_connection: args
            .symbols_per_connection
            .unwrap_or_else(|| args.exchange.symbols_per_connection()),
        max_connections: args.max_connections,
        max_symbols: args.max_symbols,
        output: if args.output == std::path::Path::new("-") {
            PathBuf::from("/dev/stdout")
        } else {
            args.output
        },
        audit: args.audit,
        snapshot_interval: Duration::from_secs(args.snapshot_seconds),
        flush_interval: Duration::from_secs(args.flush_seconds),
        registry: collector::RegistryOptions {
            poll_interval: Duration::from_secs(args.discovery_seconds),
            full_interval: Duration::from_secs(args.full_reconcile_seconds),
            command_interval: Duration::from_millis(args.command_interval_ms),
            command_batch: args.command_batch,
        },
        summary: args.summary,
        connect: ConnectOptions {
            source_port: args.source_port,
            remote_ip: args.remote_ip,
        },
    };
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let work = collector::run_with_exchange(options, exchange, stop, cancel.clone());
    tokio::pin!(work);
    let deadline = tokio::time::sleep(
        args.max_runtime
            .unwrap_or(Duration::from_secs(100 * 365 * 24 * 3600)),
    );
    tokio::pin!(deadline);
    tokio::select! {
        result=&mut work=>result,
        signal=tokio::signal::ctrl_c()=>{signal?;eprintln!("SIGINT: stopping producers and draining output");let _=cancel.send(true);work.await},
        _=&mut deadline=>{eprintln!("runtime limit: stopping producers and draining output");let _=cancel.send(true);work.await},
    }
}

fn new_json(path: &std::path::Path, value: &impl serde::Serialize) -> anyhow::Result<()> {
    let file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    serde_json::to_writer_pretty(file, value)?;
    Ok(())
}
fn symbols(mut inline: Vec<String>, file: Option<PathBuf>) -> anyhow::Result<Vec<String>> {
    if let Some(path) = file {
        inline.extend(
            std::fs::read_to_string(path)?
                .lines()
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
        );
    }
    inline.sort();
    inline.dedup();
    Ok(inline)
}
async fn bounded<T>(
    work: impl std::future::Future<Output = anyhow::Result<T>>,
    cancel: tokio::sync::watch::Sender<bool>,
    duration: Duration,
) -> anyhow::Result<T> {
    tokio::pin!(work);
    tokio::select! {
        r=&mut work=>r,
        signal=tokio::signal::ctrl_c()=>{signal?;let _=cancel.send(true);work.await},
        _=tokio::time::sleep(duration)=>{eprintln!("probe runtime cap reached");let _=cancel.send(true);work.await},
    }
}
async fn capacity(args: Capacity) -> anyhow::Result<()> {
    ensure!(!args.report.exists(), "report exists; choose a new path");
    let auth = args.access.auth.load()?;
    let mut tickers = symbols(args.access.symbols, args.access.symbols_file)?;
    let all = tickers.is_empty();
    let (cancel, mut stop) = tokio::sync::watch::channel(false);
    let work = async {
        let mut metadata = serde_json::Value::Null;
        if all {
            let client = discovery::Discovery::new(&args.access.rest_url, 250_000)?;
            let inventory = tokio::select! {_=collector::stopped(&mut stop)=>anyhow::bail!("cancelled during inventory"),r=client.scan(None)=>r?};
            metadata = serde_json::json!({"pages":inventory.pages,"scan_start_unix_seconds":inventory.watermark,"elapsed_seconds":inventory.elapsed.as_secs_f64()});
            tickers = inventory
                .instruments
                .into_iter()
                .filter(|m| m.eligible)
                .map(|m| m.symbol)
                .collect();
        }
        if let Some(path) = args.inventory_out {
            use std::io::Write;
            let mut f = std::io::BufWriter::new(
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)?,
            );
            for ticker in &tickers {
                writeln!(f, "{ticker}")?;
            }
            f.flush()?;
        }
        let unique = tickers.len();
        let report = pilot::run(
            pilot::Options {
                ws_url: args.access.ws_url,
                symbols: tickers,
                steps: args.steps,
                dwell: Duration::from_secs(args.dwell_seconds),
                step_timeout: Duration::from_secs(args.step_timeout_seconds),
                command_gap: Duration::from_millis(args.command_gap_ms),
                connect: ConnectOptions {
                    source_port: None,
                    remote_ip: args.access.remote_ip,
                },
            },
            &auth,
            stop,
        )
        .await?;
        let result = serde_json::json!({"schema_version":1,"inventory_scope":if all {"all_live_scan"}else{"explicit_subset"},"unique_regular_live_at_scan":if all {Some(unique)}else{None},
            "supplied_symbols":unique,"metadata":metadata,"pilot":report,
            "two_replica_connections_at_verified_size":if all && report.largest_verified>0 {Some(2*unique.div_ceil(report.largest_verified))}else{None},
            "provenance":provenance()});
        new_json(&args.report, &result)?;
        ensure!(
            report.clean_close && (report.failure.is_none() || report.rejection_code == Some(26)),
            "capacity pilot incomplete; inspect report"
        );
        Ok(())
    };
    bounded(work, cancel, args.access.max_runtime).await
}
fn provenance() -> serde_json::Value {
    use sha2::{Digest, Sha256};
    let command = |program: &str, args: &[&str]| {
        std::process::Command::new(program)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
    };
    let lock = std::fs::read("Cargo.lock")
        .ok()
        .map(|b| format!("{:x}", Sha256::digest(b)));
    serde_json::json!({"revision":command("git",&["rev-parse","HEAD"]),"dirty":command("git",&["status","--porcelain"]).map(|s|!s.is_empty()),
        "lockfile_sha256":lock,"rustc":command("rustc",&["--version"]),"kernel":command("uname",&["-srmo"]),
        "logical_cpus":std::thread::available_parallelism().ok().map(|n|n.get()),"debug_assertions":cfg!(debug_assertions)})
}
async fn latency_probe(args: Latency) -> anyhow::Result<()> {
    ensure!(
        !args.report.exists() && !args.samples.exists(),
        "probe artifacts exist; choose new paths"
    );
    let auth = args.access.auth.load()?;
    let options = latency::Options {
        persistent_reference: !args.fresh_reference,
        ws_url: args.access.ws_url,
        symbols: symbols(args.access.symbols, args.access.symbols_file)?,
        trials: args.trials,
        warmup: Duration::from_secs(args.warmup_seconds),
        duration: Duration::from_secs(args.trial_seconds),
        ports: args.candidate_ports,
        connect: ConnectOptions {
            source_port: None,
            remote_ip: args.access.remote_ip,
        },
        sample_cap: args.sample_cap,
        pending_cap: 100_000,
        pending_bytes: 32 << 20,
        ttl: Duration::from_secs(5),
    };
    let (cancel, stop) = tokio::sync::watch::channel(false);
    let report = bounded(
        latency::run(options, auth, stop),
        cancel,
        args.access.max_runtime,
    )
    .await?;
    use std::io::Write;
    let mut samples = std::io::BufWriter::new(
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&args.samples)?,
    );
    for trial in &report.trials {
        for sample in &trial.samples {
            serde_json::to_writer(
                &mut samples,
                &serde_json::json!({"trial":trial.index,"reference_generation":trial.reference_generation,"candidate_generation":trial.candidate_generation,"event":sample}),
            )?;
            writeln!(samples)?;
        }
    }
    samples.flush()?;
    new_json(
        &args.report,
        &serde_json::json!({"provenance":provenance(),"measurement":report}),
    )?;
    ensure!(
        report.trials.len() == args.trials
            && report.reference_failure.is_none()
            && report.trials.iter().all(|t| {
                t.failure.is_none() && matches!(t.stop_reason.as_str(), "duration" | "sample_cap")
            }),
        "probe has failed/incomplete trials; inspect artifacts"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn default_collect_and_bounded_duration_parser() {
        // Redundancy is the default, and the default socket budget must carry
        // the whole default symbol budget on every replica.
        let collect = Cli::try_parse_from(["collector"]).unwrap().collect;
        assert_eq!(collect.exchange, Exchange::Kalshi);
        assert!(collect.rest_url.is_none());
        assert!(collect.ws_url.is_none());
        assert!(collect.symbols_per_connection.is_none());
        assert_eq!(collect.replicas, 2);
        assert!(
            collect.max_connections / collect.replicas * collect.exchange.symbols_per_connection()
                >= collect.max_symbols
        );
        assert!(
            Cli::try_parse_from(["collector"])
                .unwrap()
                .command
                .is_none()
        );
        assert_eq!(parse_duration("2m").unwrap(), Duration::from_secs(120));
        for text in ["", "0", "-1", "18446744073709551615h"] {
            assert!(parse_duration(text).is_err());
        }
        assert!(Cli::try_parse_from(["collector", "verify", "--input", "capture.sbe"]).is_ok());
        assert!(matches!(
            Cli::try_parse_from(["collector", "replay", "--input", "capture.sbe"])
                .unwrap()
                .command,
            Some(Command::Verify { .. })
        ));
    }

    #[test]
    fn selected_exchange_resolves_defaults_and_preserves_explicit_overrides() {
        assert_eq!(Exchange::Kalshi.rest_url(), kalshi::REST_URL);
        assert_eq!(Exchange::Kalshi.ws_url(), kalshi::WS_URL);
        assert_eq!(Exchange::Kalshi.symbols_per_connection(), 2250);
        assert_eq!(Exchange::Polymarket.rest_url(), polymarket::REST_URL);
        assert_eq!(Exchange::Polymarket.ws_url(), polymarket::WS_URL);
        assert_eq!(Exchange::Polymarket.symbols_per_connection(), 1);
        for argv in [
            vec!["collector", "--exchange", "polymarket"],
            vec!["collector", "collect", "--exchange", "polymarket"],
        ] {
            let cli = Cli::try_parse_from(argv).unwrap();
            let collect = match cli.command {
                Some(Command::Collect(collect)) => *collect,
                None => cli.collect,
                _ => panic!("expected collection"),
            };
            assert_eq!(collect.exchange, Exchange::Polymarket);
            assert!(collect.rest_url.is_none());
            assert!(collect.ws_url.is_none());
            assert!(collect.symbols_per_connection.is_none());
        }
        let Some(Command::Collect(collect)) = Cli::try_parse_from([
            "collector",
            "collect",
            "--exchange",
            "polymarket",
            "--rest-url",
            "http://127.0.0.1:8080",
            "--ws-url",
            "ws://127.0.0.1:8081/ws/market",
            "--symbols-per-connection",
            "2",
            "--symbols",
            "000123,340282366920938463463374607431768211456",
        ])
        .unwrap()
        .command
        else {
            panic!("expected collect subcommand")
        };
        assert_eq!(collect.rest_url.as_deref(), Some("http://127.0.0.1:8080"));
        assert_eq!(
            collect.ws_url.as_deref(),
            Some("ws://127.0.0.1:8081/ws/market")
        );
        assert_eq!(collect.symbols_per_connection, Some(2));
        assert_eq!(
            collect.symbols,
            ["000123", "340282366920938463463374607431768211456"]
        );
    }

    #[test]
    fn native_probes_do_not_accept_an_exchange_option() {
        for argv in [
            vec![
                "collector",
                "capacity",
                "--exchange",
                "polymarket",
                "--report",
                "out.json",
            ],
            vec![
                "collector",
                "latency",
                "--exchange",
                "polymarket",
                "--report",
                "out.json",
                "--samples",
                "samples.jsonl",
            ],
        ] {
            assert!(Cli::try_parse_from(argv).is_err());
        }
        assert!(Cli::try_parse_from(["collector", "--exchange", "unknown"]).is_err());
    }
}
