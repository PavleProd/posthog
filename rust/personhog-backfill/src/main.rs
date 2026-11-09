use std::time::Duration;

use assignment_coordination::store::{EtcdStore, StoreConfig};
use axum::Router;
use clap::Parser;
use common_database::{get_pool_with_config, PoolConfig};
use common_kafka::config::KafkaConfig;
use common_metrics::setup_metrics_routes;
use envconfig::Envconfig;
use personhog_backfill::config::Config;
use personhog_backfill::scanner::{self, Run, TopicSink};
use personhog_coordination::changelog::{load_changelog_config, CHANGELOG_BOOT_POLL_INTERVAL};
use personhog_coordination::store::PersonhogStore;
use personhog_coordination::types::ClusterEndpoint;
use rdkafka::producer::Producer;
use tokio_util::sync::CancellationToken;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

common_alloc::used!();

const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

/// Loads the person properties already in Postgres into SlateDB through the backfill topic.
#[derive(clap::Parser)]
#[command(name = "personhog-backfill")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    /// Splits the source table into the key ranges a run scans.
    Plan(PlanArgs),
    /// Scans this worker's share of a planned run onto the backfill topic.
    Scan(ScanArgs),
}

#[derive(clap::Args)]
struct PlanArgs {
    #[arg(long)]
    run_id: String,
    #[arg(long, default_value_t = 5_000_000)]
    rows_per_range: i64,
}

#[derive(clap::Args)]
struct ScanArgs {
    /// The run `plan` planned; rerunning a finished range does nothing.
    #[arg(long)]
    run_id: String,
    /// This worker's number, such as an Indexed Job's completion index.
    #[arg(long)]
    index: usize,
    #[arg(long)]
    workers: usize,
    #[arg(long, default_value_t = 1000)]
    page_size: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .with_target(true)
                .with_thread_ids(true)
                .with_level(true),
        )
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();
    let cli = Cli::parse();
    let config = Config::init_from_env().expect("Invalid configuration");
    match cli.command {
        Command::Plan(args) => plan(&config, args).await,
        Command::Scan(args) => scan(&config, args).await,
    }
}

async fn plan(config: &Config, args: PlanArgs) -> Result<(), Box<dyn std::error::Error>> {
    let pool = pool(config)?;
    let object_store = personhog_property_store::object_store(&config.property_store)?;
    let run = Run::new(object_store, &config.property_store.prefix, &args.run_id)?;
    let plan = scanner::plan(&pool, &config.pg_target_table, args.rows_per_range).await?;
    run.save_plan(&plan).await?;
    tracing::info!(
        run_id = %args.run_id,
        table = %config.pg_target_table,
        ranges = plan.ranges.len(),
        "backfill planned"
    );
    Ok(())
}

async fn scan(config: &Config, args: ScanArgs) -> Result<(), Box<dyn std::error::Error>> {
    serve_metrics(config.metrics_port);
    let store = EtcdStore::connect_with_member_failover(StoreConfig {
        endpoints: config.etcd_endpoint_list(),
        prefix: config.etcd_prefix.clone(),
    })
    .await?;
    let store = PersonhogStore::new(store);
    let Some(loaded) = load_changelog_config(
        &store,
        CHANGELOG_BOOT_POLL_INTERVAL,
        &CancellationToken::new(),
    )
    .await?
    else {
        return Ok(());
    };
    if !loaded.draining.is_empty() {
        return Err("a changelog migration is draining; backfill after it finishes".into());
    }

    let producer = common_kafka::kafka_producer::create_kafka_producer(
        &changelog_kafka(&config.kafka, loaded.cluster.as_ref()),
        Unmonitored,
    )
    .await?;
    let partitions = {
        let producer = producer.clone();
        let topic = config.property_backfill_topic.clone();
        tokio::task::spawn_blocking(move || {
            producer
                .client()
                .fetch_metadata(Some(&topic), METADATA_TIMEOUT)
                .map(|metadata| {
                    metadata
                        .topics()
                        .iter()
                        .find(|t| t.name() == topic)
                        .map_or(0, |t| t.partitions().len() as u32)
                })
        })
        .await??
    };
    // The backfill writers' range assignor co-locates the two topics' partitions only when their counts match.
    if partitions != loaded.partitions {
        return Err(format!(
            "backfill topic {} has {partitions} partitions; the changelog has {}",
            config.property_backfill_topic, loaded.partitions
        )
        .into());
    }

    let pool = pool(config)?;
    let object_store = personhog_property_store::object_store(&config.property_store)?;
    let run = Run::new(object_store, &config.property_store.prefix, &args.run_id)?;
    let sink = TopicSink::new(
        producer,
        config.property_backfill_topic.clone(),
        loaded.partitions,
    );
    tracing::info!(
        run_id = %args.run_id,
        index = args.index,
        workers = args.workers,
        topic = %config.property_backfill_topic,
        "starting backfill worker"
    );
    let counts =
        scanner::run_worker(&pool, &sink, &run, args.index, args.workers, args.page_size).await?;
    tracing::info!(?counts, "backfill worker finished");
    Ok(())
}

fn pool(config: &Config) -> Result<sqlx::PgPool, Box<dyn std::error::Error>> {
    Ok(get_pool_with_config(
        &config.database_url,
        PoolConfig {
            max_connections: 2,
            pool_name: Some("personhog-backfill".to_string()),
            statement_timeout_ms: Some(300_000),
            ..Default::default()
        },
    )?)
}

fn serve_metrics(port: u16) {
    tokio::spawn(async move {
        let router = setup_metrics_routes(Router::new());
        let bind = format!("0.0.0.0:{port}");
        match tokio::net::TcpListener::bind(&bind).await {
            Ok(listener) => {
                tracing::info!("Metrics server listening on {bind}");
                if let Err(e) = axum::serve(listener, router).await {
                    tracing::warn!(error = %e, "metrics server stopped");
                }
            }
            Err(e) => tracing::warn!(error = %e, bind, "metrics server could not bind"),
        }
    });
}

fn changelog_kafka(base: &KafkaConfig, cluster: Option<&ClusterEndpoint>) -> KafkaConfig {
    match cluster {
        Some(cluster) => base.with_endpoint(&cluster.hosts, cluster.tls),
        None => base.clone(),
    }
}

#[derive(Clone)]
struct Unmonitored;

impl common_liveness::SyncLivenessReporter for Unmonitored {
    fn report_healthy(&self) {}
    fn report_unhealthy(&self) {}
}
