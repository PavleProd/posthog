use common_kafka::config::KafkaConfig;
use envconfig::Envconfig;

#[derive(Envconfig, Clone)]
pub struct Config {
    #[envconfig(nested = true)]
    pub kafka: KafkaConfig,

    #[envconfig(nested = true)]
    pub property_store: personhog_property_store::PropertyStoreConfig,

    /// The topic the scan writes. It must have the changelog's partition count.
    pub property_backfill_topic: String,

    pub database_url: String,

    #[envconfig(default = "personhog_person_tmp")]
    pub pg_target_table: String,

    #[envconfig(default = "http://localhost:2379")]
    pub etcd_endpoints: String,

    #[envconfig(default = "/personhog/")]
    pub etcd_prefix: String,

    #[envconfig(default = "9102")]
    pub metrics_port: u16,
}

impl Config {
    pub fn etcd_endpoint_list(&self) -> Vec<String> {
        self.etcd_endpoints
            .split(',')
            .map(str::trim)
            .filter(|endpoint| !endpoint.is_empty())
            .map(str::to_string)
            .collect()
    }
}
