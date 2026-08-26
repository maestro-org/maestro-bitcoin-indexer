use clap::Parser;
use serde::{Deserialize, Serialize};

/// Configuration for an optional external price service used to enrich responses with USD
/// values. When no base URL is configured, price lookups are skipped and USD fields in
/// responses are null.
#[derive(Debug, Clone, Parser, Serialize, Deserialize, PartialEq)]
pub struct Arranger {
    #[clap(long = "arranger-base-url", env = "ARRANGER_BASE_URL")]
    /// Base URL of the price service. If unset, USD price enrichment is disabled.
    pub base_url: Option<String>,

    #[clap(
        long = "arranger-sat-prices-path",
        default_value = "/markets/prices/batch",
        env = "ARRANGER_SAT_PRICES_PATH"
    )]
    pub sat_prices_path: String,

    #[clap(
        long = "arranger-rune-prices-path",
        default_value = "/_internal/prices/runes/batch",
        env = "ARRANGER_RUNE_PRICES_PATH"
    )]
    pub rune_prices_path: String,
}

impl Arranger {
    pub fn get_sat_prices_path(&self) -> Option<String> {
        self.base_url
            .as_ref()
            .map(|base_url| [base_url.clone(), self.sat_prices_path.clone()].concat())
    }

    pub fn get_rune_prices_path(&self) -> Option<String> {
        self.base_url
            .as_ref()
            .map(|base_url| [base_url.clone(), self.rune_prices_path.clone()].concat())
    }
}
