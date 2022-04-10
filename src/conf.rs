//! Configuration for the server binary

use config::Config;
use serde::Deserialize;

use super::storage::bitcask;

/// All configuration
#[derive(Deserialize)]
pub struct Configuration {
    /// Server configuration.
    pub net: crate::net::Config,
    /// Bitcask storage configuration.
    pub bitcask: bitcask::Config,
}

impl Configuration {
    /// Get the configuration from file.
    ///
    /// # Panics
    ///
    /// Panics if the configuration file is not valid
    pub fn get(name: &str) -> Result<Self, config::ConfigError> {
        let conf = Config::builder()
            .add_source(config::File::with_name(name))
            .add_source(
                config::Environment::with_prefix("OPAL")
                    .prefix_separator("__")
                    .separator("__"),
            )
            .build()?;
        conf.try_deserialize()
    }
}
