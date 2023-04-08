use std::sync::RwLock;

use chrono::{DateTime, Duration, Utc};

use crate::health::{HealthCheckable, HealthStatus};

pub struct ServeHealth {
    last_cache_update: RwLock<Option<DateTime<Utc>>>,
    started_on: DateTime<Utc>,
}

impl ServeHealth {
    pub fn new(started_on: DateTime<Utc>) -> Self {
        Self {
            last_cache_update: RwLock::new(None),
            started_on,
        }
    }

    pub fn set_cache_updated(&self) {
        *self.last_cache_update.write().unwrap() = Some(Utc::now());
    }
}

impl HealthCheckable for ServeHealth {
    // Cache is healthy if we have seen an update in the last five minutes, or it has been less
    // than 5 minutes since the server started.
    fn health_status(&self) -> HealthStatus {
        let now = Utc::now();
        let last_update = self
            .last_cache_update
            .read()
            .unwrap()
            .unwrap_or(self.started_on);
        let time_since_last_update = now - last_update;

        if time_since_last_update < Duration::minutes(5) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unhealthy(Some(format!(
                "cache has not been updated in {} seconds",
                time_since_last_update.num_seconds()
            )))
        }
    }
}
