use std::time::Instant;

use crate::config;

pub struct LifetimeMeasure {
    name: String,
    t0: Instant,
}

impl LifetimeMeasure {
    pub fn log_lifetime(name: &str) -> Self {
        Self {
            name: name.to_string(),
            t0: Instant::now(),
        }
    }
}

impl Drop for LifetimeMeasure {
    fn drop(&mut self) {
        if config::get_log_perf() {
            tracing::debug!("{} took {:.2?}", self.name, self.t0.elapsed());
        }
    }
}
