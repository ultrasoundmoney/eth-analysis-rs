use std::time::Instant;

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
        if std::env::var("LOG_PERF")
            .map_or(false, |log_perf_str| log_perf_str.to_lowercase() == "true")
        {
            tracing::debug!("{} took {:.2?}", self.name, self.t0.elapsed());
        }
    }
}
