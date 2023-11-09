use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use crate::env::ENV_CONFIG;

pub fn init_with_env() {
    let builder = tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env());

    let builder = if ENV_CONFIG.log_perf {
        builder.with_span_events(FmtSpan::CLOSE)
    } else {
        builder
    };

    if ENV_CONFIG.log_json {
        builder.json().init();
    } else {
        builder.init();
    };
}
