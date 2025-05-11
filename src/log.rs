use tracing_subscriber::{fmt::format::FmtSpan, EnvFilter};

use crate::env::get_env_bool;

pub fn init() {
    let builder = tracing_subscriber::fmt().with_env_filter(EnvFilter::from_default_env());

    let builder = if get_env_bool("LOG_PERF").unwrap_or(false) {
        builder.with_span_events(FmtSpan::CLOSE)
    } else {
        builder
    };

    if get_env_bool("LOG_JSON").unwrap_or(false) {
        builder.json().init();
    } else {
        builder.init();
    };
}
