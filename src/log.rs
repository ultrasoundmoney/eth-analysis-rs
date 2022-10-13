use lazy_static::lazy_static;
use tracing_subscriber::{
    fmt::{self, Layer},
    prelude::__tracing_subscriber_SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

use crate::env::{self, Env};

lazy_static! {
    static ref PRETTY_PRINT: bool = env::get_env_bool("PRETTY_PRINT");
}

pub fn init_with_env() {
    if *PRETTY_PRINT {
        fmt::init();
    } else {
        match *env::ENV {
            Env::Dev => fmt::init(),
            Env::Prod => tracing_subscriber::registry()
                .with(Layer::default().json())
                .with(EnvFilter::from_default_env())
                .init(),
            Env::Stag => tracing_subscriber::registry()
                .with(Layer::default().json())
                .with(EnvFilter::from_default_env())
                .init(),
        }
    }
}
