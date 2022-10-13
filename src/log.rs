use crate::env::{self, Env};

pub fn init_with_env() {
    match *env::ENV {
        Env::Dev => tracing_subscriber::fmt::init(),
        Env::Prod => tracing_subscriber::fmt::fmt().json().init(),
        Env::Stag => tracing_subscriber::fmt::fmt().json().init(),
    }
}
