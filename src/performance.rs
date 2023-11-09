use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use tracing::debug;

use crate::env::ENV_CONFIG;

/// A wrapper around a Future which adds timing data.
#[pin_project]
pub struct Timed<Fut>
where
    Fut: Future,
{
    #[pin]
    inner: Fut,
    name: String,
    start: Option<Instant>,
}

impl<Fut> Future for Timed<Fut>
where
    Fut: Future,
{
    type Output = Fut::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        let this = self.project();
        let start = this.start.get_or_insert_with(Instant::now);

        match this.inner.poll(cx) {
            // If the inner future is still pending, this wrapper is still pending.
            Poll::Pending => Poll::Pending,

            // If the inner future is done, measure the elapsed time and finish this wrapper future.
            Poll::Ready(v) => {
                if ENV_CONFIG.log_perf {
                    let elapsed = start.elapsed();
                    debug!("{} took {:.2?}", this.name, elapsed);
                }
                Poll::Ready(v)
            }
        }
    }
}

pub trait TimedExt: Sized + Future {
    fn timed(self, name: &str) -> Timed<Self> {
        Timed {
            inner: self,
            name: name.to_string(),
            start: None,
        }
    }
}

// All futures can use the `.timed` method defined above
impl<F: Future> TimedExt for F {}
