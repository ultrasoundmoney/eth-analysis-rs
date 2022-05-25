use std::time::Instant;

pub struct Progress {
    done: u64,
    name: String,
    print_every: u64,
    total: u64,
    started_at: Option<Instant>,
}

impl Progress {
    pub fn new(total: u64, name: &str, print_every: u64) -> Self {
        Self {
            done: 0,
            name: name.to_string(),
            print_every,
            total,
            started_at: Option::None,
        }
    }

    pub fn step(&mut self) {
        self.done = self.done + 1;

        if self.started_at == None {
            self.started_at = Some(Instant::now());
        };

        if self.done != 0 && self.done % self.print_every == 0 {
            self.print_progress();
        };
    }

    fn print_progress(&self) {
        let seconds_elapsed = self.started_at.map_or_else(
            || String::from("??"),
            |started_at| format!("{}s", Instant::now().duration_since(started_at).as_secs()),
        );

        log::debug!(
            "{} {}/{} - {:.2}% - started {}s ago",
            self.name,
            self.done,
            self.total,
            self.done as f64 / self.total as f64,
            seconds_elapsed,
        )
    }
}
