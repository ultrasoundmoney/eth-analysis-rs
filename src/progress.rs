use std::time::{Duration, Instant};

pub struct Progress {
    name: String,
    started_at: Instant,
    pub work_done: u64,
    work_total: u64,
}

impl Progress {
    pub fn new(name: &str, work_todo: u64) -> Self {
        Self {
            name: name.to_owned(),
            started_at: Instant::now(),
            work_done: 0,
            work_total: work_todo,
        }
    }

    pub fn inc_work_done(&mut self) {
        self.work_done = self.work_done + 1;
    }

    pub fn add_work_done(&mut self, units: u64) {
        self.work_done = self.work_done + units;
    }

    #[allow(dead_code)]
    pub fn set_work_done(&mut self, units: u64) {
        self.work_done = units;
    }

    fn estimate_time_left(&self) -> Duration {
        let work_not_done = self.work_total - self.work_done;
        let not_done_to_done_ratio = work_not_done as f64 / self.work_done as f64;
        let seconds_since_start = Instant::now() - self.started_at;
        let eta_seconds = not_done_to_done_ratio * seconds_since_start.as_secs() as f64;

        Duration::from_secs(eta_seconds as u64)
    }

    pub fn get_progress_string(&self) -> String {
        let seconds_elapsed = format!("{:?}s", Instant::now() - self.started_at);

        format!(
            "{} {}/{} - {:.2}% started {} ago, eta: {:?}",
            self.name,
            self.work_done,
            self.work_total,
            self.work_done as f64 / self.work_total as f64,
            seconds_elapsed,
            self.estimate_time_left()
        )
    }
}
