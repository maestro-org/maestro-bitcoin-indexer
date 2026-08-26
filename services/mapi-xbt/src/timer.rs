use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use tracing::warn;

pub static SLOW_ENDPOINT_WARNING_DURATION_MS: Duration = Duration::from_millis(200);

#[derive(Debug)]
pub struct Timer {
    checkpoints: HashMap<String, Vec<Duration>>,
    start: Instant,
}

impl Timer {
    pub fn new() -> Self {
        Self {
            checkpoints: HashMap::new(),
            start: Instant::now(),
        }
    }

    pub fn checkpoint(&mut self, name: &str) {
        let elapsed = self.start.elapsed();

        self.checkpoints
            .entry(name.to_string())
            .and_modify(|x| x.push(elapsed))
            .or_insert(vec![elapsed]);

        self.start = Instant::now(); // Reset for the next checkpoint
    }

    pub fn total(&self) -> Duration {
        self.checkpoints
            .iter()
            .map(|(_, x)| x.iter().sum::<Duration>())
            .sum()
    }

    pub fn finish(self) {
        let total_duration = self.total();

        if total_duration >= SLOW_ENDPOINT_WARNING_DURATION_MS {
            warn!(
                "slow response: {total_duration:?} {:?}",
                self.checkpoints
                    .iter()
                    .map(|(n, vs)| (n, vs.iter().sum::<Duration>()))
                    .collect::<Vec<_>>()
            )
        }
    }
}
