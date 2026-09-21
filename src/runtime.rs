use crate::config::Config;
use crate::engine::{Decision, Engine, Rejection};
use std::sync::{Condvar, Mutex, MutexGuard};
use std::time::Instant;

pub struct Runtime {
    engine: Mutex<Engine>,
    changed: Condvar,
}

impl Default for Runtime {
    fn default() -> Self {
        Self {
            engine: Mutex::new(Engine::new(Config::default())),
            changed: Condvar::new(),
        }
    }
}

impl Runtime {
    pub fn map_error(
        &self,
        status: u16,
        failure: &serde_json::Value,
    ) -> Option<crate::error_mapping::RetryableError> {
        self.lock().map_error(status, failure)
    }

    fn lock(&self) -> MutexGuard<'_, Engine> {
        // Preserve a stopped state after a panic instead of admitting new work.
        self.engine.lock().unwrap_or_else(|poisoned| {
            let mut engine = poisoned.into_inner();
            engine.stop();
            engine
        })
    }

    pub fn begin(&self, id: &str) -> Result<(), Rejection> {
        self.lock().begin(id)
    }

    pub fn begin_child(&self, id: &str, parent: &str) -> Result<(), Rejection> {
        self.lock().begin_child(id, parent)
    }

    pub fn acquire(&self, id: &str, credential: &str) -> Result<(), Rejection> {
        let mut engine = self.lock();
        let mut decision = engine.start_attempt(id, credential, Instant::now());
        // This also wakes waiters on the credential used by the previous attempt.
        self.changed.notify_all();
        loop {
            match decision {
                Decision::Admit => {
                    self.changed.notify_all();
                    return Ok(());
                }
                Decision::Reject(reason) => {
                    self.changed.notify_all();
                    return Err(reason);
                }
                Decision::WaitUntil(deadline) => {
                    let wait = deadline.saturating_duration_since(Instant::now());
                    engine = match self.changed.wait_timeout(engine, wait) {
                        Ok((guard, _)) => guard,
                        Err(poisoned) => {
                            let (mut guard, _) = poisoned.into_inner();
                            guard.stop();
                            guard
                        }
                    };
                    decision = engine.poll(id, Instant::now());
                }
            }
        }
    }

    pub fn complete(&self, id: &str) {
        self.lock().complete(id);
        self.changed.notify_all();
    }

    pub fn configure(&self, config: Config) -> Result<(), String> {
        self.lock().reconfigure(config, Instant::now())?;
        self.changed.notify_all();
        Ok(())
    }

    pub fn stop(&self) {
        self.lock().stop();
        self.changed.notify_all();
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        self.lock().counts()
    }
}
