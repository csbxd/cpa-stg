use crate::config::{Config, Policy};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

/// Subjects are opaque host identifiers, never upstream secrets or model names.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ScopeKey {
    Credential(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Rejection {
    QueueFull,
    QueueTimeout,
    RequestEnded,
    DuplicateWait,
    Capacity,
    Stopping,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Decision {
    Admit,
    WaitUntil(Instant),
    Reject(Rejection),
}

#[derive(Clone, Debug)]
enum Phase {
    Idle,
    Active(ScopeKey),
    Queued { key: ScopeKey, deadline: Instant },
}

struct Bucket {
    policy: Policy,
    tokens: f64,
    updated: Instant,
    active: usize,
    queue: VecDeque<String>,
}

impl Bucket {
    fn new(policy: Policy, now: Instant) -> Self {
        Self {
            tokens: policy.burst as f64,
            policy,
            updated: now,
            active: 0,
            queue: VecDeque::new(),
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        self.updated = now;
        if self.policy.requests_per_minute == 0 {
            self.tokens = self.policy.burst as f64;
        } else {
            self.tokens = (self.tokens + elapsed * self.policy.requests_per_minute as f64 / 60.0)
                .min(self.policy.burst as f64);
        }
    }

    fn available(&self) -> bool {
        (self.policy.max_concurrency == 0 || self.active < self.policy.max_concurrency)
            && (self.policy.requests_per_minute == 0 || self.tokens >= 1.0)
    }

    fn charge(&mut self) {
        if self.policy.requests_per_minute != 0 {
            self.tokens -= 1.0;
        }
        self.active += 1;
    }

    fn wake_at(&self, now: Instant, deadline: Instant) -> Instant {
        if self.policy.requests_per_minute != 0 && self.tokens < 1.0 {
            let seconds = (1.0 - self.tokens) * 60.0 / self.policy.requests_per_minute as f64;
            (now + Duration::from_secs_f64(seconds.max(0.000_000_001))).min(deadline)
        } else {
            deadline
        }
    }
}

/// All transitions run under Runtime's mutex; blocking waits release that mutex.
pub struct Engine {
    config: Config,
    requests: HashMap<String, Phase>,
    parents: HashMap<String, String>,
    children: HashMap<String, Vec<String>>,
    buckets: HashMap<ScopeKey, Bucket>,
    stopping: bool,
}

impl Engine {
    pub fn map_error(
        &self,
        status: u16,
        failure: &serde_json::Value,
    ) -> Option<crate::error_mapping::RetryableError> {
        if !self.config.enabled || self.stopping {
            return None;
        }
        self.config.error_mapping.map(status, failure)
    }

    pub fn new(config: Config) -> Self {
        Self {
            config,
            requests: HashMap::new(),
            parents: HashMap::new(),
            children: HashMap::new(),
            buckets: HashMap::new(),
            stopping: false,
        }
    }

    pub fn begin(&mut self, id: &str) -> Result<(), Rejection> {
        if self.stopping {
            return Err(Rejection::Stopping);
        }
        if self.requests.contains_key(id) {
            return Ok(());
        }
        if self.requests.len() >= self.config.max_tracked_requests {
            return Err(Rejection::Capacity);
        }
        self.requests.insert(id.to_owned(), Phase::Idle);
        Ok(())
    }

    pub fn begin_child(&mut self, id: &str, parent: &str) -> Result<(), Rejection> {
        // Registration and parent liveness must be checked under the same lock.
        // Late nested callbacks must not resurrect a canceled outer request.
        if id == parent || !self.requests.contains_key(parent) || self.requests.contains_key(id) {
            return Err(Rejection::RequestEnded);
        }
        self.begin(id)?;
        self.parents.insert(id.to_owned(), parent.to_owned());
        self.children
            .entry(parent.to_owned())
            .or_default()
            .push(id.to_owned());
        Ok(())
    }

    pub fn start_attempt(&mut self, id: &str, credential: &str, now: Instant) -> Decision {
        if self.stopping {
            return Decision::Reject(Rejection::Stopping);
        }
        match self.requests.get(id) {
            None => return Decision::Reject(Rejection::RequestEnded),
            Some(Phase::Queued { .. }) => return Decision::Reject(Rejection::DuplicateWait),
            _ => {}
        }
        // CPA invokes after-auth once per upstream attempt. Release the previous
        // attempt, including retries on the same credential, and charge again.
        self.detach(id);
        let policy = self.config.policy(credential);
        if !policy.enabled {
            return Decision::Admit;
        }
        let key = ScopeKey::Credential(credential.to_owned());
        if !self.buckets.contains_key(&key) {
            // Evict only fully refilled idle buckets; eviction must not erase debt.
            self.buckets.retain(|_, bucket| {
                bucket.refill(now);
                bucket.active != 0
                    || !bucket.queue.is_empty()
                    || bucket.tokens < bucket.policy.burst as f64
            });
            if self.buckets.len() >= self.config.max_credentials {
                return Decision::Reject(Rejection::Capacity);
            }
            self.buckets.insert(key.clone(), Bucket::new(policy, now));
        }
        let bucket = self.buckets.get_mut(&key).expect("bucket inserted");
        bucket.refill(now);
        if bucket.queue.is_empty() && bucket.available() {
            bucket.charge();
            self.requests.insert(id.to_owned(), Phase::Active(key));
            return Decision::Admit;
        }
        if bucket.queue.len() >= bucket.policy.max_queue {
            return Decision::Reject(Rejection::QueueFull);
        }
        let deadline = now + Duration::from_millis(bucket.policy.queue_timeout_ms);
        bucket.queue.push_back(id.to_owned());
        self.requests
            .insert(id.to_owned(), Phase::Queued { key, deadline });
        self.poll(id, now)
    }

    pub fn poll(&mut self, id: &str, now: Instant) -> Decision {
        if self.stopping {
            self.detach(id);
            return Decision::Reject(Rejection::Stopping);
        }
        let (key, deadline) = match self.requests.get(id) {
            Some(Phase::Queued { key, deadline }) => (key.clone(), *deadline),
            Some(_) => return Decision::Admit,
            None => return Decision::Reject(Rejection::RequestEnded),
        };
        if now >= deadline {
            self.detach(id);
            return Decision::Reject(Rejection::QueueTimeout);
        }
        let bucket = self.buckets.get_mut(&key).expect("queued bucket exists");
        bucket.refill(now);
        if !bucket.policy.enabled {
            self.detach(id);
            return Decision::Admit;
        }
        if bucket.queue.front().is_some_and(|front| front == id) && bucket.available() {
            bucket.queue.pop_front();
            bucket.charge();
            self.requests.insert(id.to_owned(), Phase::Active(key));
            Decision::Admit
        } else {
            // Only the head waits for a token. Other waiters wake when the head
            // changes or at their deadline, avoiding periodic wakeups behind it.
            let wake = if bucket.queue.front().is_some_and(|front| front == id) {
                bucket.wake_at(now, deadline)
            } else {
                deadline
            };
            Decision::WaitUntil(wake)
        }
    }

    fn detach(&mut self, id: &str) {
        let Some(phase) = self.requests.get_mut(id) else {
            return;
        };
        match std::mem::replace(phase, Phase::Idle) {
            Phase::Idle => {}
            Phase::Active(key) => {
                if let Some(bucket) = self.buckets.get_mut(&key) {
                    bucket.active -= 1;
                }
            }
            Phase::Queued { key, .. } => {
                if let Some(bucket) = self.buckets.get_mut(&key) {
                    bucket.queue.retain(|queued| queued != id);
                }
            }
        }
    }

    pub fn complete(&mut self, id: &str) {
        let mut pending = vec![id.to_owned()];
        while let Some(id) = pending.pop() {
            if let Some(children) = self.children.remove(&id) {
                pending.extend(children);
            }
            if let Some(parent) = self.parents.remove(&id) {
                if let Some(children) = self.children.get_mut(&parent) {
                    children.retain(|child| child != &id);
                }
            }
            self.detach(&id);
            self.requests.remove(&id);
        }
    }

    pub fn reconfigure(&mut self, config: Config, now: Instant) -> Result<(), String> {
        config.validate()?;
        if self.stopping {
            return Err("plugin is stopping".into());
        }
        for (key, bucket) in &mut self.buckets {
            bucket.refill(now);
            let ScopeKey::Credential(id) = key;
            let policy = config.policy(id);
            bucket.tokens = bucket.tokens.min(policy.burst as f64);
            bucket.policy = policy;
        }
        self.config = config;
        Ok(())
    }

    pub fn stop(&mut self) {
        self.stopping = true;
    }

    pub fn counts(&self) -> (usize, usize, usize) {
        (
            self.requests.len(),
            self.buckets.values().map(|b| b.active).sum(),
            self.buckets.values().map(|b| b.queue.len()).sum(),
        )
    }
}
