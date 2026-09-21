use serde::Deserialize;
use std::collections::HashMap;

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub enabled: bool,
    pub requests_per_minute: u32,
    pub burst: u32,
    pub max_concurrency: usize,
    pub max_queue: usize,
    pub queue_timeout_ms: u64,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            enabled: true,
            requests_per_minute: 60,
            burst: 1,
            max_concurrency: 2,
            max_queue: 100,
            queue_timeout_ms: 30_000,
        }
    }
}

impl Policy {
    fn validate(&self) -> Result<(), String> {
        if self.burst == 0 || self.burst > 1_000_000 {
            return Err("burst must be between 1 and 1000000".into());
        }
        if self.requests_per_minute > 60_000_000 {
            return Err("requests_per_minute exceeds 60000000".into());
        }
        if self.max_concurrency > 100_000 || self.max_queue > 100_000 {
            return Err("max_concurrency and max_queue must not exceed 100000".into());
        }
        if self.queue_timeout_ms == 0 || self.queue_timeout_ms > 300_000 {
            return Err("queue_timeout_ms must be between 1 and 300000".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CredentialPolicies {
    pub default: Policy,
    // Overrides are complete policies with the same defaults, not partial merges.
    pub overrides: HashMap<String, Policy>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    // These fields are supplied by the CPA host; routing uses the host's values.
    pub enabled: bool,
    pub priority: i64,
    pub store: Option<serde_json::Value>,
    pub max_tracked_requests: usize,
    pub max_credentials: usize,
    pub credentials: CredentialPolicies,
    pub error_mapping: crate::error_mapping::ErrorMapping,
    // Reserved for phase two. Non-empty policies fail validation explicitly.
    pub api_keys: HashMap<String, serde_json::Value>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            priority: 100,
            store: None,
            max_tracked_requests: 10_000,
            max_credentials: 10_000,
            credentials: CredentialPolicies::default(),
            error_mapping: crate::error_mapping::ErrorMapping::default(),
            api_keys: HashMap::new(),
        }
    }
}

impl Config {
    pub fn parse(yaml: &[u8]) -> Result<Self, String> {
        let cfg: Self = if yaml.is_empty() {
            Self::default()
        } else {
            // Do not include parser diagnostics: YAML may contain secrets.
            serde_yaml::from_slice(yaml).map_err(|_| "invalid policy YAML".to_string())?
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), String> {
        self.error_mapping.validate()?;
        if !self.api_keys.is_empty() {
            return Err("api_keys policies are reserved and not implemented in phase one".into());
        }
        if !(1..=100_000).contains(&self.max_tracked_requests)
            || !(1..=100_000).contains(&self.max_credentials)
        {
            return Err("tracking limits must be between 1 and 100000".into());
        }
        self.credentials.default.validate()?;
        if self.credentials.overrides.len() > self.max_credentials {
            return Err("too many credential overrides".into());
        }
        for (id, policy) in &self.credentials.overrides {
            if id.is_empty() || id.trim() != id || id.len() > 1024 {
                return Err("invalid credential override identifier".into());
            }
            policy.validate()?;
        }
        Ok(())
    }

    pub fn policy(&self, credential: &str) -> Policy {
        let mut policy = self
            .credentials
            .overrides
            .get(credential)
            .unwrap_or(&self.credentials.default)
            .clone();
        policy.enabled &= self.enabled;
        policy
    }
}
