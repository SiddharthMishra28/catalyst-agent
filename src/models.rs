use anyhow::Result;
use dashmap::DashMap;

use crate::config::ModelProfile;

#[derive(Debug, Clone)]
pub struct ProviderHealth {
    pub available: bool,
    pub cooldown_until: Option<std::time::Instant>,
    pub failures: u32,
}

#[derive(Debug, Clone)]
pub enum TaskClass {
    Chat,
    Coding,
    Research,
    Summarize,
    Extract,
    ToolUse,
    Planning,
}

pub struct ModelRouter {
    profiles: DashMap<String, ModelProfile>,
    health: DashMap<String, ProviderHealth>,
}

impl ModelRouter {
    pub fn new() -> Self {
        Self {
            profiles: DashMap::new(),
            health: DashMap::new(),
        }
    }

    pub fn register_profile(&self, name: String, profile: ModelProfile) {
        self.profiles.insert(name, profile);
    }

    pub fn select(&self, task_class: &TaskClass) -> Result<(String, ModelProfile)> {
        // For now, use simple selection based on task class
        let profile_name = match task_class {
            TaskClass::Chat | TaskClass::Summarize | TaskClass::Extract => "fast",
            TaskClass::Coding | TaskClass::Planning | TaskClass::Research => "smart",
            TaskClass::ToolUse => "smart",
        };

        // Try requested profile, fall back to any available
        if let Some(entry) = self.profiles.get(profile_name) {
            let profile = entry.clone();
            let provider = profile.provider.clone();

            if self.is_provider_healthy(&provider) {
                return Ok((profile_name.to_string(), profile));
            }
        }

        // Fallback: find any healthy provider
        for entry in self.profiles.iter() {
            let profile = entry.value().clone();
            if self.is_provider_healthy(&profile.provider) {
                return Ok((entry.key().clone(), profile));
            }
        }

        Err(anyhow::anyhow!("No healthy model providers available"))
    }

    pub fn select_by_name(&self, name: &str) -> Result<ModelProfile> {
        self.profiles
            .get(name)
            .map(|e| e.value().clone())
            .ok_or_else(|| anyhow::anyhow!("Model profile '{}' not found", name))
    }

    fn is_provider_healthy(&self, provider: &str) -> bool {
        match self.health.get(provider) {
            Some(health) => {
                if let Some(cooldown) = health.cooldown_until {
                    if std::time::Instant::now() < cooldown {
                        return false;
                    }
                }
                health.available
            }
            None => true,
        }
    }

    pub fn report_failure(&self, provider: &str, cooldown_secs: u64) {
        let mut health = self.health
            .entry(provider.to_string())
            .or_insert_with(|| ProviderHealth {
                available: true,
                cooldown_until: None,
                failures: 0,
            });

        health.failures += 1;
        health.cooldown_until = Some(std::time::Instant::now() + std::time::Duration::from_secs(cooldown_secs));

        if health.failures >= 5 {
            health.available = false;
        }

        tracing::warn!(
            provider = provider,
            failures = health.failures,
            "Provider failure recorded"
        );
    }

    pub fn report_success(&self, provider: &str) {
        self.health.insert(provider.to_string(), ProviderHealth {
            available: true,
            cooldown_until: None,
            failures: 0,
        });
    }

    pub fn list_profiles(&self) -> Vec<(String, ModelProfile)> {
        self.profiles.iter().map(|e| (e.key().clone(), e.value().clone())).collect()
    }
}
