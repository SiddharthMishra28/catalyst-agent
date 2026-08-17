use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    #[serde(default)]
    pub agents: AgentsConfig,
    #[serde(default)]
    pub models: ModelsConfig,
    #[serde(default)]
    pub channels: ChannelsConfig,
    #[serde(default)]
    pub scheduler: SchedulerConfig,
    #[serde(default)]
    pub heartbeat: HeartbeatConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
}

fn default_host() -> String { "127.0.0.1".to_string() }
fn default_port() -> u16 { 18789 }

#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_path")]
    pub path: String,
}

fn default_db_path() -> String { "~/.clawrig/clawrig.db".to_string() }

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AgentsConfig {
    #[serde(default)]
    pub defaults: AgentDefaults,
    #[serde(default)]
    pub agents: Vec<AgentDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentDefaults {
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default)]
    pub yolo: bool,
}

impl Default for AgentDefaults {
    fn default() -> Self {
        Self { model: "smart".to_string(), yolo: false }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct AgentDef {
    pub name: String,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub tools: Vec<String>,
    #[serde(default)]
    pub bindings: Vec<BindingDef>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BindingDef {
    pub channel: String,
    #[serde(default = "default_account")]
    pub account: String,
    #[serde(default = "default_peer")]
    pub peer: String,
}

fn default_account() -> String { "default".to_string() }
fn default_peer() -> String { "*".to_string() }
fn default_model() -> String { "smart".to_string() }

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ModelsConfig {
    #[serde(default)]
    pub fast: Option<ModelProfile>,
    #[serde(default)]
    pub smart: Option<ModelProfile>,
    #[serde(default)]
    pub reasoning: Option<ModelProfile>,
    #[serde(default)]
    pub groq: Option<ModelProfile>,
    #[serde(default)]
    pub nvidia: Option<ModelProfile>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ModelProfile {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub top_p: Option<f32>,
    #[serde(default)]
    pub extra_body: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ChannelsConfig {
    #[serde(default)]
    pub telegram: Option<TelegramConfig>,
    #[serde(default)]
    pub email: Option<EmailConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TelegramConfig {
    pub enabled: bool,
    #[serde(default = "default_dm_policy")]
    pub dm_policy: String,
    #[serde(default)]
    pub bot_token_env: Option<String>,
    #[serde(default)]
    pub bot_token: Option<String>,
}

fn default_dm_policy() -> String { "pairing".to_string() }

#[derive(Debug, Clone, Deserialize)]
pub struct EmailConfig {
    pub enabled: bool,
    pub imap_host: String,
    pub imap_port: Option<u16>,
    pub smtp_host: String,
    pub smtp_port: Option<u16>,
    pub username_env: Option<String>,
    pub password_env: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SchedulerConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

fn default_true() -> bool { true }

#[derive(Debug, Clone, Deserialize)]
pub struct HeartbeatConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_interval")]
    pub interval: String,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self { enabled: true, interval: "30m".to_string() }
    }
}

fn default_interval() -> String { "30m".to_string() }

impl Config {
    pub fn load(path: &str) -> Result<Self> {
        let expanded = shellexpand::tilde(path);
        let content = std::fs::read_to_string(expanded.as_ref())
            .with_context(|| format!("Failed to read config: {}", path))?;
        let config: Config = toml::from_str(&content)
            .with_context(|| "Failed to parse config")?;
        Ok(config)
    }
}
