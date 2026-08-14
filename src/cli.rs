use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "clawrig", about = "A tiny self-hosted personal AI Gateway")]
pub struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "~/.clawrig/config.toml")]
    pub config: String,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the gateway
    Start,
    /// Start web server with PWA UI
    Serve {
        /// Port to listen on (defaults to config server.port)
        #[arg(short, long)]
        port: Option<u16>,
        /// Enable PWA UI
        #[arg(long, default_value = "true")]
        ui: bool,
        /// YOLO mode: auto-approve all tool calls (no confirmation)
        #[arg(long)]
        yolo: bool,
    },
    /// Run health checks
    Doctor,
    /// Show gateway status
    Status,
    /// List agents
    Agent {
        #[command(subcommand)]
        command: AgentCommands,
    },
    /// List sessions
    Session {
        #[command(subcommand)]
        command: SessionCommands,
    },
    /// Manage cron jobs
    Cron {
        #[command(subcommand)]
        command: CronCommands,
    },
    /// Search memory
    Memory {
        /// Search query
        query: String,
    },
    /// List providers
    Provider,
    /// Show logs
    Logs,
}

#[derive(Subcommand)]
pub enum AgentCommands {
    /// List all agents
    List,
    /// Run agent with a prompt
    Run {
        /// Agent name
        agent: String,
        /// Prompt to send
        prompt: String,
    },
}

#[derive(Subcommand)]
pub enum SessionCommands {
    /// List sessions
    List,
    /// Reset a session
    Reset {
        /// Agent name
        agent: String,
    },
}

#[derive(Subcommand)]
pub enum CronCommands {
    /// List cron jobs
    List,
    /// Add a cron job
    Add {
        /// Agent name
        agent: String,
        /// Cron schedule
        schedule: String,
        /// Prompt to execute
        prompt: String,
    },
    /// Delete a cron job
    Delete {
        /// Job ID
        id: String,
    },
}
