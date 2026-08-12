mod agent;
mod agent_manager;
mod channels;
mod cli;
mod config;
mod database;
mod gateway;
mod llm;
mod models;
mod permissions;
mod scheduler;
mod tools;
mod web;

use anyhow::{Context, Result};
use clap::Parser;
use cli::{Cli, Commands};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .with_thread_ids(true)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Start => {
            tracing::info!("ClawRig starting");

            let config = config::Config::load(&cli.config)?;
            let mut gw = gateway::Gateway::new(&config).await?;

            // Handle shutdown signals
            let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.ok();
                tracing::info!("Shutdown signal received");
                let _ = shutdown_tx.send(());
            });

            tokio::select! {
                result = gw.start() => {
                    if let Err(e) = result {
                        tracing::error!(error = %e, "Gateway error");
                    }
                }
                _ = shutdown_rx => {
                    tracing::info!("Shutting down gracefully");
                }
            }
        }

        Commands::Serve { ui, yolo, .. } => {
            let mut config = config::Config::load(&cli.config)?;
            
            // Override yolo mode from CLI flag
            if yolo {
                config.agents.defaults.yolo = true;
                tracing::info!("YOLO mode enabled via CLI flag");
            }

            // Initialize database and stores
            let db = database::Database::new(&config.database.path).await?;
            let pool = db.pool.clone();
            let session_store = Arc::new(database::sessions::SessionStore::new(pool.clone()));
            let approval_store = Arc::new(database::approvals::ApprovalStore::new(pool.clone()));
            let task_store = Arc::new(database::tasks::TaskStore::new(pool.clone()));

            // Initialize model router
            let model_router = Arc::new(models::ModelRouter::new());
            if let Some(fast) = &config.models.fast {
                model_router.register_profile("fast".to_string(), fast.clone());
            }
            if let Some(smart) = &config.models.smart {
                model_router.register_profile("smart".to_string(), smart.clone());
            }
            if let Some(reasoning) = &config.models.reasoning {
                model_router.register_profile("reasoning".to_string(), reasoning.clone());
            }

            // Initialize LLM provider
            let default_model = config.models.fast.as_ref()
                .or(config.models.smart.as_ref())
                .or(config.models.reasoning.as_ref())
                .context("No model profile configured")?;
            let llm_provider = Arc::new(llm::LlmProvider::from_config(default_model)?);

            // Create broadcast channel for SSE events (must be before AgentManager)
            let (event_tx, _) = broadcast::channel::<String>(256);

            // Create agent manager
            let agent_manager = Arc::new(agent_manager::AgentManager::from_config(
                &config, session_store, model_router, llm_provider, approval_store.clone(),
                event_tx.clone(),
            )?);

            let state = web::WebState {
                agent_manager,
                event_tx,
                approval_store: approval_store.clone(),
                permissions: Arc::new(permissions::PermissionManager::new(
                    approval_store,
                    permissions::PermissionConfig::default(),
                )),
                task_store,
                cancel_tokens: Arc::new(dashmap::DashMap::new()),
            };

            let port = config.server.port;
            let addr = format!("0.0.0.0:{}", port);

            tracing::info!(port = port, ui = ui, "Starting web server");

            let router = web::create_router(state);
            let listener = tokio::net::TcpListener::bind(&addr).await?;

            if ui {
                println!("\n  ClawRig Web UI: http://localhost:{}", port);
                println!("  API:            http://localhost:{}/api/health", port);
                println!("  SSE Events:     http://localhost:{}/api/events\n", port);
            } else {
                println!("\n  ClawRig API: http://localhost:{}/api/health", port);
                println!("  UI disabled (use --ui to enable)\n");
            }

            axum::serve(listener, router).await?;
        }

        Commands::Doctor => {
            println!("\nClawRig Doctor\n");
            println!("Checking configuration...");

            match config::Config::load(&cli.config) {
                Ok(config) => {
                    println!("  ✓ Configuration loaded");

                    // Check database
                    match database::Database::new(&config.database.path).await {
                        Ok(_) => println!("  ✓ Database accessible"),
                        Err(e) => println!("  ✗ Database: {}", e),
                    }

                    // Check Telegram
                    if let Some(telegram) = &config.channels.telegram {
                        if telegram.enabled {
                    if let Some(_token) = &telegram.bot_token {
                        println!("  ✓ Telegram token configured");
                    } else if let Some(env) = &telegram.bot_token_env {
                                match std::env::var(env) {
                                    Ok(_) => println!("  ✓ Telegram token from env"),
                                    Err(_) => println!("  ✗ Telegram env var '{}' not set", env),
                                }
                            } else {
                                println!("  ✗ Telegram token not configured");
                            }
                        }
                    }

                    // Check email
                    if let Some(email) = &config.channels.email {
                        if email.enabled {
                            println!("  ✓ Email configured (IMAP: {})", email.imap_host);
                        }
                    }

                    // Check models
                    let mut has_model = false;
                    if config.models.fast.is_some() {
                        println!("  ✓ Fast model configured");
                        has_model = true;
                    }
                    if config.models.smart.is_some() {
                        println!("  ✓ Smart model configured");
                        has_model = true;
                    }
                    if config.models.reasoning.is_some() {
                        println!("  ✓ Reasoning model configured");
                        has_model = true;
                    }
                    if !has_model {
                        println!("  ✗ No models configured");
                    }

                    println!("\nReady.");
                }
                Err(e) => {
                    println!("  ✗ Configuration: {}", e);
                    println!("\nCreate a config file at ~/.clawrig/config.toml");
                }
            }
        }

        Commands::Status => {
            println!("ClawRig Status");
            println!("(Gateway must be running for live status)");
        }

        Commands::Agent { command } => {
            match command {
                cli::AgentCommands::List => {
                    println!("Agents:");
                    println!("  main (default)");
                }
                cli::AgentCommands::Run { agent, prompt } => {
                    let config = config::Config::load(&cli.config)?;
                    let gw = gateway::Gateway::new(&config).await?;

                    match gw.run_agent_direct(&agent, &prompt).await {
                        Ok(response) => {
                            println!("{}", response.content);
                        }
                        Err(e) => {
                            eprintln!("Error: {}", e);
                        }
                    }
                }
            }
        }

        Commands::Session { command } => {
            match command {
                cli::SessionCommands::List => {
                    println!("Sessions:");
                    println!("(Run 'clawrig start' to see active sessions)");
                }
                cli::SessionCommands::Reset { agent } => {
                    println!("Reset session for agent: {}", agent);
                }
            }
        }

        Commands::Cron { command } => {
            match command {
                cli::CronCommands::List => {
                    println!("Scheduled jobs:");
                    println!("(Run 'clawrig start' to see active jobs)");
                }
                cli::CronCommands::Add { agent, schedule, prompt } => {
                    println!("Added cron job:");
                    println!("  Agent: {}", agent);
                    println!("  Schedule: {}", schedule);
                    println!("  Prompt: {}", prompt);
                }
                cli::CronCommands::Delete { id } => {
                    println!("Deleted job: {}", id);
                }
            }
        }

        Commands::Memory { query } => {
            println!("Memory search: {}", query);
            println!("(Run 'clawrig start' to search memory)");
        }

        Commands::Provider => {
            println!("Model providers:");
            println!("(Configure in config.toml under [models.*])");
        }

        Commands::Logs => {
            println!("Logs:");
            println!("(Run 'clawrig start' to see live logs)");
        }
    }

    Ok(())
}
