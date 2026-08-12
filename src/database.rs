pub mod migrations;
pub mod sessions;
pub mod memory;
pub mod jobs;
pub mod approvals;
pub mod provider_usage;
pub mod tasks;

use anyhow::Result;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};

pub struct Database {
    pub pool: SqlitePool,
}

impl Database {
    pub async fn new(path: &str) -> Result<Self> {
        let expanded = shellexpand::tilde(path);
        let path_str = expanded.as_ref().to_string();

        if let Some(parent) = std::path::Path::new(&path_str).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let url = format!("sqlite:{}?mode=rwc", path_str);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await?;

        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool).await?;
        sqlx::query("PRAGMA busy_timeout=5000")
            .execute(&pool).await?;
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&pool).await?;

        let db = Self { pool };
        db.run_migrations().await?;
        Ok(db)
    }

    async fn run_migrations(&self) -> Result<()> {
        migrations::run(&self.pool).await
    }
}
