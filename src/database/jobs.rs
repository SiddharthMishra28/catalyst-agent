use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Job {
    pub id: String,
    pub agent_id: String,
    pub schedule: String,
    pub timezone: Option<String>,
    pub session_mode: String,
    pub prompt: String,
    pub target_channel: Option<String>,
    pub target_peer: Option<String>,
    pub enabled: bool,
    pub next_run_at: Option<i64>,
    pub last_run_at: Option<i64>,
    pub created_at: i64,
}

pub struct JobStore {
    pool: SqlitePool,
}

impl JobStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn create(
        &self,
        agent_id: &str,
        schedule: &str,
        prompt: &str,
        timezone: Option<&str>,
        target_channel: Option<&str>,
        target_peer: Option<&str>,
    ) -> Result<Job> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().timestamp();

        sqlx::query(
            "INSERT INTO jobs (id, agent_id, schedule, timezone, prompt, target_channel, target_peer, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&id)
        .bind(agent_id)
        .bind(schedule)
        .bind(timezone)
        .bind(prompt)
        .bind(target_channel)
        .bind(target_peer)
        .bind(now)
        .execute(&self.pool)
        .await?;

        self.get(&id).await?.ok_or_else(|| anyhow::anyhow!("Failed to retrieve created job"))
    }

    pub async fn get(&self, id: &str) -> Result<Option<Job>> {
        let row = sqlx::query_as::<_, (String, String, String, Option<String>, String, String, Option<String>, Option<String>, i32, Option<i64>, Option<i64>, i64)>(
            "SELECT id, agent_id, schedule, timezone, session_mode, prompt, target_channel, target_peer, enabled, next_run_at, last_run_at, created_at FROM jobs WHERE id = ?"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Job {
            id: r.0,
            agent_id: r.1,
            schedule: r.2,
            timezone: r.3,
            session_mode: r.4,
            prompt: r.5,
            target_channel: r.6,
            target_peer: r.7,
            enabled: r.8 != 0,
            next_run_at: r.9,
            last_run_at: r.10,
            created_at: r.11,
        }))
    }

    pub async fn list_enabled(&self) -> Result<Vec<Job>> {
        let rows = sqlx::query_as::<_, (String, String, String, Option<String>, String, String, Option<String>, Option<String>, i32, Option<i64>, Option<i64>, i64)>(
            "SELECT id, agent_id, schedule, timezone, session_mode, prompt, target_channel, target_peer, enabled, next_run_at, last_run_at, created_at FROM jobs WHERE enabled = 1"
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| Job {
            id: r.0,
            agent_id: r.1,
            schedule: r.2,
            timezone: r.3,
            session_mode: r.4,
            prompt: r.5,
            target_channel: r.6,
            target_peer: r.7,
            enabled: r.8 != 0,
            next_run_at: r.9,
            last_run_at: r.10,
            created_at: r.11,
        }).collect())
    }

    pub async fn delete(&self, id: &str) -> Result<()> {
        sqlx::query("DELETE FROM jobs WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn update_last_run(&self, id: &str) -> Result<()> {
        let now = Utc::now().timestamp();
        sqlx::query("UPDATE jobs SET last_run_at = ? WHERE id = ?")
            .bind(now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
