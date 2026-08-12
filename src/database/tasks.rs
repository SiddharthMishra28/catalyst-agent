use anyhow::Result;
use chrono::Utc;
use serde::Serialize;
use sqlx::SqlitePool;

#[derive(Debug, Clone, Serialize)]
pub struct Task {
    pub run_id: String,
    pub agent: String,
    pub session_id: String,
    pub status: String,
    pub started_at: i64,
    pub completed_at: Option<i64>,
    pub error: Option<String>,
}

pub struct TaskStore {
    pool: SqlitePool,
}

impl TaskStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn create_run(&self, run_id: &str, agent: &str, session_id: &str) -> Result<()> {
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO agent_runs (id, agent_id, session_id, status, started_at) VALUES (?, ?, ?, 'running', ?)"
        )
        .bind(run_id)
        .bind(agent)
        .bind(session_id)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn finish(&self, run_id: &str, status: &str, error: Option<&str>) -> Result<()> {
        let now = Utc::now().timestamp();
        sqlx::query(
            "UPDATE agent_runs SET status = ?, completed_at = ?, error = ? WHERE id = ?"
        )
        .bind(status)
        .bind(now)
        .bind(error)
        .bind(run_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn complete_run(&self, run_id: &str) -> Result<()> {
        self.finish(run_id, "complete", None).await
    }

    pub async fn fail_run(&self, run_id: &str, error: &str) -> Result<()> {
        self.finish(run_id, "error", Some(error)).await
    }

    pub async fn cancel_run(&self, run_id: &str) -> Result<()> {
        self.finish(run_id, "cancelled", None).await
    }

    pub async fn get_run(&self, run_id: &str) -> Result<Option<Task>> {
        let row = sqlx::query_as::<_, (String, String, String, String, i64, Option<i64>, Option<String>)>(
            "SELECT id, agent_id, session_id, status, started_at, completed_at, error FROM agent_runs WHERE id = ?"
        )
        .bind(run_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Task {
            run_id: r.0,
            agent: r.1,
            session_id: r.2,
            status: r.3,
            started_at: r.4,
            completed_at: r.5,
            error: r.6,
        }))
    }

    pub async fn list_runs(&self, limit: i64) -> Result<Vec<Task>> {
        let rows = sqlx::query_as::<_, (String, String, String, String, i64, Option<i64>, Option<String>)>(
            "SELECT id, agent_id, session_id, status, started_at, completed_at, error FROM agent_runs ORDER BY started_at DESC LIMIT ?"
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| Task {
            run_id: r.0,
            agent: r.1,
            session_id: r.2,
            status: r.3,
            started_at: r.4,
            completed_at: r.5,
            error: r.6,
        }).collect())
    }
}
