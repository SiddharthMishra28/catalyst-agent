use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Approval {
    pub id: String,
    pub agent_id: String,
    pub session_id: String,
    pub tool: String,
    pub arguments: String,
    pub arguments_hash: String,
    pub status: String,
    pub created_at: i64,
    pub expires_at: i64,
    pub resolved_at: Option<i64>,
}

pub struct ApprovalStore {
    pool: SqlitePool,
}

impl ApprovalStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn request(
        &self,
        agent_id: &str,
        session_id: &str,
        tool: &str,
        arguments: &str,
        arguments_hash: &str,
        expires_in_secs: i64,
    ) -> Result<Approval> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().timestamp();
        let expires = now + expires_in_secs;

        sqlx::query(
            "INSERT INTO approvals (id, agent_id, session_id, tool, arguments, arguments_hash, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&id)
        .bind(agent_id)
        .bind(session_id)
        .bind(tool)
        .bind(arguments)
        .bind(arguments_hash)
        .bind(now)
        .bind(expires)
        .execute(&self.pool)
        .await?;

        self.get(&id).await?.ok_or_else(|| anyhow::anyhow!("Failed to create approval"))
    }

    pub async fn get(&self, id: &str) -> Result<Option<Approval>> {
        let row = sqlx::query_as::<_, (String, String, String, String, String, String, String, i64, i64, Option<i64>)>(
            "SELECT id, agent_id, session_id, tool, arguments, arguments_hash, status, created_at, expires_at, resolved_at FROM approvals WHERE id = ?"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Approval {
            id: r.0,
            agent_id: r.1,
            session_id: r.2,
            tool: r.3,
            arguments: r.4,
            arguments_hash: r.5,
            status: r.6,
            created_at: r.7,
            expires_at: r.8,
            resolved_at: r.9,
        }))
    }

    pub async fn approve(&self, id: &str) -> Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE approvals SET status = 'approved', resolved_at = ? WHERE id = ? AND status = 'pending'"
        )
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn deny(&self, id: &str) -> Result<bool> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE approvals SET status = 'denied', resolved_at = ? WHERE id = ? AND status = 'pending'"
        )
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn expire_pending(&self) -> Result<u64> {
        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "UPDATE approvals SET status = 'expired', resolved_at = ? WHERE status = 'pending' AND expires_at < ?"
        )
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }
}
