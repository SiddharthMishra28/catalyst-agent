use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;

pub struct ProviderUsageStore {
    pool: SqlitePool,
}

impl ProviderUsageStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn record_call(
        &self,
        provider: &str,
        model: &str,
        input_tokens: i64,
        output_tokens: i64,
        latency_ms: i64,
        success: bool,
        error: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO agent_runs (id, agent_id, session_id, provider, model, status, started_at, completed_at, input_tokens, output_tokens, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind("system")
        .bind("system")
        .bind(provider)
        .bind(model)
        .bind(if success { "completed" } else { "failed" })
        .bind(now - (latency_ms / 1000))
        .bind(now)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_provider_state(&self, provider: &str) -> Result<Option<(bool, Option<i64>, i32)>> {
        let row = sqlx::query_as::<_, (i32, Option<i64>, i32)>(
            "SELECT available, cooldown_until, failures FROM provider_state WHERE provider = ?"
        )
        .bind(provider)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| (r.0 != 0, r.1, r.2)))
    }

    pub async fn set_provider_cooldown(&self, provider: &str, cooldown_secs: i64) -> Result<()> {
        let now = Utc::now().timestamp();
        let cooldown_until = now + cooldown_secs;

        sqlx::query(
            "INSERT OR REPLACE INTO provider_state (provider, available, cooldown_until, failures, last_check) VALUES (?, 0, ?, (SELECT COALESCE(failures, 0) + 1 FROM provider_state WHERE provider = ?), ?)"
        )
        .bind(provider)
        .bind(cooldown_until)
        .bind(provider)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_provider_available(&self, provider: &str) -> Result<()> {
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT OR REPLACE INTO provider_state (provider, available, cooldown_until, failures, last_check) VALUES (?, 1, NULL, 0, ?)"
        )
        .bind(provider)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn is_provider_available(&self, provider: &str) -> Result<bool> {
        let now = Utc::now().timestamp();
        let row = sqlx::query_as::<_, (i32, Option<i64>)>(
            "SELECT available, cooldown_until FROM provider_state WHERE provider = ?"
        )
        .bind(provider)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((available, Some(cooldown))) if available != 0 && cooldown > now => Ok(false),
            Some((available, _)) => Ok(available != 0),
            None => Ok(true), // Unknown providers are assumed available
        }
    }
}
