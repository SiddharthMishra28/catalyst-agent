use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Session {
    pub id: String,
    pub agent_id: String,
    pub channel: String,
    pub peer_id: String,
    pub thread_id: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
    pub compacted: bool,
    pub summary: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: String,
    pub session_id: String,
    pub role: String,
    pub content: String,
    pub attachments: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_calls: Option<String>,
    pub created_at: i64,
    pub tokens: Option<i32>,
}

pub struct SessionStore {
    pool: SqlitePool,
}

impl SessionStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn get_or_create(
        &self,
        agent_id: &str,
        channel: &str,
        peer_id: &str,
        thread_id: Option<&str>,
    ) -> Result<Session> {
        let thread = thread_id.unwrap_or("");
        let now = Utc::now().timestamp();

        // Try to find existing non-compacted session
        if let Some(row) = sqlx::query_as::<_, (String,)>(
            "SELECT id FROM sessions WHERE agent_id = ? AND channel = ? AND peer_id = ? AND thread_id = ? AND compacted = 0 ORDER BY updated_at DESC LIMIT 1"
        )
        .bind(agent_id)
        .bind(channel)
        .bind(peer_id)
        .bind(thread)
        .fetch_optional(&self.pool)
        .await?
        {
            let session = self.get(&row.0).await?.unwrap();
            return Ok(session);
        }

        // Create new session
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO sessions (id, agent_id, channel, peer_id, thread_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&id)
        .bind(agent_id)
        .bind(channel)
        .bind(peer_id)
        .bind(thread)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        tracing::info!(session_id = %id, agent = agent_id, channel = channel, "Session created");

        Ok(Session {
            id,
            agent_id: agent_id.to_string(),
            channel: channel.to_string(),
            peer_id: peer_id.to_string(),
            thread_id: thread_id.map(String::from),
            created_at: now,
            updated_at: now,
            compacted: false,
            summary: None,
        })
    }

    pub async fn get(&self, id: &str) -> Result<Option<Session>> {
        let row = sqlx::query_as::<_, (String, String, String, String, Option<String>, i64, i64, i32, Option<String>)>(
            "SELECT id, agent_id, channel, peer_id, thread_id, created_at, updated_at, compacted, summary FROM sessions WHERE id = ?"
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| Session {
            id: r.0,
            agent_id: r.1,
            channel: r.2,
            peer_id: r.3,
            thread_id: r.4,
            created_at: r.5,
            updated_at: r.6,
            compacted: r.7 != 0,
            summary: r.8,
        }))
    }

    pub async fn add_message(&self, msg: &Message) -> Result<()> {
        let now = Utc::now().timestamp();
        sqlx::query(
            "INSERT INTO messages (id, session_id, role, content, attachments, tool_call_id, tool_calls, created_at, tokens) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&msg.id)
        .bind(&msg.session_id)
        .bind(&msg.role)
        .bind(&msg.content)
        .bind(&msg.attachments)
        .bind(&msg.tool_call_id)
        .bind(&msg.tool_calls)
        .bind(msg.created_at)
        .bind(msg.tokens)
        .execute(&self.pool)
        .await?;

        // Update session timestamp
        sqlx::query("UPDATE sessions SET updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(&msg.session_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn get_messages(&self, session_id: &str, limit: i64) -> Result<Vec<Message>> {
        let rows = sqlx::query_as::<_, (String, String, String, String, Option<String>, Option<String>, Option<String>, i64, Option<i32>)>(
            "SELECT id, session_id, role, content, attachments, tool_call_id, tool_calls, created_at, tokens FROM messages WHERE session_id = ? ORDER BY created_at ASC LIMIT ?"
        )
        .bind(session_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| Message {
            id: r.0,
            session_id: r.1,
            role: r.2,
            content: r.3,
            attachments: r.4,
            tool_call_id: r.5,
            tool_calls: r.6,
            created_at: r.7,
            tokens: r.8,
        }).collect())
    }

    pub async fn count_messages(&self, session_id: &str) -> Result<i64> {
        let row = sqlx::query_as::<_, (i64,)>(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?"
        )
        .bind(session_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(row.0)
    }

    pub async fn append_summary(&self, session_id: &str, summary: &str) -> Result<()> {
        sqlx::query(
            "UPDATE sessions SET summary = CASE
                WHEN summary IS NULL OR summary = '' THEN ?
                ELSE summary || char(10) || char(10) || ?
             END,
             updated_at = ? WHERE id = ?"
        )
        .bind(summary)
        .bind(summary)
        .bind(Utc::now().timestamp())
        .bind(session_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn delete_oldest_messages(&self, session_id: &str, keep: i64) -> Result<()> {
        sqlx::query(
            "DELETE FROM messages WHERE session_id = ?1 AND id NOT IN (
                SELECT id FROM messages WHERE session_id = ?1
                ORDER BY created_at DESC, rowid DESC LIMIT ?2
            )"
        )
        .bind(session_id)
        .bind(keep)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn reset(&self, session_id: &str) -> Result<()> {
        let now = Utc::now().timestamp();
        sqlx::query("UPDATE sessions SET compacted = 1, updated_at = ? WHERE id = ?")
            .bind(now)
            .bind(session_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_by_agent(&self, agent_id: &str) -> Result<Vec<Session>> {
        let rows = sqlx::query_as::<_, (String, String, String, String, Option<String>, i64, i64, i32, Option<String>)>(
            "SELECT id, agent_id, channel, peer_id, thread_id, created_at, updated_at, compacted, summary FROM sessions WHERE agent_id = ? ORDER BY updated_at DESC"
        )
        .bind(agent_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| Session {
            id: r.0,
            agent_id: r.1,
            channel: r.2,
            peer_id: r.3,
            thread_id: r.4,
            created_at: r.5,
            updated_at: r.6,
            compacted: r.7 != 0,
            summary: r.8,
        }).collect())
    }
}
