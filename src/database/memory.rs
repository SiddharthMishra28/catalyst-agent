use anyhow::Result;
use chrono::Utc;
use sqlx::SqlitePool;

#[derive(Debug, Clone)]
pub struct MemoryRecord {
    pub id: Option<i64>,
    pub agent_id: String,
    pub memory_type: String,
    pub content: String,
    pub source_session: Option<String>,
    pub importance: f64,
    pub confidence: f64,
    pub created_at: i64,
    pub accessed_at: Option<i64>,
    pub access_count: i32,
}

pub struct MemoryStore {
    pool: SqlitePool,
}

impl MemoryStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn write(
        &self,
        agent_id: &str,
        memory_type: &str,
        content: &str,
        source_session: Option<&str>,
        importance: f64,
    ) -> Result<i64> {
        // Check for duplicates
        if let Some(existing) = sqlx::query_scalar::<_, i64>(
            "SELECT id FROM memories WHERE agent_id = ? AND content = ? LIMIT 1"
        )
        .bind(agent_id)
        .bind(content)
        .fetch_optional(&self.pool)
        .await?
        {
            // Update existing
            let now = Utc::now().timestamp();
            sqlx::query(
                "UPDATE memories SET accessed_at = ?, access_count = access_count + 1, importance = MAX(importance, ?) WHERE id = ?"
            )
            .bind(now)
            .bind(importance)
            .bind(existing)
            .execute(&self.pool)
            .await?;
            return Ok(existing);
        }

        let now = Utc::now().timestamp();
        let result = sqlx::query(
            "INSERT INTO memories (agent_id, type, content, source_session, importance, confidence, created_at, accessed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(agent_id)
        .bind(memory_type)
        .bind(content)
        .bind(source_session)
        .bind(importance)
        .bind(0.5)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    pub async fn search(
        &self,
        agent_id: &str,
        query: &str,
        limit: i64,
    ) -> Result<Vec<MemoryRecord>> {
        let now = Utc::now().timestamp();

        // FTS5 search with BM25 ranking
        let rows = sqlx::query_as::<_, (i64, String, String, String, Option<String>, f64, f64, i64, Option<i64>, i32)>(
            "SELECT m.id, m.agent_id, m.type, m.content, m.source_session, m.importance, m.confidence, m.created_at, m.accessed_at, m.access_count
             FROM memories m
             JOIN memories_fts f ON m.id = f.rowid
             WHERE m.agent_id = ? AND memories_fts MATCH ?
             ORDER BY rank, m.importance DESC
             LIMIT ?"
        )
        .bind(agent_id)
        .bind(query)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        // Update access timestamps
        for row in &rows {
            sqlx::query("UPDATE memories SET accessed_at = ?, access_count = access_count + 1 WHERE id = ?")
                .bind(now)
                .bind(row.0)
                .execute(&self.pool)
                .await?;
        }

        Ok(rows.into_iter().map(|r| MemoryRecord {
            id: Some(r.0),
            agent_id: r.1,
            memory_type: r.2,
            content: r.3,
            source_session: r.4,
            importance: r.5,
            confidence: r.6,
            created_at: r.7,
            accessed_at: r.8,
            access_count: r.9,
        }).collect())
    }

    pub async fn get_recent(
        &self,
        agent_id: &str,
        limit: i64,
    ) -> Result<Vec<MemoryRecord>> {
        let rows = sqlx::query_as::<_, (i64, String, String, String, Option<String>, f64, f64, i64, Option<i64>, i32)>(
            "SELECT id, agent_id, type, content, source_session, importance, confidence, created_at, accessed_at, access_count
             FROM memories WHERE agent_id = ? ORDER BY created_at DESC LIMIT ?"
        )
        .bind(agent_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(|r| MemoryRecord {
            id: Some(r.0),
            agent_id: r.1,
            memory_type: r.2,
            content: r.3,
            source_session: r.4,
            importance: r.5,
            confidence: r.6,
            created_at: r.7,
            accessed_at: r.8,
            access_count: r.9,
        }).collect())
    }
}
