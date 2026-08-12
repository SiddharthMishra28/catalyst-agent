use anyhow::Result;
use sqlx::SqlitePool;

pub async fn run(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            channel TEXT NOT NULL,
            peer_id TEXT NOT NULL,
            thread_id TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            compacted INTEGER DEFAULT 0,
            summary TEXT
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            session_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            attachments TEXT,
            tool_call_id TEXT,
            tool_calls TEXT,
            created_at INTEGER NOT NULL,
            tokens INTEGER,
            FOREIGN KEY (session_id) REFERENCES sessions(id)
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS memories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            agent_id TEXT NOT NULL,
            type TEXT NOT NULL,
            content TEXT NOT NULL,
            source_session TEXT,
            importance REAL DEFAULT 0.5,
            confidence REAL DEFAULT 0.5,
            created_at INTEGER NOT NULL,
            accessed_at INTEGER,
            access_count INTEGER DEFAULT 0
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE VIRTUAL TABLE IF NOT EXISTS memories_fts USING fts5(
            content,
            content='memories',
            content_rowid='id'
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            schedule TEXT NOT NULL,
            timezone TEXT,
            session_mode TEXT DEFAULT 'isolated',
            prompt TEXT NOT NULL,
            target_channel TEXT,
            target_peer TEXT,
            enabled INTEGER DEFAULT 1,
            next_run_at INTEGER,
            last_run_at INTEGER,
            created_at INTEGER NOT NULL
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS approvals (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            tool TEXT NOT NULL,
            arguments TEXT NOT NULL,
            arguments_hash TEXT NOT NULL,
            status TEXT DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            resolved_at INTEGER
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS pairings (
            id TEXT PRIMARY KEY,
            channel TEXT NOT NULL,
            peer_id TEXT NOT NULL,
            code_hash TEXT NOT NULL,
            expires_at INTEGER NOT NULL,
            approved_at INTEGER
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS agent_runs (
            id TEXT PRIMARY KEY,
            agent_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            provider TEXT,
            model TEXT,
            status TEXT DEFAULT 'running',
            started_at INTEGER NOT NULL,
            completed_at INTEGER,
            input_tokens INTEGER DEFAULT 0,
            output_tokens INTEGER DEFAULT 0,
            error TEXT
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS provider_state (
            provider TEXT PRIMARY KEY,
            available INTEGER DEFAULT 1,
            cooldown_until INTEGER,
            failures INTEGER DEFAULT 0,
            rate_limit_remaining INTEGER,
            last_check INTEGER
        )"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS session_summaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            summary TEXT NOT NULL,
            facts TEXT,
            created_at INTEGER NOT NULL,
            message_range_start INTEGER,
            message_range_end INTEGER
        )"
    ).execute(pool).await?;

    // Triggers for FTS5 sync
    sqlx::query(
        "CREATE TRIGGER IF NOT EXISTS memories_ai AFTER INSERT ON memories BEGIN
            INSERT INTO memories_fts(rowid, content) VALUES (new.id, new.content);
        END"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TRIGGER IF NOT EXISTS memories_ad AFTER DELETE ON memories BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content) VALUES('delete', old.id, old.content);
        END"
    ).execute(pool).await?;

    sqlx::query(
        "CREATE TRIGGER IF NOT EXISTS memories_au AFTER UPDATE ON memories BEGIN
            INSERT INTO memories_fts(memories_fts, rowid, content) VALUES('delete', old.id, old.content);
            INSERT INTO memories_fts(rowid, content) VALUES (new.id, new.content);
        END"
    ).execute(pool).await?;

    tracing::info!("Database migrations complete");
    Ok(())
}
