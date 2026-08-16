use anyhow::Result;
use chrono::Utc;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::database::jobs::{Job, JobStore};
use crate::agent::{AgentRequest, AgentRuntime};

pub struct Scheduler {
    job_store: Arc<JobStore>,
    agent_runtime: Arc<AgentRuntime>,
    outbound_tx: mpsc::Sender<crate::channels::OutboundMessage>,
}

impl Scheduler {
    pub fn new(
        job_store: Arc<JobStore>,
        agent_runtime: Arc<AgentRuntime>,
        outbound_tx: mpsc::Sender<crate::channels::OutboundMessage>,
    ) -> Self {
        Self { job_store, agent_runtime, outbound_tx }
    }

    pub async fn start(&self) -> Result<()> {
        tracing::info!("Scheduler started");

        loop {
            // Check for due jobs
            let jobs = self.job_store.list_enabled().await?;

            for job in &jobs {
                if self.is_due(job) {
                    if let Err(e) = self.execute_job(job).await {
                        tracing::error!(job_id = %job.id, error = %e, "Failed to execute scheduled job");
                    }
                }
            }

            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }

    fn is_due(&self, job: &Job) -> bool {
        let now = Utc::now().timestamp();

        if let Some(next_run) = job.next_run_at {
            return now >= next_run;
        }

        // Simple check: if never run and schedule exists
        if job.last_run_at.is_none() && !job.schedule.is_empty() {
            return true;
        }

        false
    }

    async fn execute_job(&self, job: &Job) -> Result<()> {
        tracing::info!(job_id = %job.id, agent = %job.agent_id, "Executing scheduled job");

        let request = AgentRequest {
            agent_id: job.agent_id.clone(),
            session_id: format!("cron:{}", job.id),
            channel: job.target_channel.clone().unwrap_or_else(|| "internal".to_string()),
            peer_id: job.target_peer.clone().unwrap_or_else(|| "scheduler".to_string()),
            content: job.prompt.clone(),
            attachments: Vec::new(),
            run_id: None,
            model_profile: None,
            cancel_token: None,
        };

        let response = self.agent_runtime.run(request).await?;

        // Deliver response if target specified
        if let (Some(channel), Some(peer)) = (&job.target_channel, &job.target_peer) {
            let outbound = crate::channels::OutboundMessage {
                channel: channel.clone(),
                target: peer.clone(),
                reply_to: None,
                text: response.content,
                attachments: Vec::new(),
                metadata: serde_json::json!({
                    "source": "scheduler",
                    "job_id": job.id,
                }),
            };

            if let Err(e) = self.outbound_tx.send(outbound).await {
                tracing::error!(error = %e, "Failed to deliver scheduled job result");
            }
        }

        // Update last run time
        self.job_store.update_last_run(&job.id).await?;

        Ok(())
    }

    pub async fn add_cron(
        &self,
        agent_id: &str,
        schedule: &str,
        prompt: &str,
        target_channel: Option<&str>,
        target_peer: Option<&str>,
    ) -> Result<Job> {
        self.job_store.create(agent_id, schedule, prompt, None, target_channel, target_peer).await
    }

    pub async fn list_jobs(&self) -> Result<Vec<Job>> {
        self.job_store.list_enabled().await
    }

    pub async fn delete_job(&self, id: &str) -> Result<()> {
        self.job_store.delete(id).await
    }
}
