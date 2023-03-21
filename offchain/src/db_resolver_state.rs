use std::time::Duration;

use crate::resolver::{ResolveTask, ResolverState, TaskState};
use anyhow::Result;
use async_trait::async_trait;
use futures::TryStreamExt;
use sqlx::PgPool;
use tokio_util::time::delay_queue::DelayQueue;
use int_enum::IntEnum;
use substreams_sink::{OffchainData};

/// Saves resolver state in Posgtres DB.
pub struct DBResolverState {
    connection_pool: PgPool,
}

impl DBResolverState {
    pub async fn new(connection_pool: PgPool) -> Result<Self> {
        sqlx::query!(
            r#"CREATE TABLE IF NOT EXISTS resolver_tasks
            (
                uri               TEXT,
                manifest          TEXT,
                handler           TEXT,
                max_retries       INTEGER,
                wait_before_retry INTEGER,
                num_retries       INTEGER,
                state             INTEGER,
                PRIMARY KEY (uri, manifest)
            )"#
        )
        .execute(&mut connection_pool.acquire().await?)
        .await?;
        Ok(Self { connection_pool })
    }
}

#[async_trait]
impl ResolverState for DBResolverState {
    async fn load_tasks(&mut self) -> Result<DelayQueue<ResolveTask>> {
        let mut connection = self.connection_pool.acquire().await?;
        let mut rows = sqlx::query!("SELECT uri, manifest, handler, max_retries, wait_before_retry, num_retries, state FROM resolver_tasks WHERE state = $1", TaskState::Queued.int_value())
        .fetch(&mut connection);

        let mut task_queue = DelayQueue::new();
        while let Some(row) = rows.as_mut().try_next().await? {
            task_queue.insert(
                ResolveTask {
                    manifest: row.manifest,
                    request: OffchainData {
                        uri: row.uri,
                        handler: row.handler.unwrap(),
                        max_retries: row.max_retries.unwrap(),
                        wait_before_retry: row.wait_before_retry.unwrap(),
                    },
                    num_retries: row.num_retries.unwrap(),
                },
                Duration::ZERO,
            );
        }
        Ok(task_queue)
    }

    async fn add_task(&mut self, task: &ResolveTask) -> Result<()> {
        sqlx::query!(
            "INSERT INTO resolver_tasks (uri, manifest, handler, max_retries, wait_before_retry, num_retries, state) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            task.request.uri,
            &task.manifest,
            task.request.handler,
            task.request.max_retries,
            task.request.wait_before_retry,
            task.num_retries,
            TaskState::Queued.int_value(),
        )
        .execute(&mut self.connection_pool.acquire().await?)
        .await?;
        Ok(())
    }

    async fn update_retry_counter(&mut self, task: &ResolveTask) -> Result<()> {
        sqlx::query!(
            "UPDATE resolver_tasks SET num_retries = $1 WHERE uri = $2 AND manifest = $3",
            task.num_retries,
            &task.request.uri,
            &task.manifest,
        )
        .execute(&mut self.connection_pool.acquire().await?)
        .await?;
        Ok(())
    }

    async fn update_task_state(&mut self, task: &ResolveTask, state: TaskState) -> Result<()> {
        sqlx::query!(
            "UPDATE resolver_tasks SET state = $1 WHERE uri = $2 AND manifest = $3",
            state.int_value(),
            &task.request.uri,
            &task.manifest,
        )
        .execute(&mut self.connection_pool.acquire().await?)
        .await?;
        Ok(())
    }
}
