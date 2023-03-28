use crate::db_resolver_state::DBResolverState;
use crate::wasm::{self, WasmJob};
use anyhow::Result;
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use futures::StreamExt;
use int_enum::IntEnum;
use sqlx::PgPool;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use substreams_sink::OffchainData;
use tokio_util::time::delay_queue::DelayQueue;
use tonic::codegen::http::uri::Uri;

#[repr(i32)]
#[derive(Copy, Clone, IntEnum)]
pub enum TaskState {
    Queued = 0,
    UnknownURI = 1,
    UnknownParser = 2,
    DownloadFailed = 3,
    ParsingFailed = 4,
    Finished = 5,
}

/// Resolve task
#[derive(Clone)]
pub struct ResolveTask {
    pub manifest: String,
    pub request: OffchainData,
    pub num_retries: i32,
}

impl ResolveTask {
    fn increment_try_counter(&mut self) -> bool {
        if self.num_retries < self.request.max_retries {
            self.num_retries = self.num_retries + 1;
            true
        } else {
            false
        }
    }
}

/// Message to the resolver
pub enum Message {
    Job(ResolveTask),
    ScheduleRetry(ResolveTask),
    Termination,
}

/// Link resolver
#[async_trait]
pub trait LinkResolver: Send + Sync + 'static {
    async fn download(&self, uri: &str) -> Result<Vec<u8>>;
}

/// Resolver state
#[async_trait]
pub trait ResolverState {
    async fn load_tasks(&mut self) -> Result<DelayQueue<ResolveTask>>;
    async fn add_task(&mut self, task: &ResolveTask) -> Result<()>;
    async fn update_task_state(&mut self, task: &ResolveTask, state: TaskState) -> Result<()>;
    async fn update_retry_counter(&mut self, task: &ResolveTask) -> Result<()>;
}

/// Off-chain content parser
pub trait ContentParser {
    fn parse(&mut self, task: &ResolveTask, content: Vec<u8>) -> Result<()>;
}

/// Off-chain content resolver
/// Jobs are accepted through the input channel.
pub struct Resolver {
    off_chain_task_receiver: Receiver<Message>,
    off_chain_task_sender: Sender<Message>,
    state: DBResolverState,
    queue: DelayQueue<ResolveTask>,
    downloaders: HashMap<String, Arc<dyn LinkResolver>>,
    is_stopped: bool,
    num_running_tasks: Arc<AtomicU64>,
    max_concurrent_resolver_tasks: u64,
}

impl Resolver {
    /// Create a new resolver
    /// # Arguments
    ///    * `pg_database_url` - Postgres database URL
    ///    * `downloaders` - Map of downloader schemes to downloader implementations
    ///    * `max_concurrent_resolver_tasks` - Maximum number of concurrent resolver tasks
    /// # Returns
    ///   * `Resolver` - Resolver instance
    ///   * `Sender<Message>` - Sender to the resolver
    pub async fn new(
        pg_database_url: &str,
        downloaders: HashMap<String, Arc<dyn LinkResolver>>,
        max_concurrent_resolver_tasks: u64,
    ) -> Result<(Self, Sender<Message>)> {
        let (off_chain_task_sender, off_chain_task_receiver) =
            async_channel::unbounded::<Message>();

        let connection_pool = PgPool::connect(pg_database_url).await?;
        let mut state = DBResolverState::new(connection_pool.clone()).await?;
        Ok((
            Self {
                off_chain_task_receiver,
                off_chain_task_sender: off_chain_task_sender.clone(),
                queue: state.load_tasks().await?,
                state,
                downloaders,
                is_stopped: false,
                num_running_tasks: Arc::new(AtomicU64::new(0)),
                max_concurrent_resolver_tasks,
            },
            off_chain_task_sender,
        ))
    }

    /// Run the resolver
    /// # Arguments
    ///    * `parsers` - Map of manifest names to parsers
    pub async fn run(&mut self, parsers: HashMap<String, Sender<wasm::Message>>) -> Result<()> {
        while !(self.is_stopped && self.queue.is_empty()) {
            let task = tokio::select! {
                Some(expired) = self.queue.next() => {
                    debug!("resolver: expired");
                    expired.into_inner()
                },
                Ok(message) = self.off_chain_task_receiver.recv() => {
                    debug!("resolver: new task");
                    match message {
                        Message::Job(task) => {
                            self.state.add_task(&task).await?;
                            task
                        }
                        Message::ScheduleRetry(mut task) => {
                            match task.increment_try_counter() {
                                true => {
                                    trace!(
                                        "scheduling retry {} {}",
                                        task.num_retries,
                                        task.request.max_retries
                                    );
                                    self.state.update_retry_counter(&task).await?;
                                    self.queue.insert(
                                        task.clone(),
                                        Duration::from_secs(task.request.wait_before_retry as u64),
                                    );
                                }
                                false => self.state.update_task_state(&task, DownloadFailed).await?
                            }
                            continue;
                        }
                        Message::Termination => {
                            debug!("resolver: stopped {}", self.queue.len());
                            self.is_stopped = true;
                            continue;
                        }
                    }
                },
                else => break,
            };

            let parser = parsers.get(&task.manifest).clone();
            let downloader = {
                match task.request.uri.parse::<Uri>() {
                    Ok(uri) => {
                        if let Some(protocol) = uri.scheme() {
                            self.downloaders.get(protocol.as_str())
                        } else {
                            None
                        }
                    }
                    Err(_) => None,
                }
            };

            use TaskState::*;
            match (downloader, parser) {
                (Some(downloader), Some(parser)) => {
                    self.throttle().await;
                    let downloader = downloader.clone();
                    let parser = parser.clone();
                    let off_chain_task_sender = self.off_chain_task_sender.clone();
                    let num_running_tasks = self.num_running_tasks.clone();

                    tokio::spawn(async move {
                        num_running_tasks.fetch_add(1, Ordering::SeqCst);
                        let _ = Self::process_task(task, downloader, parser, off_chain_task_sender)
                            .await;
                        num_running_tasks.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                (None, _) => self.state.update_task_state(&task, UnknownURI).await?,
                (_, None) => self.state.update_task_state(&task, UnknownParser).await?,
            };
        }

        while self.num_running_tasks.load(Ordering::SeqCst) > 0 {
            debug!(
                "waiting for tasks to complete {}",
                self.num_running_tasks.load(Ordering::SeqCst)
            );
            tokio::time::sleep(Duration::from_millis(1000)).await;
        }
        debug!("resolver thread exited");
        Ok(())
    }

    async fn process_task(
        task: ResolveTask,
        downloader: Arc<dyn LinkResolver>,
        parser: Sender<wasm::Message>,
        off_chain_task_sender: Sender<Message>,
    ) -> Result<()> {
        debug!("resolver: processing task {}", task.request.uri,);
        match downloader.download(&task.request.uri).await {
            Ok(bytes) => {
                debug!("sending task to parser {}", task.request.uri);
                parser
                    .send(wasm::Message::Job(WasmJob::new(task.clone(), bytes)))
                    .await?;
            }
            Err(_) => {
                off_chain_task_sender
                    .send(Message::ScheduleRetry(task))
                    .await?
            }
        }
        Ok(())
    }

    async fn throttle(&self) {
        if self.num_running_tasks.load(Ordering::SeqCst) >= self.max_concurrent_resolver_tasks {
            debug!(
                "maximum number of concurrent tasks reached {}",
                self.max_concurrent_resolver_tasks
            );
            while self.num_running_tasks.load(Ordering::SeqCst)
                >= self.max_concurrent_resolver_tasks
            {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}
