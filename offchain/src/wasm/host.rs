use crate::ContentParser;
use crate::ResolveTask;
use anyhow::Result;
use tokio::sync::mpsc::{channel as bounded, Sender};
use sqlx::PgPool;
use std::collections::HashMap;

/// Message to the WASM module executor.
#[derive(Debug)]
pub enum Message {
    Job(WasmJob),
    Termination,
}

/// WASM parsing job
#[derive(Debug)]
pub struct WasmJob {
    task: ResolveTask,
    content: Vec<u8>,
}

impl WasmJob {
    pub fn new(task: ResolveTask, content: Vec<u8>) -> Self {
        Self { task, content }
    }
}

/// WASM module executor.
struct Module {
    sender: Sender<Message>,
    thread: tokio::task::JoinHandle<()>,
}
/// WASM module executor.
/// Tasks are currently processed sequentially per module (Instance is not Send).
pub struct Host {
    modules: HashMap<String, Module>,
}

impl Host {
    /// Spawn a new WASM module executor.
    /// The executor will spawn a thread for each module.
    ///
    /// # Arguments
    ///   * `modules` - A map of module names to their WASM bytecode.
    ///   * `connection_pool` - A connection pool to the database.
    /// # Returns
    ///  * `Host` - The WASM module executor.
    pub async fn spawn_wasm(
        modules: HashMap<String, &[u8]>,
        connection_pool: PgPool,
    ) -> Result<Self> {
        let mut wasm_modules: HashMap<String, Module> = HashMap::new();
        let connection_pool_clone = connection_pool.clone();
        for m in modules.iter() {
            let (sender, mut receiver) = bounded::<Message>(1000);
            let mut parser = crate::Parser::new(m.1, connection_pool_clone.clone()).unwrap();
            wasm_modules.insert(
                m.0.clone(),
                Module {
                    sender,
                    thread: tokio::spawn(async move {
                        debug!("started parsing thread");
                        loop {
                            match receiver.recv().await {
                                Some(message) => match message {
                                    Message::Job(job) => {
                                        debug!("Parsing {}", job.task.request.uri);
                                        if let Err(e) = parser.parse(&job.task, job.content).await {
                                            error!("Error parsing {}: {}", job.task.request.uri, e);
                                        }
                                        debug!("Done parsing {}", job.task.request.uri);
                                    }
                                    Message::Termination => {
                                        debug!("received end of stream");
                                        break;
                                    }
                                },
                                None => {
                                    error!("wasm::host: Failed to receive parsing job.");
                                }
                            }
                        }
                    }),
                },
            );
        }
        Ok(Self {
            modules: wasm_modules,
        })
    }

    /// Get a map of module names to their channels.
    pub fn get_channels(&self) -> HashMap<String, Sender<Message>> {
        self.modules
            .iter()
            .map(|m| (m.0.clone(), m.1.sender.clone()))
            .collect()
    }

    /// Wait for all modules to finish.
    pub async fn wait(self) -> Result<()> {
        for (_, module) in self.modules {
            module.sender.send(Message::Termination).await?;
            module.thread.await?;
        }
        Ok(())
    }
}
