use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::net::TcpListener;

use libsql_graph::graph::DefaultGraphEngine;

use crate::error::BoltError;
use crate::session;

pub struct BoltServer {
    listener: TcpListener,
    engine: Arc<Mutex<DefaultGraphEngine>>,
}

impl BoltServer {
    pub async fn bind(addr: &str, db_path: &str) -> Result<Self, BoltError> {
        let engine = if Path::new(db_path).exists() {
            DefaultGraphEngine::open(db_path).map_err(|e| BoltError::Engine(format!("{}", e)))?
        } else {
            DefaultGraphEngine::create(db_path, 4096).map_err(|e| BoltError::Engine(format!("{}", e)))?
        };
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            engine: Arc::new(Mutex::new(engine)),
        })
    }

    pub async fn run(&self) -> Result<(), BoltError> {
        let mut conn_counter: u64 = 0;
        loop {
            let (stream, _addr) = self.listener.accept().await?;
            conn_counter += 1;
            let conn_id = format!("bolt-{}", conn_counter);
            let engine = Arc::clone(&self.engine);
            tokio::spawn(async move {
                if let Err(e) = session::handle_connection(stream, engine, conn_id).await {
                    eprintln!("connection error: {}", e);
                }
            });
        }
    }
}
