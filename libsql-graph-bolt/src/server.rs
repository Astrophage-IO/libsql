use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::Semaphore;

use crate::error::BoltError;
use crate::session;

const MAX_CONNECTIONS: usize = 256;

pub struct BoltServer {
    listener: TcpListener,
    db_path: String,
    semaphore: Arc<Semaphore>,
}

impl BoltServer {
    pub async fn bind(addr: &str, db_path: &str) -> Result<Self, BoltError> {
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            db_path: db_path.to_string(),
            semaphore: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        })
    }

    pub async fn run(&self) -> Result<(), BoltError> {
        let mut conn_counter: u64 = 0;
        loop {
            let permit = self
                .semaphore
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| BoltError::Protocol("semaphore closed".into()))?;
            let (stream, _addr) = self.listener.accept().await?;
            conn_counter += 1;
            let conn_id = format!("bolt-{}", conn_counter);
            let db_path = self.db_path.clone();
            tokio::spawn(async move {
                if let Err(e) = session::handle_connection(stream, &db_path, conn_id).await {
                    eprintln!("connection error: {}", e);
                }
                drop(permit);
            });
        }
    }
}
