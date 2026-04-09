use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use libsql_graph::cypher::executor::QueryResult;
use libsql_graph::graph::DefaultGraphEngine;

use crate::error::BoltError;
use crate::packstream::{self, PackValue};
use crate::protocol::state::{BoltState, RequestKind, TransitionResult};
use crate::protocol::message::{BoltRequest, BoltResponse};
use crate::protocol::handshake::{parse_handshake, handshake_response, handshake_failure};
use crate::transport;
use crate::types;

pub struct Session {
    state: BoltState,
    engine: Arc<Mutex<DefaultGraphEngine>>,
    connection_id: String,
    pending_result: Option<QueryResult>,
    pending_cursor: usize,
    bookmark_counter: u64,
}

async fn send_response(stream: &mut TcpStream, response: &BoltResponse) -> Result<(), BoltError> {
    let pack_value = response.to_pack_value();
    let mut buf = BytesMut::new();
    packstream::encode::encode(&pack_value, &mut buf);
    transport::write_message(stream, &buf).await
}

fn request_kind(req: &BoltRequest) -> RequestKind {
    match req {
        BoltRequest::Hello { .. } => RequestKind::Hello,
        BoltRequest::Goodbye => RequestKind::Goodbye,
        BoltRequest::Reset => RequestKind::Reset,
        BoltRequest::Run { .. } => RequestKind::Run,
        BoltRequest::Begin { .. } => RequestKind::Begin,
        BoltRequest::Commit => RequestKind::Commit,
        BoltRequest::Rollback => RequestKind::Rollback,
        BoltRequest::Discard { .. } => RequestKind::Discard,
        BoltRequest::Pull { .. } => RequestKind::Pull,
    }
}

impl Session {
    fn new(engine: Arc<Mutex<DefaultGraphEngine>>, connection_id: String) -> Self {
        Self {
            state: BoltState::Negotiation,
            engine,
            connection_id,
            pending_result: None,
            pending_cursor: 0,
            bookmark_counter: 0,
        }
    }

    fn handle_hello(&self) -> BoltResponse {
        let mut metadata = HashMap::new();
        metadata.insert("server".into(), PackValue::String("LibSQL-Graph/0.1.0".into()));
        metadata.insert("connection_id".into(), PackValue::String(self.connection_id.clone()));
        BoltResponse::Success { metadata }
    }

    fn handle_run(&mut self, query: &str, params: &HashMap<String, PackValue>) -> BoltResponse {
        let graph_params = types::pack_params_to_hashmap(params);
        let result = {
            let mut engine = self.engine.lock().unwrap();
            if graph_params.is_empty() {
                engine.query(query)
            } else {
                engine.query_with_params(query, graph_params)
            }
        };
        match result {
            Ok(qr) => {
                let fields: Vec<PackValue> = qr.columns.iter().map(|c| PackValue::String(c.clone())).collect();
                self.pending_result = Some(qr);
                self.pending_cursor = 0;
                let mut metadata = HashMap::new();
                metadata.insert("fields".into(), PackValue::List(fields));
                metadata.insert("t_first".into(), PackValue::Int(0));
                BoltResponse::Success { metadata }
            }
            Err(e) => {
                let (code, message) = types::graph_error_to_bolt(&e);
                BoltResponse::Failure { code, message }
            }
        }
    }

    fn handle_pull(&mut self, n: i64) -> (Vec<BoltResponse>, bool) {
        let mut responses = Vec::new();
        let mut has_more = false;

        if let Some(ref result) = self.pending_result {
            let total_rows = result.rows.len();
            let start = self.pending_cursor;
            let count = if n < 0 {
                total_rows - start
            } else {
                (n as usize).min(total_rows - start)
            };
            let end = start + count;

            for row in &result.rows[start..end] {
                let data: Vec<PackValue> = row.iter().map(types::graph_value_to_pack).collect();
                responses.push(BoltResponse::Record { data });
            }

            self.pending_cursor = end;
            has_more = self.pending_cursor < total_rows;

            if has_more {
                let mut metadata = HashMap::new();
                metadata.insert("has_more".into(), PackValue::Bool(true));
                responses.push(BoltResponse::Success { metadata });
            } else {
                let mut metadata = HashMap::new();
                metadata.insert("type".into(), PackValue::String("w".into()));
                metadata.insert("t_last".into(), PackValue::Int(0));
                metadata.insert("db".into(), PackValue::String("libsql-graph".into()));
                let stats_map = types::query_stats_to_map(&result.stats);
                if !stats_map.is_empty() {
                    let pairs: Vec<(String, PackValue)> = stats_map.into_iter().collect();
                    metadata.insert("stats".into(), PackValue::Map(pairs));
                }
                responses.push(BoltResponse::Success { metadata });
                self.pending_result = None;
                self.pending_cursor = 0;
            }
        } else {
            let mut metadata = HashMap::new();
            metadata.insert("type".into(), PackValue::String("w".into()));
            metadata.insert("t_last".into(), PackValue::Int(0));
            metadata.insert("db".into(), PackValue::String("libsql-graph".into()));
            responses.push(BoltResponse::Success { metadata });
        }

        (responses, has_more)
    }

    fn handle_discard(&mut self, n: i64) -> (BoltResponse, bool) {
        let mut has_more = false;

        if let Some(ref result) = self.pending_result {
            let total_rows = result.rows.len();
            let start = self.pending_cursor;
            let count = if n < 0 {
                total_rows - start
            } else {
                (n as usize).min(total_rows - start)
            };
            self.pending_cursor = start + count;
            has_more = self.pending_cursor < total_rows;

            if !has_more {
                self.pending_result = None;
                self.pending_cursor = 0;
            }
        }

        if has_more {
            let mut metadata = HashMap::new();
            metadata.insert("has_more".into(), PackValue::Bool(true));
            (BoltResponse::Success { metadata }, true)
        } else {
            (BoltResponse::Success { metadata: HashMap::new() }, false)
        }
    }

    fn handle_begin(&mut self) -> BoltResponse {
        let result = {
            let mut engine = self.engine.lock().unwrap();
            engine.begin()
        };
        match result {
            Ok(()) => BoltResponse::Success { metadata: HashMap::new() },
            Err(e) => {
                let (code, message) = types::graph_error_to_bolt(&e);
                BoltResponse::Failure { code, message }
            }
        }
    }

    fn handle_commit(&mut self) -> BoltResponse {
        let result = {
            let mut engine = self.engine.lock().unwrap();
            engine.commit()
        };
        match result {
            Ok(()) => {
                self.bookmark_counter += 1;
                let mut metadata = HashMap::new();
                metadata.insert(
                    "bookmark".into(),
                    PackValue::String(format!("bk:{}", self.bookmark_counter)),
                );
                BoltResponse::Success { metadata }
            }
            Err(e) => {
                let (code, message) = types::graph_error_to_bolt(&e);
                BoltResponse::Failure { code, message }
            }
        }
    }

    fn handle_rollback(&mut self) -> BoltResponse {
        let result = {
            let mut engine = self.engine.lock().unwrap();
            engine.rollback()
        };
        match result {
            Ok(()) => BoltResponse::Success { metadata: HashMap::new() },
            Err(e) => {
                let (code, message) = types::graph_error_to_bolt(&e);
                BoltResponse::Failure { code, message }
            }
        }
    }

    fn handle_reset(&mut self) -> BoltResponse {
        if self.state == BoltState::TxReady || self.state == BoltState::TxStreaming {
            let _ = self.engine.lock().unwrap().rollback();
        }
        self.pending_result = None;
        self.pending_cursor = 0;
        BoltResponse::Success { metadata: HashMap::new() }
    }
}

pub async fn handle_connection(
    mut stream: TcpStream,
    engine: Arc<Mutex<DefaultGraphEngine>>,
    conn_id: String,
) -> Result<(), BoltError> {
    let mut handshake_buf = [0u8; 20];
    stream.read_exact(&mut handshake_buf).await?;

    match parse_handshake(&handshake_buf) {
        Ok(result) => {
            let resp = handshake_response(&result);
            stream.write_all(&resp).await?;
        }
        Err(_) => {
            stream.write_all(&handshake_failure()).await?;
            return Ok(());
        }
    }

    let mut session = Session::new(engine, conn_id);

    loop {
        let msg_bytes = match transport::read_message(&mut stream).await {
            Ok(b) => b,
            Err(BoltError::ConnectionClosed) => return Ok(()),
            Err(e) => return Err(e),
        };

        if msg_bytes.is_empty() {
            continue;
        }

        let mut bytes = msg_bytes;
        let pack_value = packstream::decode::decode(&mut bytes)?;
        let request = BoltRequest::parse(pack_value)?;

        if matches!(request, BoltRequest::Goodbye) {
            return Ok(());
        }

        let kind = request_kind(&request);
        let transition = session.state.transition(kind, true, false);

        match transition {
            TransitionResult::Invalid => {
                let resp = BoltResponse::Failure {
                    code: "Neo.ClientError.Request.Invalid".into(),
                    message: format!("invalid request {:?} in state {:?}", kind, session.state),
                };
                send_response(&mut stream, &resp).await?;
                session.state = BoltState::Failed;
                continue;
            }
            TransitionResult::Ignored => {
                send_response(&mut stream, &BoltResponse::Ignored).await?;
                continue;
            }
            TransitionResult::NewState(_) => {}
        }

        match request {
            BoltRequest::Hello { .. } => {
                let resp = session.handle_hello();
                let success = matches!(resp, BoltResponse::Success { .. });
                send_response(&mut stream, &resp).await?;
                session.state = match session.state.transition(kind, success, false) {
                    TransitionResult::NewState(s) => s,
                    _ => BoltState::Defunct,
                };
            }
            BoltRequest::Run { ref query, ref params, .. } => {
                let resp = session.handle_run(query, params);
                let success = matches!(resp, BoltResponse::Success { .. });
                send_response(&mut stream, &resp).await?;
                session.state = match session.state.transition(kind, success, false) {
                    TransitionResult::NewState(s) => s,
                    _ => BoltState::Failed,
                };
            }
            BoltRequest::Pull { n, .. } => {
                let (responses, has_more) = session.handle_pull(n);
                for resp in &responses {
                    send_response(&mut stream, resp).await?;
                }
                let success = responses.last().map_or(false, |r| matches!(r, BoltResponse::Success { .. }));
                session.state = match session.state.transition(kind, success, has_more) {
                    TransitionResult::NewState(s) => s,
                    _ => BoltState::Failed,
                };
            }
            BoltRequest::Discard { n, .. } => {
                let (resp, has_more) = session.handle_discard(n);
                let success = matches!(resp, BoltResponse::Success { .. });
                send_response(&mut stream, &resp).await?;
                session.state = match session.state.transition(kind, success, has_more) {
                    TransitionResult::NewState(s) => s,
                    _ => BoltState::Failed,
                };
            }
            BoltRequest::Begin { .. } => {
                let resp = session.handle_begin();
                let success = matches!(resp, BoltResponse::Success { .. });
                send_response(&mut stream, &resp).await?;
                session.state = match session.state.transition(kind, success, false) {
                    TransitionResult::NewState(s) => s,
                    _ => BoltState::Failed,
                };
            }
            BoltRequest::Commit => {
                let resp = session.handle_commit();
                let success = matches!(resp, BoltResponse::Success { .. });
                send_response(&mut stream, &resp).await?;
                session.state = match session.state.transition(kind, success, false) {
                    TransitionResult::NewState(s) => s,
                    _ => BoltState::Failed,
                };
            }
            BoltRequest::Rollback => {
                let resp = session.handle_rollback();
                let success = matches!(resp, BoltResponse::Success { .. });
                send_response(&mut stream, &resp).await?;
                session.state = match session.state.transition(kind, success, false) {
                    TransitionResult::NewState(s) => s,
                    _ => BoltState::Failed,
                };
            }
            BoltRequest::Reset => {
                let resp = session.handle_reset();
                let success = matches!(resp, BoltResponse::Success { .. });
                send_response(&mut stream, &resp).await?;
                session.state = match session.state.transition(kind, success, false) {
                    TransitionResult::NewState(s) => s,
                    _ => BoltState::Defunct,
                };
            }
            BoltRequest::Goodbye => unreachable!(),
        }

        if session.state == BoltState::Defunct {
            return Ok(());
        }
    }
}
