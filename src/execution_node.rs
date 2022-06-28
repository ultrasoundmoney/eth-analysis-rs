mod blocks;
mod stream_supply_deltas;

use core::panic;
use std::collections::HashMap;
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;

use crate::config;
use crate::execution_node::blocks::*;
use crate::execution_node::stream_supply_deltas::*;
use async_tungstenite::{
    tokio::{connect_async, TokioAdapter},
    tungstenite::Message,
    WebSocketStream,
};
use futures::channel::mpsc;
use futures::channel::oneshot;
use futures::prelude::*;
use futures::stream::SplitSink;
use futures::stream::SplitStream;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use tokio::net::TcpStream;

#[derive(Debug, Deserialize)]
struct RpcMessage<A> {
    id: u16,
    result: A,
}

fn make_supply_delta_subscribe_message(number: &u32) -> String {
    let msg = json!({
        "id": 0,
        "method": "eth_subscribe",
        "params": ["issuance", number]
    });

    serde_json::to_string(&msg).unwrap()
}

#[allow(dead_code)]
fn make_supply_delta_unsubscribe_message(id: &str) -> String {
    let msg = json!({
        "id": 0,
        "method": "eth_unsubscribe",
        "params": [id]

    });

    serde_json::to_string(&msg).unwrap()
}

struct IdPool {
    next_id: u16,
    in_use_ids: HashSet<u16>,
}

impl IdPool {
    fn new(size: usize) -> Self {
        Self {
            next_id: 0,
            in_use_ids: HashSet::with_capacity(size),
        }
    }

    fn get_next_id(&mut self) -> u16 {
        if self.in_use_ids.len() == self.in_use_ids.capacity() {
            panic!("execution node id pool exhausted")
        }

        while self.in_use_ids.contains(&self.next_id) {
            self.next_id = self.next_id + 1;
        }

        self.in_use_ids.insert(self.next_id);

        self.next_id
    }

    fn free_id(&mut self, id: &u16) {
        self.in_use_ids.remove(id);
    }
}

pub fn stream_supply_deltas_from(
    from: u32,
    chunk_size: usize,
) -> mpsc::UnboundedReceiver<Vec<SupplyDelta>> {
    let (mut supply_deltas_tx, supply_deltas_rx) = mpsc::unbounded();

    tokio::spawn(async move {
        let mut ws = connect_async(format!("{}", &config::get_execution_url()))
            .await
            .unwrap()
            .0;

        let deltas_subscribe_message = make_supply_delta_subscribe_message(&from);

        ws.send(Message::text(deltas_subscribe_message))
            .await
            .unwrap();

        loop {
            if let Some(_) = ws.next().await {
                tracing::debug!("got subscription confirmation message");
                break;
            }
        }

        let mut supply_delta_buffer = Vec::with_capacity(chunk_size);

        while let Some(message) = ws.next().await {
            let message_text = message.unwrap().into_text().unwrap();
            let supply_delta_message =
                serde_json::from_str::<SupplyDeltaMessage>(&message_text).unwrap();
            let supply_delta = SupplyDelta::from(supply_delta_message);

            supply_delta_buffer.push(supply_delta);

            if supply_delta_buffer.len() >= chunk_size {
                supply_deltas_tx.send(supply_delta_buffer).await.unwrap();
                supply_delta_buffer = vec![];
            }
        }
    });

    supply_deltas_rx
}

type NodeMessageRx = SplitStream<WebSocketStream<TokioAdapter<TcpStream>>>;
type MessageHandlersShared = Arc<Mutex<HashMap<u16, oneshot::Sender<String>>>>;
type IdPoolShared = Arc<Mutex<IdPool>>;

async fn handle_messages(
    mut ws_rx: NodeMessageRx,
    message_handlers: MessageHandlersShared,
    id_pool: IdPoolShared,
) -> Result<(), Box<dyn Error>> {
    while let Some(message) = ws_rx.next().await {
        let message_text = message?.into_text()?;
        let rpc_message =
            serde_json::from_str::<RpcMessage<serde_json::Value>>(&message_text).unwrap();
        match message_handlers.lock().unwrap().remove(&rpc_message.id) {
            None => {
                tracing::error!("got a message without a handler: {:?}", rpc_message);
            }
            Some(tx) => {
                id_pool.lock().unwrap().free_id(&rpc_message.id);
                tx.send(message_text)?;
            }
        };
    }

    Ok(())
}

pub struct ExecutionNode {
    id_pool: Arc<Mutex<IdPool>>,
    message_handlers: Arc<Mutex<HashMap<u16, oneshot::Sender<String>>>>,
    message_sink: SplitSink<WebSocketStream<TokioAdapter<TcpStream>>, Message>,
}

impl ExecutionNode {
    pub async fn connect() -> Self {
        let id_pool_am = Arc::new(Mutex::new(IdPool::new(u16::MAX.into())));

        let message_handlers_am = Arc::new(Mutex::new(
            HashMap::<u16, oneshot::Sender<String>>::with_capacity(u16::MAX.into()),
        ));

        let (ws_tx, ws_rx) = connect_async(format!("{}", &config::get_execution_url()))
            .await
            .unwrap()
            .0
            .split();

        // Our execution node websocket uses pipelining, this means we'd like to read messages
        // concurrently while the main program executes. However, if message reading fails, our
        // thread may panic, causing main thread channels to hang. To avoid this main should panic
        // when this thread panics.
        let default_panic = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            default_panic(info);
            std::process::exit(1);
        }));

        let id_pool_ref = id_pool_am.clone();
        let message_handlers_ref = message_handlers_am.clone();
        tokio::spawn(async move {
            handle_messages(ws_rx, message_handlers_ref, id_pool_ref)
                .await
                .unwrap()
        });

        ExecutionNode {
            id_pool: id_pool_am,
            message_handlers: message_handlers_am,
            message_sink: ws_tx,
        }
    }

    pub async fn get_latest_block(&mut self) -> Block {
        let message = self
            .call(
                "eth_getBlockByNumber",
                &json!((String::from("latest"), false)),
            )
            .await;

        let rpc_message = serde_json::from_str::<RpcMessage<BlockF>>(&message).unwrap();

        Block::from(rpc_message)
    }

    async fn call(&mut self, method: &str, params: &Value) -> String {
        let id = self.id_pool.lock().unwrap().get_next_id();

        let json = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });

        let message = serde_json::to_string(&json).unwrap();

        let (tx, rx) = oneshot::channel();

        self.message_handlers.lock().unwrap().insert(id, tx);
        self.message_sink
            .send(Message::Text(message))
            .await
            .unwrap();

        rx.await.unwrap()
    }

    pub async fn close(&mut self) {
        self.message_sink.close().await.unwrap();
    }
}
