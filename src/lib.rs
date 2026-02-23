use std::sync::atomic::AtomicU64;

pub mod engine_registry;
pub mod matching_engine;
pub mod persist;
pub mod websocket;
pub mod error;
pub mod event;
pub mod input;
pub mod output;
pub mod orderbook;
pub mod route;
pub mod kafka_worker;

pub static ORDER_ID: AtomicU64 = AtomicU64::new(1);
