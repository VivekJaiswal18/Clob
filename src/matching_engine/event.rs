use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use wincode_derive::{SchemaRead, SchemaWrite};

use crate::input::Side;
// use crate::orderbook::Order;

#[derive(SchemaRead, SchemaWrite)]
pub enum PersistEvent{
    TradeExecuted{
        maker_id: i64,
        taker_id: i64,
        price: i64,
        quantity: i64,
        timestamp: i64,
        // timestamp: chrono::DateTime<Utc>,
        // timestamp: DateTime<Utc>,
    },
    NewOrder{
        order_id: u64,
        price: u64,
        quantity: u64,
        side: Side,
    },
    DeleteOrder{
        order_id: u64
    },
    Snapshot{
        market: String,
        snapshot_time: i64,
        kafka_offset: i64,
        kafka_partition: i32,
        asks: Vec<u8>,
        bids: Vec<u8>,
        checksum: String
    }
}