use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use wincode_derive::{SchemaRead, SchemaWrite};

#[derive(SchemaRead, SchemaWrite)]
pub enum PersistEvent{
    TradeExecuted{
        maker_id: i64,
        taker_id: i64,
        price: i64,
        quantity: i64,
        timestamp: i64,
        // timestamp: DateTime<Utc>,
    },
    // NewOrder(Order),
    // DeleteOrder{
    //     order_id: u64
    // },
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