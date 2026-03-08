use serde::Serialize;
use crate::input::Side; 

#[derive(Serialize, Clone)]
#[serde(tag="type")]
pub enum MarketEvent{
    Trade{
        price: u64,
        side: Side,
        quantity: u64,
        timestamp: i64,
        // timestamp: chrono::DateTime<Utc>,
    },
    DepthSnapshot{
        asks: Vec<(u64, u64)>,
        bids: Vec<(u64, u64)>,
    },
    DepthUpdate{
        asks: Vec<(u64, u64)>,
        bids: Vec<(u64, u64)>,
    }
}