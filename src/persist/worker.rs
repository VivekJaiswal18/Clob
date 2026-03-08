use scylla::errors::ExecutionError;
use crate::output::Depth;
use tokio::time::{self, interval};
use crate::persist::event::PersistEvent;
use crate::persist::client::{ScyllaClient};
use tokio::sync::mpsc::UnboundedReceiver;
use std::hash;
use std::str::Bytes;
use std::sync::{Arc, atomic::{Ordering, AtomicI64, AtomicI32} };
use std::time::Duration;
use parking_lot::RwLock;
use chrono::Utc;
use sha2::{Digest, Sha256};
use tracing::error;

// pub async fn start_persistance_worker(mut event: UnboundedReceiver<PersistEvent>, scylla: ScyllaClient){
//     while let Some(event) = event.recv().await{
//     match event{
//         PersistEvent::TradeExecuted{taker_id, maker_id, price, quantity, timestamp, } =>{
//             if let Err(e) = scylla.insert_trade(taker_id, maker_id, price, quantity, timestamp).await{ //, timestamp)
//                 eprint!("Failed to Persist Trade Event, {:?}", e);
//             }
//         },
//         PersistEvent::Snapshot{market, snapshot_time, kafka_offset, kafka_partition, asks, bids, checksum} =>{
//             if let Err(e) = scylla.insert_snapshot(market, snapshot_time, kafka_offset, kafka_partition, asks, bids, checksum).await{
//                 eprint!("Failed to Persist Order Snapshot, {:?}", e)
//             }
//         },
//         PersistEvent::NewOrder{ order_id, price, quantity, side }=>{},
//         PersistEvent::DeleteOrder { order_id }=>{}

//     }
// }
// }


pub async fn start_snapshot_persistance(scylla: Arc<ScyllaClient>, depth: Arc<RwLock<Depth>>, kafka_offset: Arc<AtomicI64>, kafka_partition: Arc<AtomicI32>){
    let mut tick = interval(Duration::from_secs(5));
    let mut prev_checksum: Option<String> = None;
    loop{
        tick.tick().await;
        let (asks, bids) = {
        let depth = depth.read();
        let asks = wincode::serialize(&depth.asks).unwrap();
        let bids = wincode::serialize(&depth.bids).unwrap();
        (asks, bids)
        // drop(depth);
        };
        let offset = kafka_offset.load(Ordering::Relaxed);
        let partition = kafka_partition.load(Ordering::Relaxed);
        let checksum = get_checksum_hash(&asks, &bids, &offset, &partition);
        // prev_checksum = &checksum;
        let snapshot_time = Utc::now().timestamp_millis();
        let time = i64::try_from(snapshot_time).unwrap();
        // println!("checksum is {:?}", checksum);
        if prev_checksum.as_ref() != Some(&checksum) {
        // println!("checksum inside the loop is {:?}", checksum);
            if let Err(e) = scylla.insert_snapshot(time, offset, partition, asks, bids, checksum.clone()).await{
                eprintln!("Inserting Snapshot to DB failed {:?}", e);
            }
            prev_checksum= Some(checksum.clone());
        }
    }

}

fn get_checksum_hash(asks: &Vec<u8>, bids: &Vec<u8>, offset: &i64, partition: &i32)->String{
    let mut hasher = Sha256::new();
    hasher.update((asks.len() as u64).to_le_bytes());
    hasher.update(asks);
    hasher.update((bids.len() as u64).to_le_bytes());
    hasher.update(bids);
    hasher.update((offset).to_le_bytes());
    hasher.update((partition).to_le_bytes());
    format!("{:x}", hasher.finalize())
}