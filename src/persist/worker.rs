use scylla::errors::ExecutionError;
use crate::persist::event::PersistEvent;
use crate::persist::client::{ScyllaClient};
use tokio::sync::mpsc::UnboundedReceiver;
pub async fn start_persistance_worker(mut event: UnboundedReceiver<PersistEvent>, scylla: ScyllaClient){
    while let Some(event) = event.recv().await{
    match event{
        PersistEvent::TradeExecuted{taker_id, maker_id, price, quantity, timestamp, } =>{
            if let Err(e) = scylla.insert_trade(taker_id, maker_id, price, quantity, timestamp).await{ //, timestamp)
                eprint!("Failed to Persist Trade Event, {:?}", e);
            }
        },
        PersistEvent::Snapshot{market, snapshot_time, kafka_offset, kafka_partition, asks, bids, checksum} =>{
            if let Err(e) = scylla.insert_snapshot(market, snapshot_time, kafka_offset, kafka_partition, asks, bids, checksum).await{
                eprint!("Failed to Persist Order Snapshot, {:?}", e)
            }
        }
    }
}
}