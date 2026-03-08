use actix_web::Error;
use actix_web::cookie::time::{Date, UtcDateTime};
use chrono::{Utc, DateTime};
use rmp_serde::config::StructMapConfig;
use scylla::{client::session::Session, client::session_builder::SessionBuilder, statement::prepared::PreparedStatement, errors::ExecutionError};
use uuid::Uuid;
use core::error;
use std::sync::{Arc, atomic::{AtomicI32, AtomicI64}};
use crate::orderbook::{ExecutedOrder, Order};
use crate::persist::event::PersistEvent;

pub struct ScyllaClient{
    session: Session,
    insert_trade_statement: Arc<PreparedStatement> ,
    insert_snapshot_statement: Arc<PreparedStatement>,
    // insert_order_placed: Arc<PreparedStatement>,
}

impl ScyllaClient{
    // pub async fn new(url: &str)->Self{
        pub async fn new()->Self{
        let scylla_username = std::env::var("SCYLLA_USERNAME").expect("Scylla username missing");
        let scylla_password = std::env::var("SCYLLA_PASSWORD").expect("Scylla password missing");
       let session = loop{
        match SessionBuilder::new().known_nodes([
            std::env::var("NODE0").expect("NODE0 missing"),
            std::env::var("NODE1").expect("NODE1 missing"),
            std::env::var("NODE2").expect("NODE2 missing")
        ])
        .user(scylla_username.as_str(), scylla_password.as_str())
        .build().await{
            Ok(s)=>{eprint!("Scylla connected"); break s},
            Err(e)=>{
                eprint!("ScyllaDB not connected, retrying in 3s {}", e);
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            }
        }
    };

    session.query_unpaged(
        "CREATE KEYSPACE IF NOT EXISTS orderbook \
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
        &[],
    )
    .await
    .unwrap();

    session.query_unpaged( //change primary key to market_id also type timestamp timestamp
        "CREATE TABLE IF NOT EXISTS orderbook.trade_orders(
            
            maker_id bigint,
            taker_id bigint,
            price bigint,
            quantity bigint,
            timestamp bigint,

        PRIMARY KEY((maker_id), timestamp)
    )
    WITH CLUSTERING ORDER BY (timestamp DESC)    
        ", &[],)
        .await
        .unwrap();

    session.query_unpaged( //market* text, and last line PRIMARY KEY ((market), snapshot_time) WITH CLUSTERING ORDER BY (snapshot_time DESC)
        "CREATE TABLE IF NOT EXISTS orderbook.orderbook_snapshots(
        snapshot_time bigint,
        kafka_offset bigint,
        kafka_partition int,
        asks blob,
        bids blob,
        checksum text,
        PRIMARY KEY ((kafka_offset), snapshot_time)
    ) WITH CLUSTERING ORDER BY (snapshot_time DESC);
    ",
        &[],
    )
    .await
    .unwrap();

    let insert_trade_statement = Arc::new(
        // session.prepare("INSERT INTO orderbook.trade_orders (market, maker_id, taker_id, quantity, price, timestamp) VALUES(? ? ? ? ? ?);")
        session.prepare("INSERT INTO orderbook.trade_orders (maker_id, taker_id, quantity, price, timestamp) VALUES(?, ?, ?, ?, ?);")
        .await
        .unwrap(),
    );

    let insert_snapshot_statement = Arc::new(
        // session.prepare("INSERT INTO orderbook.orderbook_snapshots (market*, snapshot_time, kafka_offset, asks, bids) VALUES (? ? ? ? ?)")
        session.prepare("INSERT INTO orderbook.orderbook_snapshots (snapshot_time, kafka_offset, kafka_partition, asks, bids, checksum) VALUES (?, ?, ?, ?, ?, ?)")
        .await
        .unwrap()
    );

    Self{
        session,
        insert_trade_statement,
        insert_snapshot_statement
    }

    }

    // pub async fn insert_trade(&self, traded_order: ExecutedOrder)->Result<(), ExecutionError>{
    pub async fn insert_trade(&self, maker_id: i64, taker_id: i64, price: i64, quantity: i64, timestamp: i64)->Result<(), ExecutionError>{
        // let trade_id = Uuid::new_v4();
        self.session.execute_unpaged(&self.insert_trade_statement, (
            // market,
            maker_id,
            taker_id,
            price,
            quantity,
            timestamp
        ),
        )
        .await?;
        Ok(())

    }

    pub async fn insert_snapshot(&self, /*market: String*,*/ snapshot_time: i64, kafka_offset: i64, kafka_partition: i32, asks: Vec<u8>, bids: Vec<u8>, checksum: String)->Result<(), ExecutionError>{
        self.session.execute_unpaged(&self.insert_snapshot_statement, (
            // market*,
            snapshot_time,
            kafka_offset,
            kafka_partition,
            asks,
            bids,
            checksum 
        ),
        )
        .await?;
    // println!("inserting snapshot successful");
        Ok(())

    }

    pub async fn handle_event(&self, event: PersistEvent){
        // eprintln!("reached handle event before match");
        if let Err(e) = match event{
            PersistEvent::Snapshot {
                // market*, 
                snapshot_time,
                kafka_offset, 
                kafka_partition, 
                asks, 
                bids, 
                checksum } => {self.insert_snapshot( snapshot_time, kafka_offset, kafka_partition, asks, bids, checksum).await},
                // checksum } => {self.insert_snapshot( market*, snapshot_time, kafka_offset, kafka_partition, asks, bids, checksum).await},
            
            PersistEvent::TradeExecuted { 
                // market
                maker_id, 
                taker_id, 
                price, 
                quantity, 
                timestamp} => {
                    // eprintln!("trade just about to be inserted");
                    if let Err(e) = self.insert_trade( maker_id, taker_id, price, quantity, timestamp).await{
                        // eprintln!("trade inserted in scylla {:?}", e);
                    }
                    Ok(())
                },

            PersistEvent::NewOrder { order_id, price, quantity, side }=>{Ok(())},
            PersistEvent::DeleteOrder { order_id }=>{Ok(())} 
        }
        {
            eprint!("Event Persisting Failed {:?}", e);
        }
        }
    }