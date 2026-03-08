use crate::persist::{ScyllaClient, event::PersistEvent};
use actix::dev::{MessageResponse};
use rdkafka::{Message, consumer::{Consumer, StreamConsumer}, producer::{FutureProducer, FutureRecord}};
use tokio::time::{Duration};
use rdkafka::util::Timeout;
use tokio::sync::mpsc::Receiver;
use std::sync::{Arc, atomic::{AtomicI64, AtomicI32, Ordering}};

pub async fn start_kafka_producer(mut rx: Receiver<PersistEvent>, producer: FutureProducer){
    // eprintln!("kafka producer started");
    while let Some(event)=rx.recv().await{
        // eprintln!("kafka producer getting {:?}", event);
        if let Ok(payload) = wincode::serialize(&event){
            let topic = match event{
                    PersistEvent::TradeExecuted{..} => "trades",
                    _ =>"orders",
                };

                let record = FutureRecord::to(topic).payload(&payload).key("clob-events");
                
                if let Err((e, _)) = producer.send(record, Timeout::After(Duration::from_secs(5))).await{
                    eprint!("Kafka ingestion error {:?}", e)
                }
            }
        }
    }

    pub async fn start_kafka_consumer(scylla: Arc<ScyllaClient>, kafka_offset: Arc<AtomicI64>, kafka_partition: Arc<AtomicI32>){
        let consumer : StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("group.id", "clob-consumer")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "50000")
        .create()
        .expect("failed to create kafka consumer ");
        // eprint!("start kafka consumer started");
        consumer.subscribe(&["trades", "orders"]).expect("Failed to subscribe to topics");

        // while let Ok(msg) = consumer.recv().await{
        //     eprintln!("kafka consumer started in producer.rs");
        //     if let Some(pay) = msg.payload() && let Ok(event) = wincode::deserialize::<PersistEvent>(pay){
        //         eprint!("kafka sent mesg to scylla {:?}", &event);
        //         scylla.handle_event(event).await;
        //     }
        // }
        loop{
            match consumer.recv().await{
                Ok(msg)=>{
                    // eprintln!("message in consumer {:?}", msg);
                    // eprintln!("kafka offset {:?} partition {:?}", msg.offset(), msg.partition());
                    if let Some(pay) = msg.payload() && let Ok(event) = wincode::deserialize::<PersistEvent>(pay){
                                // eprint!("kafka sent mesg to scylla {:?}", &event);
                                scylla.handle_event(event).await;
                                kafka_offset.store(msg.offset(), Ordering::Relaxed);
                                kafka_partition.store(msg.partition(), Ordering::Relaxed);
                }},
                Err(e)=>{eprintln!("message in consumer {:?}", e)}
            }
        }
    }
