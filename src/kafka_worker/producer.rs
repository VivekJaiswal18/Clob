use crate::persist::{ScyllaClient, event::PersistEvent};
use actix::dev::MessageResponse;
use rdkafka::{Message, consumer::{Consumer, StreamConsumer}, producer::{FutureProducer, FutureRecord}};
use tokio::time::{Duration};
use rdkafka::util::Timeout;
use tokio::sync::mpsc::UnboundedReceiver;
use std::sync::Arc;

pub async fn start_kafka_producer(mut rx: UnboundedReceiver<PersistEvent>, producer: FutureProducer){
    while let Some(event)=rx.recv().await{
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

    pub async fn start_kafka_consumer(scylla: Arc<ScyllaClient>){
        let consumer : StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("group.id", "clob-consumer")
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "5000")
        .create()
        .expect("failed to create kafka consumer ");

        consumer.subscribe(&["trades", "orders"]).expect("Failed to subscribe to topics");

        while let Ok(msg) = consumer.recv().await{
            if let Some(pay) = msg.payload() && let Ok(event) = wincode::deserialize::<PersistEvent>(pay){
                scylla.handle_event(event).await;
            }
        }
    }
