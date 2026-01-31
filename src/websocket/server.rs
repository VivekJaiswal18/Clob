use actix::prelude::*;
use actix_web::Error;
use uuid::Uuid;
use super::event::MarketEvent;
use std::{collections::HashMap, sync::Arc};

#[derive(Message)]
#[rtype(result="()")]
pub struct WsMessage(pub Arc<MarketEvent>); //implement arc

#[derive(Message)]
#[rtype(result = "Uuid")]
// #[rtype(result = Response)]
pub struct Connect{
    pub addr: Recipient<WsMessage>
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Disconnect{
    pub uuid: Uuid,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct BroadCastMarketEvent{
    pub event: MarketEvent,
}

pub struct MarketDataServer{
    session: HashMap<Uuid, Recipient<WsMessage>>,
}

impl Actor for MarketDataServer{
    type Context = Context<Self>;
}

impl MarketDataServer{
    pub fn new()->Self{
        Self{
            session: HashMap::new(),
        }
    }

    pub fn broadcast(&self, msg: Arc<MarketEvent>){
        for addr in self.session.values(){
            let _ = addr.do_send(WsMessage(msg.to_owned()));
        }
        
    }
}

impl Handler<Connect> for MarketDataServer{
    // type Result = Uuid;
    type Result = MessageResult<Connect>;
    fn handle(&mut self, msg: Connect, _: &mut Context<Self>)->Self::Result{
        let id = Uuid::new_v4();
        self.session.insert(id, msg.addr);
        MessageResult(id)
    }
}

impl Handler<Disconnect> for MarketDataServer{
    type Result = ();
    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>)->Self::Result{
        self.session.remove(&msg.uuid);
    }
}

impl Handler<BroadCastMarketEvent> for MarketDataServer{
    type Result = ();
    fn handle(&mut self, msg: BroadCastMarketEvent, _: &mut Context<Self>)->Self::Result{
        let event = Arc::new(msg.event);
        self.broadcast(event);
    }
}