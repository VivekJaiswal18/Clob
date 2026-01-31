// use actix::{Actor, Addr, fut, AsyncContext, ActorFutureExt, ActorContext, clock::Instant, Running, Handler, StreamHandler};
use actix::prelude::*;
use actix::clock::Instant;
use actix_web_actors::ws;
use uuid::Uuid;

use crate::websocket::{Connect, Disconnect, MarketDataServer, WsMessage};

pub struct WsSession{
    pub id: Uuid,
    pub hb: Instant,
    pub server: Addr<MarketDataServer> 
}

impl Actor for WsSession{
    type Context = ws::WebsocketContext<Self>;
    fn started(&mut self, ctx: &mut Self::Context){
        let addr = ctx.address();
        self.server.send(Connect{
            addr: addr.recipient(),
        })
        .into_actor(self)
        .then(|res, act, ctx|{
            match res{
                Ok(id)=> act.id = id,
                _ => ctx.stop(),
            }
            fut::ready(())
        })
        .wait(ctx);
    }
    fn stopping(&mut self, ctx: &mut Self::Context) -> actix::Running {
        self.server.do_send(Disconnect{uuid: self.id});
        Running::Stop
    }
}

impl Handler<WsMessage> for WsSession{
    type Result = ();
    fn handle(&mut self, msg: WsMessage, ctx: &mut Self::Context){
        let json = serde_json::to_string(&*msg.0).unwrap();
        ctx.text(json)
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for WsSession{
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context){
        match msg{
            Ok(ws::Message::Ping(v))=>{
                self.hb = Instant::now();
                ctx.pong(&v)
            }
            Ok(ws::Message::Pong(v))=>{
                self.hb = Instant::now();
            }
            Ok(ws::Message::Close(c))=>{
                ctx.close(c);
                ctx.stop();
            }
            _ =>{}
        }
    }
}
