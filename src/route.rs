use actix_web::{HttpRequest, Responder, HttpResponse, get, post, web, delete};
use actix_web::web::{Bytes, Data};
use parking_lot::RwLock;
use reqwest::get;
use tokio::{sync::mpsc};
use serde_json;
use std::sync::{Arc, atomic::Ordering};
use crate::ORDER_ID;
use crate::input::{CreateOrderInput, DeleteOrderInput};
use crate::event::{OrderEvent};
use crate::output::{CreateOrderResponse, DeleteOrderResponse, Depth};
use crate::websocket::MarketDataServer;
// use crate::ORDER_ID;


type OrderSender = Arc<mpsc::UnboundedSender<OrderEvent>>;

fn is_wincode(req: &HttpRequest)->bool{
    req.headers()
    .get("content-type")
    .and_then(|v| v.to_str().ok())
    .map(|s| s.contains("application/octet-stream") || s.contains("wincode"))
    .unwrap_or(false)
}

// fn is_msgpack(req: &HttpRequest)-> bool{
//     req.heades()
//     .get("content-type")
//     .and_then(|v| v.to_str().ok())
//     .map(|s| s.contains("msgpack"))
//     .unwrap_or(false)
// }

fn wants_wincode(req: &HttpRequest)->bool{
    req.headers()
    .get("accept")
    .and_then(|v| v.to_str().ok())
    .map(|s| s.contains("application/octet-stream") || s.contains("wincode"))
    .unwrap_or(false)
}

// fn wants_msgpack(&req: &HttpRequest) -> bool{
//     req.headers()
//     .get("accept")
//     .and_then(|v| v.to_str().ok())
//     .map(|s| s.contains("msgpack"))
//     .unwrap_or(false)
// }

#[post("/order")]
pub async fn create_order(
    req: HttpRequest,
    body: web::Bytes,
    sender: Data<OrderSender>
)-> impl Responder{

    let input : CreateOrderInput= 
    if is_wincode(&req){
        match wincode::deserialize::<CreateOrderInput>(&body){
            Ok(data)=> data,
            Err(e)=> return HttpResponse::BadRequest().body(format!("Invalid wincode format {:?}", e)),
        }
    }
    // else if is_msgpack(&req){
    //     match rmp_serde::from_slice(&body){
    //         Ok(data)=> data,
    //         Err(e)=> return HttpResponse.BadRequest().body(!format("Invalid msgpack format {}", e)),
    //     }
    // }
    else{
        match serde_json::from_slice(&body){
            Ok(data) => data,
            Err(e) => return HttpResponse::BadRequest().body(format!("Invalid request format {:?}", e)),
        }
    };
    let order_id: u64 = ORDER_ID.fetch_add(1, Ordering::Relaxed);
    let event = OrderEvent::NewOrder{
        order_id: order_id,
        // user_id: input.user_id,
        price: input.price,
        quantity: input.quantity,
        side: input.side
    };

    match sender.send(event){
        Ok(_) =>{
        let response = CreateOrderResponse{
            order_id: order_id.to_string()
        };
        if wants_wincode(&req){
            match wincode::serialize(&response){
                Ok(bytes)=> return HttpResponse::Ok().content_type("application/octet-stream").body(bytes),
                Err(e)=> return HttpResponse::BadRequest().body(format!("Could not send response: {:?}", e)),
            }
        } 
        else{
            return HttpResponse::Ok().json(&response)
        }
    }
    Err(e)=> return HttpResponse::InternalServerError().body(format!("Order not sent: {:?}", e)),
    };

}

#[delete("/order")]
pub async fn delete_order(req: HttpRequest, sender: Data<OrderSender>, body: web::Bytes)-> impl Responder{
    let input : DeleteOrderInput = 
    if is_wincode(&req){
        match wincode::deserialize::<DeleteOrderInput>(&body){
            Ok(data) => data,
            Err(e) => return HttpResponse::BadRequest().body(format!("Invalid wincode Input Data: {:?}", e)),
        }
    }
    else {
        match serde_json::from_slice(&body){
            Ok(data) => data,
            Err(e) => return HttpResponse::BadRequest().body(format!("Invalid Json Input: {:?}", e)),
        }
    };
    let event = OrderEvent::DeleteOrder{
        order_id: input.order_id,
    };

   match sender.send(event){
    Ok(_) => {
        let response = DeleteOrderResponse{
            order_id: input.order_id,
            success: true,
        };
        if wants_wincode(&req){
            match wincode::serialize(&response){
                Ok(bytes)=> HttpResponse::Ok().content_type("application/octet-stream").body(bytes),
                Err(_e)=> HttpResponse::Ok().json(response)
            }
        }
        else{
            HttpResponse::Ok().json(response)
        }
    }
    Err(e)=>{
        HttpResponse::InternalServerError().json(format!("Could not send delete order: {:?}", e))
    }
   }
}

#[get("/depth")]
pub async fn get_depth(req: HttpRequest, depth: Data<Arc<RwLock<Depth>>>)-> impl Responder{

    let depth_read = depth.read();
    let depth_snapshot = Depth{
        asks: depth_read.asks.clone(),
        bids: depth_read.bids.clone(),
        last_update_id: depth_read.last_update_id.clone()
    };
    drop(depth_read);

    if wants_wincode(&req){
        match wincode::serialize(&depth_snapshot){
         Ok(bytes)=>HttpResponse::Ok().content_type("application/octect-stream").body(bytes),
         Err(e)=> HttpResponse::Ok().json(&depth_snapshot)
        }
    }
    else{
        HttpResponse::Ok().json(&depth_snapshot)
    }
}
