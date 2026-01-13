use wincode_derive::{SchemaRead, SchemaWrite};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, SchemaRead, SchemaWrite)]
pub struct CreateOrderResponse{
    pub order_id: String,
}

#[derive(Deserialize, Serialize, Debug, SchemaRead, SchemaWrite)]
pub struct DeleteOrderResponse{
    pub order_id: u64,
    pub success: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Depth{
    pub asks: Vec<[u64; 2]>,
    pub bids: Vec<[u64; 2]>,
    pub last_update_id: String
}