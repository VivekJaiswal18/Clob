use serde::{Deserialize, Serialize};
use wincode_derive::{SchemaWrite, SchemaRead};

#[derive(Debug, Deserialize, Serialize, SchemaWrite, Copy, SchemaRead, Clone)]
pub enum Side{
    Buy,
    Sell, 
}

#[derive(Deserialize, Serialize, Debug, SchemaRead)]
pub struct CreateOrderInput{
    // pub user_id: u32,
    pub quantity: u64,
    pub side: Side,
    pub price: u64,
}

#[derive(Deserialize, Serialize, Debug, SchemaRead)]
pub struct DeleteOrderInput{
    pub order_id: u64,
}
