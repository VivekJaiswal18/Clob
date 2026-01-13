use crate::input::{Side};
use wincode_derive::{SchemaWrite, SchemaRead};
use serde::{Deserialize, Serialize};


#[derive(Deserialize, Serialize, Debug, SchemaWrite, SchemaRead)]
pub enum OrderEvent{
    NewOrder {
        order_id: u64,
        // user_id: u32,
        price: u64,
        side: Side,
        quantity: u64
    },

    DeleteOrder{
        order_id: u64,
    }
}

// #[derive(Serializa, Deserialize, Debug)]
// pub struct Depth{
//     pub price: u32,
//     pub quantity: u32,
//     pub 
// }