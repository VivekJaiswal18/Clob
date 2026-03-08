use actix_web::Error;
use actix_web::cookie::time::convert::Millisecond;
use actix_web::cookie::time::{Time, UtcDateTime};
use chrono::Utc;
use serde::de::Expected;
use tracing::warn;
use crate::error::{OrderbookError, Result};
use crate::input::Side;
use crate::matching_engine;
use std::collections::{BTreeMap, HashMap};
use std::mem::MaybeUninit;
use crate::output::{Depth};

pub struct Order{
    // market : String,
    pub order_id: u64,
    // pub user_id: u64, //will use for checking orders by same user on both sides
    pub price: u64,
    pub quantity: u64,
    pub side: Side
} 

impl Order{
    pub fn validate(&self)->Result<()>{
        if self.quantity == 0{
            return Err(OrderbookError::PriceError)
        }
        if self.price == 0{
            return Err(OrderbookError::QuantityError)
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct PriceLevel{
    pub orders: Vec<u64>,
    pub quantity: Vec<u64>,
    pub total_quantity: u64,    
    pub tombstone: Vec<bool>
}

impl PriceLevel{
    #[inline]
    pub fn new()-> Self{
        Self{
        orders: Vec::with_capacity(20),
        quantity: Vec::with_capacity(20),
        total_quantity: 0,
        tombstone: Vec::with_capacity(20)
    }
}

#[inline]
pub fn push(&mut self, order: &Order){  
    self.orders.push(order.order_id);
    self.quantity.push(order.quantity);
    self.total_quantity += order.quantity;
    self.tombstone.push(false);
}

#[inline]
pub fn remove_order(&mut self, idx: usize)->Result<()>{
    if idx > self.tombstone.len(){
        return Err(OrderbookError::IndexError)
    }
    self.total_quantity = self.total_quantity.saturating_sub(self.quantity[idx]);
    self.tombstone[idx] = true;
    Ok(())
}

pub fn reduce_quantity(&mut self, idx: usize, new_maker_quantity: u64)->Result<()>{
    if idx > self.orders.len(){
        return Err(OrderbookError::IndexError);
    }
    if new_maker_quantity > self.quantity[idx]{
        return Err(OrderbookError::OrderQuantityExceeded);
    }
    self.quantity[idx] = new_maker_quantity;
    self.total_quantity = self.total_quantity.saturating_sub(self.quantity[idx]).saturating_add(new_maker_quantity);
    Ok(())
}
}

#[derive(Debug)]
pub struct ExecutedOrder{
    pub maker_id: u64,
    pub taker_id: u64,
    pub price: u64,
    pub quantity: u64,
    pub timestamp: i64
    // pub timestamp: chrono::DateTime<Utc>
    // pub timestamp: UtcDateTime
}

#[derive(Debug)]
pub struct DepthCache{
    pub asks: [[u64; 2]; 20],
    pub bids: [[u64; 2]; 20],
    pub stale: bool,
    pub bid_count: usize,
    pub ask_count: usize
}

pub struct TradeMsg{
    price: u64,
    maker_id: u64,
    taker_id: u64,
    timestamp: i64,
    // timestamp: chrono::DateTime<Utc>,
    // timestamp: UtcDateTime,
    quantity: u64,
}

#[derive(Debug)]
pub struct OrderLocation{
    pub side: Side,
    pub price: u64,
    pub index: usize
}

#[derive(Debug)]
pub struct OrderBook{
    pub asks: BTreeMap<u64, PriceLevel>,
    pub bids: BTreeMap<u64, PriceLevel>,
    pub trade_len: usize,
    pub depth_cache: DepthCache,
    pub trade_buf: [MaybeUninit<TradeMsg>; 50], //not using vec to avoid heap allocation, reallocation, etc
    pub order_location: HashMap<u64, OrderLocation>, 
    // pub order_persist todo
}

impl OrderBook{
    pub fn new()-> Self{
        Self { 
            asks: BTreeMap::new(),
            bids: BTreeMap::new(), 
            trade_len: 0,
            trade_buf: unsafe {MaybeUninit::uninit().assume_init()},
            order_location: HashMap::with_capacity(1000),
            depth_cache:  DepthCache { 
                asks: [[0; 2]; 20], 
                bids: [[0; 2]; 20], 
                stale: true, 
                bid_count: 0, 
                ask_count: 0 
            }
        }
    }

pub fn matching_order(&mut self, mut taker: Order)->Result<Vec<ExecutedOrder>>{
    let timestamp = Utc::now().timestamp_millis();
    // let timestamp: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
    let mut executed_trades= Vec::new();
    let mut remove_price = Vec::new();
    taker.validate()?;
    let match_ask= matches!(taker.side, Side::Buy);

    {
    let book = if match_ask {
        &mut self.asks
    }
    else{
        &mut self.bids 
    };

    let price_keys : Vec<u64> = if match_ask{
        book.range(..=taker.price).map(|(p, _)| *p).collect()
    } 
    else{
        book.range(taker.price..).rev().map(|(p, _)| *p).collect()
    };

    for price in price_keys{
        if taker.quantity == 0{
        break;
        }
        let level = match book.get_mut(&price){ //gives mutable access of the book at that price
            Some(l) => l,
            None => continue,
        };

        let mut idx = 0;
        while idx < level.orders.len() && taker.quantity>0{
            if level.tombstone[idx]{
                    idx += 1;
                    continue;
            }
            let maker_id = level.orders[idx];
            let maker_quantity = level.quantity[idx];
            let traded = taker.quantity.min(maker_quantity);
            taker.quantity -= traded;
            let new_maker_quantity = maker_quantity.saturating_sub(traded);
            level.reduce_quantity(idx, new_maker_quantity)?;

            if self.trade_len >= 50 {
                warn!("Trade Buffer full");
                break;
            };
            self.trade_buf[self.trade_len].write(
                TradeMsg{
                    price: price,
                    quantity: traded,
                    maker_id: maker_id,
                    taker_id: taker.order_id,
                    timestamp
                }
            );

            self.trade_len +=1;

            executed_trades.push(ExecutedOrder{
                maker_id: maker_id,
                taker_id: taker.order_id,
                price: price,
                quantity: traded,
                timestamp: timestamp,
            });

            if new_maker_quantity == 0{
                level.remove_order(idx)?;
                // break;
            }
            else{
                idx +=1;
            }
        }

            // if level.is_empty(){
            if level.total_quantity==0{
                remove_price.push(price);
            }
            
            if self.trade_len > 50 {
                warn!("Trade Buffer full");
                break;
            };
        }

        for price in remove_price{
            book.remove(&price);
        }        
    }

    if taker.quantity>0{
        self.insert_resting_order(taker)?;
    }
    self.depth_cache.stale = true;
    Ok(executed_trades)
    
    }

    // #[inline]
    // pub fn flush_trade()->Result<()>{

    //     Ok(())
    // }

    #[inline]
    pub fn insert_resting_order(&mut self, order: Order)->Result<()>{
        order.validate()?;
        let match_buy = matches!(order.side , Side::Buy);
        let book = if match_buy{
            &mut self.bids
        }
        else{
            &mut self.asks
        };
        
        let level = book.entry(order.price).or_insert_with(PriceLevel::new);
        let idx = level.orders.len(); //here the len of roders will the be next index in that array
        level.push(&order);
        self.order_location.insert(
            // order.price,
            order.order_id,
            OrderLocation{
            side: order.side,
            price: order.price,
            index: idx
            }
        );

        self.depth_cache.stale = true;
        Ok(())

    }

    #[inline]
    pub fn delete_order(&mut self, order_id: u64)->Result<()>{

        let loc = self.order_location.remove(&order_id).ok_or(OrderbookError::OrderDoNotExist)?;
        let match_buy = matches!(loc.side, Side::Buy);
        let book = if match_buy{
            &mut self.bids
        }
        else{
            &mut self.asks
        };
        let price = loc.price;
        let price_level = book.get_mut(&price); 
        if let Some(l) = price_level{
            l.remove_order(loc.index);
            if l.total_quantity ==0{
            book.remove(&price);
        }
    }

    self.depth_cache.stale = true;
        Ok(())
    }

    pub fn get_depth(&mut self, limit: usize)->Depth{
        if self.depth_cache.stale{
            self.rebuild_depth_cache();
        };

        let bids = self.depth_cache.bids[..self.depth_cache.bid_count.min(limit)].to_vec();
        let asks = self.depth_cache.asks[..self.depth_cache.ask_count.min(limit)].to_vec();
        // println!("depth in orderbook asks {:?}, bids- {:?}", asks, bids);
        Depth{
            bids,
            asks,
            last_update_id: "0".to_string(),
        }

    }

    #[inline]
    pub fn rebuild_depth_cache(&mut self){
        self.depth_cache.bid_count = 0;
        for (&price, level) in self.bids.iter().rev().take(20){
            self.depth_cache.bids[self.depth_cache.bid_count] = [price, level.total_quantity];
            self.depth_cache.bid_count +=1;
        }

        self.depth_cache.ask_count = 0;
        for (&price, level) in self.asks.iter().take(20){
            self.depth_cache.asks[self.depth_cache.ask_count] = [price, level.total_quantity];
            self.depth_cache.ask_count +=1;
        }

        self.depth_cache.stale = false;
    }

    // #[inline]
    // pub fn drop_orderbook(&mut self)->Result<()>{
    //     if 
    //     Ok(())
    // }

       
}