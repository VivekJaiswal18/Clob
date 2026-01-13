use thiserror::Error;

#[derive(Error, Debug)]
pub enum OrderbookError{
    #[error("Order price cannot be 0")]
    PriceError,
    #[error("Order quantity cannot be 0")]
    QuantityError,
    #[error("Index is out of bound")]
    IndexError,
    #[error("New order quantity is exceeding old order quantity")]
    OrderQuantityExceeded,
    #[error("This order does not exist in the orderbook")]
    OrderDoNotExist,
    #[error("Depth Cache is stale")]
    DepthCacheStale
}

pub type Result<T> = std::result::Result<T, OrderbookError>;