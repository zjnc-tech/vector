mod limited_queue;
mod receiver;
mod sender;

pub use limited_queue::TrySendError as LimitedTrySendError;
pub use limited_queue::{limited, LimitedReceiver, LimitedSender, SendError};
pub use receiver::*;
pub use sender::*;

#[cfg(test)]
mod tests;
