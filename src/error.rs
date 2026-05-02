// ════════════════════════════════════════════════════════════════════════════
// Error types
// ════════════════════════════════════════════════════════════════════════════

/// Error returned by blocking recv operations.
///
/// # Variants
///
/// - `Disconnected`: All senders have been dropped. For broadcast channels,
///   this means no new messages will arrive.
///
/// - `Lagged { skipped }`: *Only for bounded broadcast channels.* Indicates that
///   the receiver fell behind the sender(s) and missed `skipped` messages. The
///   receiver's cursor has been advanced to the oldest available message.
///
/// # Lag Recovery
///
/// When `Lagged { skipped }` is encountered, the receiver has been automatically
/// advanced to the oldest message still in the ring buffer. Typical recovery patterns:
///
/// ```text
/// match rx.recv() {
///     Ok(msg) => process(msg),
///     Err(RecvError::Lagged { skipped }) => {
///         eprintln!("Fell behind by {} messages", skipped);
///         // Continue receiving: the next recv() will return the oldest
///         // available message, not an error.
///     }
///     Err(RecvError::Disconnected) => {
///         eprintln!("All senders disconnected");
///         break;
///     }
/// }
/// ```
///
/// To avoid lag:
/// - Ensure receiver threads wake promptly (use select! with short timeouts)
/// - Consider increasing broadcast channel capacity if lag is frequent
/// - Process messages quickly in the recv handler
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecvError {
    Disconnected,
    Lagged { skipped: usize },
}

impl std::fmt::Display for RecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecvError::Disconnected => f.write_str("receive on a disconnected channel"),
            RecvError::Lagged { skipped } => {
                write!(f, "receiver lagged and missed {} messages", skipped)
            }
        }
    }
}

impl std::error::Error for RecvError {}

/// Error returned by non-blocking try_recv operations.
///
/// # Variants
///
/// - `Empty`: Channel is currently empty (but senders are still connected).
///
/// - `Disconnected`: All senders have been dropped.
///
/// - `Lagged { skipped }`: *Only for bounded broadcast channels.* The receiver
///   fell behind and missed `skipped` messages. The receiver's cursor has been
///   automatically advanced to recover.
///
/// # Retry Strategy
///
/// A typical pattern with try_recv:
///
/// ```text
/// loop {
///     match rx.try_recv() {
///         Ok(msg) => return process(msg),
///         Err(TryRecvError::Empty) => {
///             // No message ready, come back later
///             spin_wait();
///         }
///         Err(TryRecvError::Lagged { skipped }) => {
///             eprintln!("Receiver lagged by {} messages; recovering", skipped);
///             // Continue retrying: next try_recv will get the oldest available
///         }
///         Err(TryRecvError::Disconnected) => {
///             eprintln!("Channel closed");
///             return None;
///         }
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TryRecvError {
    Empty,
    Disconnected,
    Lagged { skipped: usize },
}

impl std::fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TryRecvError::Empty => f.write_str("channel is empty"),
            TryRecvError::Disconnected => f.write_str("receive on a disconnected channel"),
            TryRecvError::Lagged { skipped } => {
                write!(f, "receiver lagged and missed {} messages", skipped)
            }
        }
    }
}

impl std::error::Error for TryRecvError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T: std::fmt::Debug> std::fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "send on a disconnected channel: {:?}", self.0)
    }
}

impl<T: std::fmt::Debug + 'static> std::error::Error for SendError<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recv_error_display() {
        assert_eq!(
            RecvError::Disconnected.to_string(),
            "receive on a disconnected channel"
        );
        assert_eq!(
            RecvError::Lagged { skipped: 3 }.to_string(),
            "receiver lagged and missed 3 messages"
        );
    }

    #[test]
    fn try_recv_error_display() {
        assert_eq!(TryRecvError::Empty.to_string(), "channel is empty");
        assert_eq!(
            TryRecvError::Disconnected.to_string(),
            "receive on a disconnected channel"
        );
        assert_eq!(
            TryRecvError::Lagged { skipped: 5 }.to_string(),
            "receiver lagged and missed 5 messages"
        );
    }

    #[test]
    fn send_error_display() {
        let err = SendError(42i32);
        assert_eq!(err.to_string(), "send on a disconnected channel: 42");
    }

    #[test]
    fn errors_implement_std_error() {
        fn assert_error<E: std::error::Error>() {}
        assert_error::<RecvError>();
        assert_error::<TryRecvError>();
        assert_error::<SendError<i32>>();
    }
}
