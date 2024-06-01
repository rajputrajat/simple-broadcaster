use std::{
    borrow::Cow,
    fmt::Debug,
    sync::{mpsc, Arc, Mutex},
};
use thiserror::Error as ThisError;
use tracing::{self, error, trace, warn};

#[derive(Copy, Debug)]
struct UniqueId(u64);

impl UniqueId {
    const fn new() -> Self {
        Self(1)
    }

    const fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl Clone for UniqueId {
    #[allow(clippy::non_canonical_clone_impl)]
    fn clone(&self) -> Self {
        self.next()
    }
}

struct MpscSyncSender<T> {
    id: UniqueId,
    inner: mpsc::SyncSender<T>,
}

impl<T> Debug for MpscSyncSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("SenderId # {}", self.id.0))
    }
}

struct MpscReceiver<T> {
    id: UniqueId,
    inner: mpsc::Receiver<T>,
}

impl<T> Debug for MpscReceiver<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("ReceiverId # {}", self.id.0))
    }
}

struct BroadcasterInner<T> {
    name: Cow<'static, str>,
    last_id: UniqueId,
    senders: Vec<MpscSyncSender<T>>,
}

impl<T> Debug for BroadcasterInner<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Broadcaster inner {{ name: {:?}, sender_cnt: {} }}",
            self.name,
            self.senders.len()
        ))
    }
}

impl<T: Clone + Debug> BroadcasterInner<T> {
    fn broadcast(&self, message: T) {
        trace!(
            "broadcasting '{message:?}' to '{}' senders",
            self.senders.len()
        );
        for sender in &self.senders {
            trace!("sender # {sender:?} is sending");
            if sender.inner.send(message.clone()).is_err() {
                warn!("sender # {sender:?} failed while broadcasting");
            }
        }
    }

    fn add_receiver(&mut self) -> MpscReceiver<T> {
        let (sender, receiver) = mpsc::sync_channel(10);
        self.last_id = self.last_id.next();
        self.senders.push(MpscSyncSender {
            id: self.last_id,
            inner: sender,
        });
        trace!(
            "added a new receiver # {:?}. new sender count: '{}'",
            self.last_id,
            self.senders.len()
        );
        MpscReceiver {
            id: self.last_id,
            inner: receiver,
        }
    }
}

pub fn broadcasting_channel<T: Clone, N>(name: N) -> (Broadcaster<T>, Subscriber<T>)
where
    N: Into<Cow<'static, str>>,
{
    let (sender, receiver) = mpsc::sync_channel(10);
    let last_id = UniqueId::new();
    let inner = Arc::new(Mutex::new(BroadcasterInner {
        name: name.into(),
        last_id,
        senders: vec![MpscSyncSender {
            id: last_id,
            inner: sender,
        }],
    }));
    let receiver = MpscReceiver {
        id: last_id,
        inner: receiver,
    };
    (
        Broadcaster {
            name: Cow::Borrowed("initial"),
            inner: inner.clone(),
        },
        Subscriber {
            name: Cow::Borrowed("initial"),
            inner,
            receiver,
        },
    )
}

pub struct Broadcaster<T> {
    name: Cow<'static, str>,
    inner: Arc<Mutex<BroadcasterInner<T>>>,
}

impl<T> Debug for Broadcaster<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Broadcaster {{ name: {:?}, inner: {:?} }}",
            self.name, self.inner
        ))
    }
}

pub trait CloneAs<N>
where
    N: Into<Cow<'static, str>>,
{
    fn clone_as(&self, name: N) -> Self;
}

impl<T: Debug + Clone> Clone for Broadcaster<T> {
    fn clone(&self) -> Self {
        self.clone_as(format!("anonymous. cloned from {}", self.name))
    }
}

impl<T: Clone + Debug> Broadcaster<T> {
    pub fn broadcast(&self, message: T) {
        trace!("broadcaster {:?} is broadcasting '{message:?}'", self.name);
        #[allow(clippy::unwrap_used)]
        (*self.inner).lock().unwrap().broadcast(message);
    }
}

impl<T, N> CloneAs<N> for Broadcaster<T>
where
    T: Clone + Debug,
    N: Into<Cow<'static, str>>,
{
    fn clone_as(&self, name: N) -> Self {
        let name = name.into();
        trace!(
            "Broadcaster {name:?} is cloned from {:?}. total count # {}",
            self.name,
            Arc::strong_count(&self.inner)
        );
        Self {
            name,
            inner: self.inner.clone(),
        }
    }
}

pub struct Subscriber<T> {
    name: Cow<'static, str>,
    inner: Arc<Mutex<BroadcasterInner<T>>>,
    receiver: MpscReceiver<T>,
}

impl<T: Debug + Clone> Subscriber<T> {
    pub fn try_recv(&self) -> Result<T, Error<T>> {
        trace!("subscriber {:?} is going to recv now", self.name);
        let value = self.receiver.inner.try_recv()?;
        trace!("subscriber {:?} has received '{value:?}'", self.name);
        Ok(value)
    }

    pub fn recv(&self) -> Result<T, Error<T>> {
        trace!("subscriber {:?} is going to recv now", self.name);
        let value = self.receiver.inner.recv()?;
        trace!("subscriber {:?} has received '{value:?}'", self.name);
        Ok(value)
    }
}

impl<T, N> CloneAs<N> for Subscriber<T>
where
    T: Clone + Debug,
    N: Into<Cow<'static, str>>,
{
    fn clone_as(&self, name: N) -> Self {
        #[allow(clippy::unwrap_used)]
        let receiver = (*self.inner).lock().unwrap().add_receiver();
        let name = name.into();
        trace!(
            "Subscriber - {name:?} is cloned from {:?}. new subscriber # {receiver:?}",
            self.name
        );
        Self {
            name,
            inner: self.inner.clone(),
            receiver,
        }
    }
}

impl<T> Debug for Subscriber<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Subscriber {{ name: {:?}, inner: {:?} }}",
            self.name, self.inner
        ))
    }
}

impl<T: Debug + Clone> Clone for Subscriber<T> {
    fn clone(&self) -> Self {
        self.clone_as(format!("anonymous. cloned from {}", self.name))
    }
}

#[derive(Debug, ThisError)]
pub enum Error<T> {
    #[error("receiver is not there in the option")]
    NoReceiver,
    #[error(transparent)]
    RecvError(#[from] mpsc::RecvError),
    #[error(transparent)]
    SendError(#[from] mpsc::SendError<T>),
    #[error(transparent)]
    TryRecvError(#[from] mpsc::TryRecvError),
}

#[derive(Debug, Clone)]
pub struct Canceller(pub Subscriber<()>);

impl From<Subscriber<()>> for Canceller {
    fn from(value: Subscriber<()>) -> Self {
        Self(value)
    }
}

impl<N> CloneAs<N> for Canceller
where
    N: Into<Cow<'static, str>>,
{
    fn clone_as(&self, name: N) -> Self {
        Self(self.0.clone_as(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result as AnyResult;
    use std::thread;

    #[test]
    fn broadcaster_test_simple_drop_1() -> AnyResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        const FIRST: i32 = 10;
        let (b, s) = broadcasting_channel("test 1");
        let b1 = b.clone_as("first broadcaster copy");
        let s1 = s.clone_as("first subscriber copy");
        let v_s = (0..100)
            .map(|i| s.clone_as(format!("sub#{i}")))
            .collect::<Vec<_>>();
        thread::spawn(move || {
            b1.broadcast(FIRST);
        });
        assert_eq!(s1.recv()?, FIRST);
        assert_eq!(s.recv()?, FIRST);
        drop(b);
        drop(s);
        drop(s1);
        for s_elem in &v_s {
            assert_eq!(s_elem.recv()?, FIRST);
        }
        Ok(())
    }

    #[test]
    fn broadcaster_test_simple_drop_2() -> AnyResult<()> {
        let _ = tracing_subscriber::fmt::try_init();
        const FIRST: i32 = 10;
        const SECOND: i32 = 5;
        let (b, s) = broadcasting_channel("test 2");
        let b1 = b.clone_as("first broadcaster clone");
        let s1 = s.clone_as("first subscriber clone");
        let v_s = (0..100)
            .map(|i| s.clone_as(format!("sub#{i}")))
            .collect::<Vec<_>>();
        thread::spawn(move || {
            b1.broadcast(FIRST);
            b1.broadcast(SECOND);
        });
        assert_eq!(s1.recv()?, FIRST);
        assert_eq!(s.recv()?, FIRST);
        drop(b);
        assert_eq!(s.recv()?, SECOND);
        drop(s);
        assert_eq!(s1.recv()?, SECOND);
        drop(s1);
        for s_elem in &v_s {
            assert_eq!(s_elem.recv()?, FIRST);
        }
        for s_elem in &v_s {
            assert_eq!(s_elem.recv()?, SECOND);
        }
        Ok(())
    }

    #[test]
    fn broadcaster_test_simple_drop_3() {
        let _ = tracing_subscriber::fmt::try_init();
        let (b, s) = broadcasting_channel("test 2");
        let b_1 = b.clone_as("b 1");
        thread::spawn(move || {
            thread::spawn(move || {
                for i in 0..100 {
                    let s = s.clone_as(format!("looped sub # {i}"));
                    thread::spawn(move || {
                        trace!("{:?} received {:?}", s, s.recv());
                    });
                }
            });
            thread::spawn(move || {
                b_1.broadcast("hello there");
            });
        });
    }
}
