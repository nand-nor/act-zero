//! `glommio`-specific functionality

use crate::{timer, Actor, Addr};
use futures::{
    task::{Spawn, SpawnError},
    Future,
};
use glommio::{executor, timer::Timer as GlommioTimer, Latency, Shares, TaskQueueHandle};
use std::{pin::Pin, task::Poll, time::Instant};

/// Type representing the glommio runtime.
/// Includes a task queue onto which tasks
/// can be spawned as needed
#[derive(Debug, Copy, Clone, Default)]
pub struct Runtime {
    pub(crate) tq: TaskQueueHandle,
}

impl Runtime {
    /// A new glommio `Runtime` object requires a `TaskQueueHandle`
    pub fn new(tq: TaskQueueHandle) -> Self {
        Self { tq }
    }

    pub(crate) fn spawn<F>(&self, f: F) -> Result<glommio::Task<F::Output>, ()>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        glommio::spawn_local_into(f, self.tq).map_err(|e| {
            panic!("Error spawning future: {:?}", e);
        })
    }
}

/// Alias for a timer based on glommio. This type can be default-constructed.
pub type Timer = timer::Timer<Runtime>;

/// Provides an infallible way to spawn an actor onto the glommio runtime via dedicated task queue,
/// equivalent to `Addr::new`. To specify the latency and shares for the task queue, use the
/// `spawn_actor_with_tq` method
pub fn spawn_actor<T: Actor>(actor: T) -> Addr<T> {
    let tq = executor().create_task_queue(Shares::default(), Latency::NotImportant, "actor-tq");
    Addr::new(&Runtime { tq }, actor).unwrap()
}

/// Provides an infallible way to spawn an actor onto the glommio runtime via dedicated task queue,
/// equivalent to `Addr::new`. To specify the latency and shares for the task queue, use the
/// `spawn_actor_with_tq` method
pub fn spawn_actor_with_tq<T: Actor>(actor: T, tq: TaskQueueHandle) -> Addr<T> {
    Addr::new(&Runtime { tq }, actor).unwrap()
}

impl Spawn for Runtime {
    fn spawn_obj(&self, future: futures::future::FutureObj<'static, ()>) -> Result<(), SpawnError> {
        self.spawn(future).map(|task| task.detach()).ok();
        Ok(())
    }
}

/// Special struct needed to support
/// polling glommio timers
pub struct GlommioSleep {
    #[doc(hidden)]
    pub inner: GlommioTimer,
}

impl Future for GlommioSleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match Pin::new(&mut self.inner).poll(cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

unsafe impl Send for GlommioSleep {}
unsafe impl Sync for GlommioSleep {}

impl timer::SupportsTimers for Runtime {
    type Delay = Pin<Box<GlommioSleep>>;
    fn delay(&self, deadline: Instant) -> Self::Delay {
        let duration = deadline.saturating_duration_since(Instant::now());
        Box::pin(GlommioSleep {
            inner: GlommioTimer::new(duration),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::*;
    use glommio::{executor, Latency, LocalExecutorBuilder, Placement, Shares, Task};
    struct Echo;

    impl Actor for Echo {}
    impl Echo {
        async fn echo(&mut self, x: &'static str) -> ActorResult<&'static str> {
            Produces::ok(x)
        }
    }

    #[test]
    fn smoke_test() {
        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .name(&format!("{}{}", "test-handle", 0))
            .spawn(move || async move {
                // use default task queue handle builder
                let addr = spawn_actor(Echo);
                let res = call!(addr.echo("test")).await.unwrap();
                assert_eq!(res, "test");
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn wait_drop_test() {
        use std::time::Duration;
        struct WaitDrop {
            tx: std::sync::mpsc::SyncSender<u32>,
        }
        impl Actor for WaitDrop {}
        impl Drop for WaitDrop {
            fn drop(&mut self) {
                std::thread::sleep(Duration::from_millis(100));
                self.tx.send(5).unwrap();
            }
        }
        let (tx, rx) = std::sync::mpsc::sync_channel(1);

        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .name(&format!("{}{}", "test-handle", 0))
            .spawn(move || async move {
                let tq = executor().create_task_queue(
                    Shares::Static(2),
                    Latency::Matters(Duration::from_millis(1)),
                    "actor-tq",
                );
                let addr = spawn_actor_with_tq(WaitDrop { tx }, tq);
                let ended = addr.termination();
                drop(addr);
                ended.await;
                let res = rx.try_recv();
                assert_eq!(res, Ok(5));
            })
            .unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn weak_timer_test() {
        use std::time::{Duration, Instant};

        use async_trait::async_trait;
        use futures::channel::oneshot;

        #[derive(Default)]
        struct DebouncedEcho {
            addr: WeakAddr<Self>,
            timer: Timer,
            response: Option<(&'static str, oneshot::Sender<&'static str>)>,
        }

        #[async_trait]
        impl Actor for DebouncedEcho {
            async fn started(&mut self, addr: Addr<Self>) -> ActorResult<()> {
                self.addr = addr.downgrade();
                Produces::ok(())
            }
        }

        #[async_trait]
        impl timer::Tick for DebouncedEcho {
            async fn tick(&mut self) -> ActorResult<()> {
                if self.timer.tick() {
                    let (msg, tx) = self.response.take().unwrap();
                    let _ = tx.send(msg);
                }
                Produces::ok(())
            }
        }
        impl DebouncedEcho {
            async fn echo(
                &mut self,
                msg: &'static str,
            ) -> ActorResult<oneshot::Receiver<&'static str>> {
                let (tx, rx) = oneshot::channel();
                self.response = Some((msg, tx));
                self.timer
                    .set_timeout_for_weak(self.addr.clone(), Duration::from_secs(1));
                Produces::ok(rx)
            }
        }

        let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
            .name(&format!("{}{}", "test-handle", 0))
            .spawn(move || async move {
                let tq = executor().create_task_queue(
                    Shares::Static(100),
                    Latency::Matters(Duration::from_millis(1)),
                    "actor-tq",
                );
                let addr = spawn_actor_with_tq(DebouncedEcho::default(), tq);

                let start_time = Instant::now();
                let res = call!(addr.echo("test")).await.unwrap();
                drop(addr);

                assert!(res.await.is_err());
                let end_time = Instant::now();

                assert!(end_time - start_time < Duration::from_millis(10));
            })
            .unwrap();

        handle.join().unwrap();
    }
}
