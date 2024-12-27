//! This example shows how you can use the glommio runtime with act-zero.

use act_zero::runtimes::glommio::{spawn_actor, spawn_actor_with_tq};
use act_zero::*;
use glommio::{executor, Latency, LocalExecutorBuilder, Placement, Shares};

struct HelloWorldActor(usize);

impl Actor for HelloWorldActor {}

impl HelloWorldActor {
    async fn say_hello(&mut self) {
        println!("Hello, world! {}", self.0);
    }
}

fn main() -> Result<(), ActorError> {
    let handle = LocalExecutorBuilder::new(Placement::Fixed(0))
        .name(&format!("{}{}", "actor-rt-handle", 0))
        .spawn(move || async move {
            let addr = spawn_actor(HelloWorldActor(0));
            call!(addr.say_hello()).await.unwrap();

            // create another tq with specified shares and latency;
            // the spawn_actor method creates a tq with default
            // shares and latency
            let other_tq = executor().create_task_queue(
                Shares::Static(2),
                Latency::Matters(std::time::Duration::from_millis(1)),
                "other-actor-tq",
            );

            let addr = spawn_actor_with_tq(HelloWorldActor(1), other_tq);
            call!(addr.say_hello()).await.unwrap();
        })
        .unwrap();
    handle.join().unwrap();

    Ok(())
}
