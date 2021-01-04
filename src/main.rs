#[macro_use]
extern crate serde_derive;

use async_std::task;
use futures::channel::mpsc::channel;
use log::*;
use std::path::PathBuf;
mod errors;
mod schemas;
mod settings;
mod streams;

fn main() {
    pretty_env_logger::init();

    let settings = settings::load_settings();

    let (sender, receiver) = channel::<streams::DispatchMessage>(settings.internal.sendbuffer);

    let _: Vec<_> = settings
        .topics
        .iter()
        .map(|t| {
            info!("Starting stream for {:?}", t.name);
            task::spawn(streams::consume_topic(
                settings.kafka.clone(),
                t.clone(),
                PathBuf::from("schemas.d"),
                sender.clone(),
            ))
        })
        .collect();

    let producer_handle = streams::produce(settings.kafka.clone(), receiver);

    task::block_on(producer_handle);
}
