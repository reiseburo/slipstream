#[macro_use]
extern crate serde_derive;

use async_std::sync::Arc;
use config;
use log::*;

mod settings;

#[async_std::main]
async fn main() {
    pretty_env_logger::init();
    let settings = Arc::new(settings::load_settings());
}
