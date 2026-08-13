#[cfg(any(target_os = "android", all(test, target_os = "linux")))]
mod android;
pub mod app;
pub mod cli;
pub mod config;
pub mod dns;
pub mod ingress;
#[cfg(feature = "lab-diagnostics")]
pub(crate) mod lab_diagnostics;
mod model;
pub mod mux;
mod observability;
mod operations;
pub mod outbound;
pub mod performance;
pub mod platform;
pub mod product;
pub mod protocol;
pub mod runtime;
pub mod scheduler;
pub mod simulator;
pub mod transport;
mod update;
