//! Request-direction sender ownership.
//!
//! Request services coordinate product offsets and path intents. TCP receipt
//! calibration and QUIC packet-ACK calibration remain separate controllers
//! that emit shared eligibility evidence to this layer.

use super::*;

mod service;
mod startup;

pub(in crate::runtime) use service::*;
use startup::*;
