use crate::transaction::system::envelope::SystemTransactionEnvelope;
use alloy::primitives::{Address, address};
use serde::{Deserialize, Serialize};

pub mod envelope;
pub mod tx;

pub const BOOTLOADER_FORMAL_ADDRESS: Address =
    address!("0x0000000000000000000000000000000000008001");

pub type InteropRootsEnvelope = SystemTransactionEnvelope<InteropRootsTxType>;

impl InteropRootsEnvelope {
    pub fn interop_roots_count(&self) -> u64 {
        unimplemented!("implement me");
    }
}

pub trait SystemTxType: Clone + Send + Sync + std::fmt::Debug + 'static {
    const TX_TYPE: u8;
}

#[derive(Debug, Clone, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct InteropRootsTxType;

impl SystemTxType for InteropRootsTxType {
    const TX_TYPE: u8 = 0x7d;
}
