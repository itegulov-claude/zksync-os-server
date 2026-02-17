use crate::{ReadReplay, ReplayRecord};
use alloy::primitives::BlockNumber;
use std::collections::HashMap;
use zksync_os_interface::types::BlockContext;

#[derive(Debug, Clone, Default)]
pub struct InMemReplay(HashMap<BlockNumber, ReplayRecord>);

impl FromIterator<(BlockNumber, ReplayRecord)> for InMemReplay {
    fn from_iter<I: IntoIterator<Item = (BlockNumber, ReplayRecord)>>(iter: I) -> Self {
        Self(HashMap::from_iter(iter))
    }
}

impl ReadReplay for InMemReplay {
    fn get_context(&self, block_number: BlockNumber) -> Option<BlockContext> {
        self.0.get(&block_number).map(|r| r.block_context)
    }

    fn get_replay_record_by_key(
        &self,
        block_number: BlockNumber,
        _db_key: Option<Vec<u8>>,
    ) -> Option<ReplayRecord> {
        self.0.get(&block_number).cloned()
    }

    fn latest_record(&self) -> BlockNumber {
        self.0.keys().last().copied().unwrap_or_default()
    }
}
