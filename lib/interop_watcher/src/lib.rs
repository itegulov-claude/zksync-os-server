use std::time::Duration;

use alloy::rpc::types::Filter;
use alloy::sol_types::SolEvent;
use alloy::{
    primitives::Address,
    providers::{DynProvider, Provider},
};
use tokio::sync::mpsc;
use zksync_os_contract_interface::IMessageRoot::NewInteropRoot;
use zksync_os_contract_interface::{Bridgehub, InteropRoot};
use zksync_os_types::{InteropRootsEnvelope, InteropRootsLogIndex};

pub const INTEROP_ROOTS_PER_IMPORT: u64 = 100;
const LOOKBEHIND_BLOCKS: u64 = 1000;

pub struct InteropRootsWatcher {
    contract_address: Address,
    provider: DynProvider,
    // first number is block number, second is log index
    next_log_to_scan_from: InteropRootsLogIndex,
    output: mpsc::Sender<InteropRootsEnvelope>,
}

impl InteropRootsWatcher {
    pub async fn new(
        bridgehub: Bridgehub<DynProvider>,
        output: mpsc::Sender<InteropRootsEnvelope>,
        next_log_to_scan_from: InteropRootsLogIndex,
    ) -> anyhow::Result<Self> {
        let provider = bridgehub.provider().clone();
        let contract_address = bridgehub
            .message_root()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get message root: {}", e))?;

        Ok(Self {
            provider,
            contract_address,
            next_log_to_scan_from,
            output,
        })
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        let mut timer = tokio::time::interval(Duration::from_secs(5));
        loop {
            timer.tick().await;
            self.poll().await?;
        }
    }

    async fn fetch_events(
        &mut self,
        start_log_index: InteropRootsLogIndex,
        to_block: u64,
    ) -> anyhow::Result<(Vec<InteropRoot>, InteropRootsLogIndex)> {
        // todo: add binary search here to manage giant amount of logs
        let filter = Filter::new()
            .from_block(start_log_index.block_number)
            .to_block(to_block)
            .address(self.contract_address)
            .event_signature(NewInteropRoot::SIGNATURE_HASH);
        let logs = self.provider.get_logs(&filter).await?;

        if logs.is_empty() {
            return Ok((Vec::new(), InteropRootsLogIndex::default()));
        }

        let mut interop_roots = Vec::new();
        for log in &logs {
            let log_index = InteropRootsLogIndex {
                block_number: log.block_number.unwrap(),
                index_in_block: log.log_index.unwrap(),
            };

            if log_index < start_log_index {
                continue;
            }
            let interop_root_event = NewInteropRoot::decode_log(&log.inner)?.data;

            anyhow::ensure!(
                interop_root_event.sides.len() == 1,
                "Expected exactly one side for interop root, found {}",
                interop_root_event.sides.len()
            );

            let interop_root = InteropRoot {
                chainId: interop_root_event.chainId,
                blockOrBatchNumber: interop_root_event.blockNumber,
                sides: interop_root_event.sides,
            };

            interop_roots.push(interop_root);

            self.next_log_to_scan_from = InteropRootsLogIndex {
                block_number: log_index.block_number,
                index_in_block: log_index.index_in_block + 1,
            };

            if interop_roots.len() >= INTEROP_ROOTS_PER_IMPORT as usize {
                break;
            }
        }

        // if we didn't get enough interop roots, it should be safe to continue from the last block we already scanned
        // edge case would be if the last root we included was already in the last block, then we should leave the value as is(it was updated before)
        if interop_roots.len() < INTEROP_ROOTS_PER_IMPORT as usize
            && self.next_log_to_scan_from.block_number < to_block
        {
            self.next_log_to_scan_from = InteropRootsLogIndex {
                block_number: to_block,
                index_in_block: 0,
            };
        }

        let last_log = logs.last().unwrap();

        let last_log_index = InteropRootsLogIndex {
            block_number: last_log.block_number.unwrap(),
            index_in_block: last_log.log_index.unwrap(),
        };

        Ok((interop_roots, last_log_index))
    }

    async fn poll(&mut self) -> anyhow::Result<()> {
        let latest_block = self.provider.get_block_number().await?;

        if self.next_log_to_scan_from.block_number + LOOKBEHIND_BLOCKS < latest_block {
            tracing::warn!(
                from_block = self.next_log_to_scan_from.block_number,
                latest_block,
                "From block is found to be behind the latest block by more than {}, it shouldn't happen normally",
                LOOKBEHIND_BLOCKS
            );
        }

        let (interop_roots, last_log_index) = self
            .fetch_events(self.next_log_to_scan_from.clone(), latest_block)
            .await?;

        if !interop_roots.is_empty() {
            self.output
                .send(InteropRootsEnvelope::from_interop_roots(
                    interop_roots,
                    last_log_index,
                ))
                .await?;
        }

        Ok(())
    }
}
