use crate::ReadRpcStorage;
use crate::result::ToRpcResult;
use alloy::eips::BlockHashOrNumber;
use alloy::primitives::{Address, B256, BlockNumber, TxHash, TxNonce};
use async_trait::async_trait;
use jsonrpsee::core::RpcResult;
use zksync_os_interface::traits::{PreimageSource, ReadStorage};
use zksync_os_mini_merkle_tree::MiniMerkleTree;
use zksync_os_rpc_api::unstable::UnstableApiServer;
use zksync_os_storage_api::{PersistedBatch, RepositoryError, StateError};
use zksync_os_types::L2_TO_L1_TREE_SIZE;

pub struct UnstableNamespace<RpcStorage> {
    storage: RpcStorage,
}

impl<RpcStorage> UnstableNamespace<RpcStorage> {
    pub fn new(storage: RpcStorage) -> Self {
        Self { storage }
    }
}

impl<RpcStorage: ReadRpcStorage> UnstableNamespace<RpcStorage> {
    fn get_batch_by_block_number_impl(&self, block_number: u64) -> UnstableResult<PersistedBatch> {
        self.storage
            .batch()
            .get_batch_by_block_number(block_number)?
            .ok_or(UnstableError::BatchNotAvailableYet)
    }

    fn get_local_root_impl(&self, batch_number: u64) -> UnstableResult<B256> {
        let batch = self
            .storage
            .batch()
            .get_batch_by_number(batch_number)?
            .ok_or(UnstableError::BatchNotAvailableYet)?;

        let mut merkle_tree_leaves = vec![];
        for block in batch.block_range.clone() {
            let Some(block) = self.storage.repository().get_block_by_number(block)? else {
                return Err(UnstableError::BlockNotAvailable(block));
            };
            for block_tx_hash in block.unseal().body.transactions {
                let Some(receipt) = self
                    .storage
                    .repository()
                    .get_transaction_receipt(block_tx_hash)?
                else {
                    return Err(UnstableError::TxNotAvailable(block_tx_hash));
                };
                let l2_to_l1_logs = receipt.into_l2_to_l1_logs();
                for l2_to_l1_log in l2_to_l1_logs {
                    merkle_tree_leaves.push(l2_to_l1_log.encode());
                }
            }
        }

        let local_root =
            MiniMerkleTree::new(merkle_tree_leaves.into_iter(), Some(L2_TO_L1_TREE_SIZE))
                .merkle_root();

        Ok(local_root)
    }

    fn get_storage_value_impl(&self, block_number: u64, key: B256) -> UnstableResult<Option<B256>> {
        let mut state = self.storage.state_view_at(block_number)?;
        Ok(state.read(key))
    }

    fn get_preimage_impl(&self, hash: B256) -> UnstableResult<Option<Vec<u8>>> {
        let latest_block = *self.storage.block_range_available().end();
        let mut state = self.storage.state_view_at(latest_block)?;
        Ok(state.get_preimage(hash))
    }

    fn get_repository_block_impl(
        &self,
        block: BlockHashOrNumber,
    ) -> UnstableResult<Option<zksync_os_storage_api::RepositoryBlock>> {
        Ok(self.storage.get_block_by_hash_or_number(block)?)
    }

    fn get_raw_transaction_impl(&self, hash: TxHash) -> UnstableResult<Option<Vec<u8>>> {
        Ok(self.storage.repository().get_raw_transaction(hash)?)
    }

    fn get_transaction_impl(
        &self,
        hash: TxHash,
    ) -> UnstableResult<Option<zksync_os_types::ZkTransaction>> {
        Ok(self.storage.repository().get_transaction(hash)?)
    }

    fn get_transaction_receipt_impl(
        &self,
        hash: TxHash,
    ) -> UnstableResult<Option<zksync_os_types::ZkReceiptEnvelope>> {
        Ok(self.storage.repository().get_transaction_receipt(hash)?)
    }

    fn get_transaction_meta_impl(
        &self,
        hash: TxHash,
    ) -> UnstableResult<Option<zksync_os_storage_api::TxMeta>> {
        Ok(self.storage.repository().get_transaction_meta(hash)?)
    }

    fn get_transaction_hash_by_sender_nonce_impl(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> UnstableResult<Option<TxHash>> {
        Ok(self
            .storage
            .repository()
            .get_transaction_hash_by_sender_nonce(sender, nonce)?)
    }

    fn get_stored_transaction_impl(
        &self,
        hash: TxHash,
    ) -> UnstableResult<Option<zksync_os_storage_api::StoredTxData>> {
        Ok(self.storage.repository().get_stored_transaction(hash)?)
    }

    fn get_latest_block_number_impl(&self) -> u64 {
        self.storage.repository().get_latest_block()
    }

    fn get_replay_record_impl(
        &self,
        block_number: u64,
    ) -> Option<zksync_os_storage_api::ReplayRecord> {
        self.storage
            .replay_storage()
            .get_replay_record(block_number)
    }

    fn get_latest_replay_record_number_impl(&self) -> u64 {
        self.storage.replay_storage().latest_record()
    }
}

#[async_trait]
impl<RpcStorage: ReadRpcStorage> UnstableApiServer for UnstableNamespace<RpcStorage> {
    async fn get_batch_by_block_number(&self, block_number: u64) -> RpcResult<PersistedBatch> {
        self.get_batch_by_block_number_impl(block_number)
            .to_rpc_result()
    }

    async fn get_local_root(&self, batch_number: u64) -> RpcResult<B256> {
        self.get_local_root_impl(batch_number).to_rpc_result()
    }

    async fn get_storage_value(&self, block_number: u64, key: B256) -> RpcResult<Option<B256>> {
        self.get_storage_value_impl(block_number, key)
            .to_rpc_result()
    }

    async fn get_preimage(&self, hash: B256) -> RpcResult<Option<Vec<u8>>> {
        self.get_preimage_impl(hash).to_rpc_result()
    }

    async fn get_repository_block(
        &self,
        block: BlockHashOrNumber,
    ) -> RpcResult<Option<zksync_os_storage_api::RepositoryBlock>> {
        self.get_repository_block_impl(block).to_rpc_result()
    }

    async fn get_raw_transaction(&self, hash: TxHash) -> RpcResult<Option<Vec<u8>>> {
        self.get_raw_transaction_impl(hash).to_rpc_result()
    }

    async fn get_transaction(
        &self,
        hash: TxHash,
    ) -> RpcResult<Option<zksync_os_types::ZkTransaction>> {
        self.get_transaction_impl(hash).to_rpc_result()
    }

    async fn get_transaction_receipt(
        &self,
        hash: TxHash,
    ) -> RpcResult<Option<zksync_os_types::ZkReceiptEnvelope>> {
        self.get_transaction_receipt_impl(hash).to_rpc_result()
    }

    async fn get_transaction_meta(
        &self,
        hash: TxHash,
    ) -> RpcResult<Option<zksync_os_storage_api::TxMeta>> {
        self.get_transaction_meta_impl(hash).to_rpc_result()
    }

    async fn get_transaction_hash_by_sender_nonce(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> RpcResult<Option<TxHash>> {
        self.get_transaction_hash_by_sender_nonce_impl(sender, nonce)
            .to_rpc_result()
    }

    async fn get_stored_transaction(
        &self,
        hash: TxHash,
    ) -> RpcResult<Option<zksync_os_storage_api::StoredTxData>> {
        self.get_stored_transaction_impl(hash).to_rpc_result()
    }

    async fn get_latest_block_number(&self) -> RpcResult<u64> {
        Ok(self.get_latest_block_number_impl())
    }

    async fn get_replay_record(
        &self,
        block_number: u64,
    ) -> RpcResult<Option<zksync_os_storage_api::ReplayRecord>> {
        Ok(self.get_replay_record_impl(block_number))
    }

    async fn get_latest_replay_record_number(&self) -> RpcResult<u64> {
        Ok(self.get_latest_replay_record_number_impl())
    }
}

/// `unstable` namespace result type.
pub type UnstableResult<Ok> = Result<Ok, UnstableError>;

/// General `unstable` namespace errors
#[derive(Debug, thiserror::Error)]
pub enum UnstableError {
    #[error(
        "L1 batch containing the transaction has not been finalized or indexed by this node yet"
    )]
    BatchNotAvailableYet,
    #[error(transparent)]
    Batch(#[from] anyhow::Error),
    #[error(transparent)]
    State(#[from] StateError),
    /// Historical block could not be found on this node (e.g., pruned).
    #[error("historical block {0} is not available")]
    BlockNotAvailable(BlockNumber),
    /// Historical transaction could not be found on this node (e.g., pruned).
    #[error("historical transaction {0} is not available")]
    TxNotAvailable(TxHash),
    #[error(transparent)]
    Repository(#[from] RepositoryError),
}
