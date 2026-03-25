use alloy::eips::BlockHashOrNumber;
use alloy::primitives::{Address, B256, TxHash, TxNonce};
use jsonrpsee::core::RpcResult;
use jsonrpsee::proc_macros::rpc;
use zksync_os_storage_api::{PersistedBatch, ReplayRecord, RepositoryBlock, StoredTxData, TxMeta};
use zksync_os_types::{ZkReceiptEnvelope, ZkTransaction};

#[cfg_attr(not(feature = "server"), rpc(client, namespace = "unstable"))]
#[cfg_attr(feature = "server", rpc(server, client, namespace = "unstable"))]
pub trait UnstableApi {
    #[method(name = "getBatchByBlockNumber")]
    async fn get_batch_by_block_number(&self, block_number: u64) -> RpcResult<PersistedBatch>;

    #[method(name = "getLocalRoot")]
    async fn get_local_root(&self, batch_number: u64) -> RpcResult<B256>;

    #[method(name = "getStorageValue")]
    async fn get_storage_value(&self, block_number: u64, key: B256) -> RpcResult<Option<B256>>;

    #[method(name = "getPreimage")]
    async fn get_preimage(&self, hash: B256) -> RpcResult<Option<Vec<u8>>>;

    #[method(name = "getRepositoryBlock")]
    async fn get_repository_block(
        &self,
        block: BlockHashOrNumber,
    ) -> RpcResult<Option<RepositoryBlock>>;

    #[method(name = "getRawTransaction")]
    async fn get_raw_transaction(&self, hash: TxHash) -> RpcResult<Option<Vec<u8>>>;

    #[method(name = "getTransaction")]
    async fn get_transaction(&self, hash: TxHash) -> RpcResult<Option<ZkTransaction>>;

    #[method(name = "getTransactionReceipt")]
    async fn get_transaction_receipt(&self, hash: TxHash) -> RpcResult<Option<ZkReceiptEnvelope>>;

    #[method(name = "getTransactionMeta")]
    async fn get_transaction_meta(&self, hash: TxHash) -> RpcResult<Option<TxMeta>>;

    #[method(name = "getTransactionHashBySenderNonce")]
    async fn get_transaction_hash_by_sender_nonce(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> RpcResult<Option<TxHash>>;

    #[method(name = "getStoredTransaction")]
    async fn get_stored_transaction(&self, hash: TxHash) -> RpcResult<Option<StoredTxData>>;

    #[method(name = "getLatestBlockNumber")]
    async fn get_latest_block_number(&self) -> RpcResult<u64>;

    #[method(name = "getReplayRecord")]
    async fn get_replay_record(&self, block_number: u64) -> RpcResult<Option<ReplayRecord>>;

    #[method(name = "getLatestReplayRecordNumber")]
    async fn get_latest_replay_record_number(&self) -> RpcResult<u64>;
}
