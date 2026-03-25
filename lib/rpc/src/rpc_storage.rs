use alloy::eips::{BlockHashOrNumber, BlockId, BlockNumberOrTag};
use alloy::primitives::{Address, B256, BlockNumber, TxHash, TxNonce};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use std::sync::Mutex;
use std::{ops::RangeInclusive, sync::Arc};
use zksync_os_interface::traits::{PreimageSource, ReadStorage};
use zksync_os_merkle_tree_api::MerkleTreeProver;
use zksync_os_rpc_api::unstable::UnstableApiClient;
use zksync_os_storage_api::{
    ReadBatch, ReadFinality, ReadReplay, ReadRepository, ReadStateHistory, ReplayRecord,
    RepositoryBlock, RepositoryError, RepositoryResult, StateError, StateResult, StoredTxData,
    TxMeta, ViewState, WriteReplay, WriteRepository, WriteState, notifications::SubscribeToBlocks,
};
use zksync_os_types::{ZkReceiptEnvelope, ZkTransaction};

pub trait ReadRpcStorage: ReadStateHistory + Clone {
    fn repository(&self) -> &dyn ReadRepository;
    fn block_subscriptions(&self) -> &dyn SubscribeToBlocks;
    fn replay_storage(&self) -> &dyn ReadReplay;
    fn finality(&self) -> &dyn ReadFinality;
    fn batch(&self) -> &dyn ReadBatch;
    fn tree(&self) -> &dyn MerkleTreeProver;

    /// Get sealed block with transaction hashes by its hash OR number.
    fn get_block_by_hash_or_number(
        &self,
        hash_or_number: BlockHashOrNumber,
    ) -> RepositoryResult<Option<RepositoryBlock>> {
        match hash_or_number {
            BlockHashOrNumber::Hash(hash) => self.repository().get_block_by_hash(hash),
            BlockHashOrNumber::Number(number) => self.repository().get_block_by_number(number),
        }
    }

    /// Resolve block's hash OR number by its id. This method can be useful when caller does not
    /// care which of the block's hash or number to deal with and wants to perform as few look-up
    /// actions as possible.
    ///
    /// WARNING: Does not ensure that the returned block's hash or number actually exists
    fn resolve_block_hash_or_number(&self, block_id: BlockId) -> BlockHashOrNumber {
        match block_id {
            BlockId::Hash(hash) => hash.block_hash.into(),
            BlockId::Number(BlockNumberOrTag::Pending) => {
                self.repository().get_latest_block().into()
            }
            BlockId::Number(BlockNumberOrTag::Latest) => {
                self.repository().get_latest_block().into()
            }
            BlockId::Number(BlockNumberOrTag::Safe) => self
                .finality()
                .get_finality_status()
                .last_committed_block
                .into(),
            BlockId::Number(BlockNumberOrTag::Finalized) => self
                .finality()
                .get_finality_status()
                .last_executed_block
                .into(),
            BlockId::Number(BlockNumberOrTag::Earliest) => {
                self.repository().get_earliest_block().into()
            }
            BlockId::Number(BlockNumberOrTag::Number(number)) => number.into(),
        }
    }

    /// Resolve block's number by its id.
    fn resolve_block_number(&self, block_id: BlockId) -> RepositoryResult<Option<BlockNumber>> {
        let block_hash_or_number = self.resolve_block_hash_or_number(block_id);
        match block_hash_or_number {
            // todo: should be possible to not load the entire block here
            BlockHashOrNumber::Hash(block_hash) => Ok(self
                .repository()
                .get_block_by_hash(block_hash)?
                .map(|header| header.number)),
            BlockHashOrNumber::Number(number) => Ok(Some(number)),
        }
    }

    /// Get sealed block with transaction hashes number by its id.
    fn get_block_by_id(&self, block_id: BlockId) -> RepositoryResult<Option<RepositoryBlock>> {
        // We presume that a reasonable number of historical blocks are being saved, so that
        // `Latest`/`Pending`/`Safe`/`Finalized` always resolve even if we don't take a look between
        // two actions below.
        let block_hash_or_number = self.resolve_block_hash_or_number(block_id);
        self.get_block_by_hash_or_number(block_hash_or_number)
    }

    fn state_at_block_id_or_latest(
        &self,
        block_id: Option<BlockId>,
    ) -> RpcStorageResult<impl ViewState> {
        let block_id = block_id.unwrap_or_default();
        let Some(block_number) = self.resolve_block_number(block_id)? else {
            return Err(RpcStorageError::BlockNotFound(block_id));
        };
        Ok(self.state_view_at(block_number)?)
    }

    /// Fetch state as stored at the end of the provided block. If there is no such block yet, then
    /// uses latest available state as a fallback (useful for pending blocks).
    fn state_at_block_number_or_latest(
        &self,
        block_number: BlockNumber,
    ) -> StateResult<impl ViewState> {
        let block_range = self.block_range_available();
        if &block_number <= block_range.end() {
            self.state_view_at(block_number)
        } else {
            self.state_view_at(*block_range.end())
        }
    }
}

#[derive(Clone)]
pub struct RpcStorage<Repository, Replay, Finality, Batch, StateHistory> {
    repository: Repository,
    replay_storage: Replay,
    finality: Finality,
    batch: Batch,
    state: StateHistory,
    tree: Arc<dyn MerkleTreeProver>,
}

impl<Repository, Replay, Finality, Batch, StateHistory> std::fmt::Debug
    for RpcStorage<Repository, Replay, Finality, Batch, StateHistory>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcStorage").finish()
    }
}

impl<Repository, Replay, Finality, Batch, StateHistory>
    RpcStorage<Repository, Replay, Finality, Batch, StateHistory>
{
    pub fn new(
        repository: Repository,
        replay_storage: Replay,
        finality: Finality,
        batch: Batch,
        state: StateHistory,
        tree: Arc<dyn MerkleTreeProver>,
    ) -> Self {
        Self {
            repository,
            replay_storage,
            finality,
            batch,
            state,
            tree,
        }
    }
}

impl<
    Repository: ReadRepository + SubscribeToBlocks + Clone,
    Replay: ReadReplay + Clone,
    Finality: ReadFinality + Clone,
    Batch: ReadBatch + Clone,
    StateHistory: ReadStateHistory + Clone,
> ReadRpcStorage for RpcStorage<Repository, Replay, Finality, Batch, StateHistory>
{
    fn repository(&self) -> &dyn ReadRepository {
        &self.repository
    }

    fn block_subscriptions(&self) -> &dyn SubscribeToBlocks {
        &self.repository
    }

    fn replay_storage(&self) -> &dyn ReadReplay {
        &self.replay_storage
    }

    fn finality(&self) -> &dyn ReadFinality {
        &self.finality
    }

    fn batch(&self) -> &dyn ReadBatch {
        &self.batch
    }

    fn tree(&self) -> &dyn MerkleTreeProver {
        self.tree.as_ref()
    }
}

impl<
    Repository: ReadRepository + SubscribeToBlocks + Clone,
    Replay: ReadReplay + Clone,
    Finality: ReadFinality + Clone,
    Batch: ReadBatch + Clone,
    StateHistory: ReadStateHistory + Clone,
> ReadStateHistory for RpcStorage<Repository, Replay, Finality, Batch, StateHistory>
{
    fn state_view_at(
        &self,
        block_number: BlockNumber,
    ) -> StateResult<impl ReadStorage + PreimageSource + Clone> {
        self.state.state_view_at(block_number)
    }

    fn block_range_available(&self) -> RangeInclusive<u64> {
        self.state.block_range_available()
    }
}

/// RPC storage result type.
pub type RpcStorageResult<Ok> = Result<Ok, RpcStorageError>;

/// Generic error type for RPC storage.
#[derive(Clone, Debug, thiserror::Error)]
pub enum RpcStorageError {
    /// Block could not be found by its id (hash/number/tag).
    #[error("block `{0}` not found")]
    BlockNotFound(BlockId),

    #[error(transparent)]
    Repository(#[from] RepositoryError),
    #[error(transparent)]
    State(#[from] StateError),
}

pub trait RemoteForkSource: std::fmt::Debug + Send + Sync {
    fn read_value(&self, block_number: BlockNumber, key: B256) -> StateResult<Option<B256>>;
    fn read_preimage(&self, hash: B256) -> StateResult<Option<Vec<u8>>>;
    fn get_repository_block(&self, block: BlockHashOrNumber) -> Option<RepositoryBlock>;
    fn get_raw_transaction(&self, hash: TxHash) -> Option<Vec<u8>>;
    fn get_transaction(&self, hash: TxHash) -> Option<ZkTransaction>;
    fn get_transaction_receipt(&self, hash: TxHash) -> Option<ZkReceiptEnvelope>;
    fn get_transaction_meta(&self, hash: TxHash) -> Option<TxMeta>;
    fn get_transaction_hash_by_sender_nonce(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> Option<TxHash>;
    fn get_stored_transaction(&self, hash: TxHash) -> Option<StoredTxData>;
    fn get_latest_block(&self) -> Option<BlockNumber>;
    fn get_replay_record(&self, block_number: BlockNumber) -> Option<ReplayRecord>;
    fn get_latest_replay_record(&self) -> Option<BlockNumber>;
}

#[derive(Clone, Debug)]
pub struct ForkedRepository<Local> {
    local: Local,
    remote_fork: Option<Arc<dyn RemoteForkSource>>,
}

impl<Local> ForkedRepository<Local> {
    pub fn new(local: Local, remote_fork: Option<Arc<dyn RemoteForkSource>>) -> Self {
        Self { local, remote_fork }
    }

    pub fn local(&self) -> Local
    where
        Local: Clone,
    {
        self.local.clone()
    }
}

impl<Local: ReadRepository> ReadRepository for ForkedRepository<Local> {
    fn get_block_by_number(
        &self,
        number: BlockNumber,
    ) -> RepositoryResult<Option<RepositoryBlock>> {
        Ok(self.local.get_block_by_number(number)?.or_else(|| {
            self.remote_fork
                .as_ref()
                .and_then(|remote| remote.get_repository_block(number.into()))
        }))
    }

    fn get_block_by_hash(
        &self,
        hash: alloy::primitives::BlockHash,
    ) -> RepositoryResult<Option<RepositoryBlock>> {
        Ok(self.local.get_block_by_hash(hash)?.or_else(|| {
            self.remote_fork
                .as_ref()
                .and_then(|remote| remote.get_repository_block(hash.into()))
        }))
    }

    fn get_raw_transaction(&self, hash: TxHash) -> RepositoryResult<Option<Vec<u8>>> {
        Ok(self.local.get_raw_transaction(hash)?.or_else(|| {
            self.remote_fork
                .as_ref()
                .and_then(|remote| remote.get_raw_transaction(hash))
        }))
    }

    fn get_transaction(&self, hash: TxHash) -> RepositoryResult<Option<ZkTransaction>> {
        Ok(self.local.get_transaction(hash)?.or_else(|| {
            self.remote_fork
                .as_ref()
                .and_then(|remote| remote.get_transaction(hash))
        }))
    }

    fn get_transaction_receipt(&self, hash: TxHash) -> RepositoryResult<Option<ZkReceiptEnvelope>> {
        Ok(self.local.get_transaction_receipt(hash)?.or_else(|| {
            self.remote_fork
                .as_ref()
                .and_then(|remote| remote.get_transaction_receipt(hash))
        }))
    }

    fn get_transaction_meta(&self, hash: TxHash) -> RepositoryResult<Option<TxMeta>> {
        Ok(self.local.get_transaction_meta(hash)?.or_else(|| {
            self.remote_fork
                .as_ref()
                .and_then(|remote| remote.get_transaction_meta(hash))
        }))
    }

    fn get_transaction_hash_by_sender_nonce(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> RepositoryResult<Option<TxHash>> {
        Ok(self
            .local
            .get_transaction_hash_by_sender_nonce(sender, nonce)?
            .or_else(|| {
                self.remote_fork
                    .as_ref()
                    .and_then(|remote| remote.get_transaction_hash_by_sender_nonce(sender, nonce))
            }))
    }

    fn get_stored_transaction(&self, hash: TxHash) -> RepositoryResult<Option<StoredTxData>> {
        Ok(self.local.get_stored_transaction(hash)?.or_else(|| {
            self.remote_fork
                .as_ref()
                .and_then(|remote| remote.get_stored_transaction(hash))
        }))
    }

    fn get_latest_block(&self) -> u64 {
        self.remote_fork
            .as_ref()
            .and_then(|remote| remote.get_latest_block())
            .map_or_else(
                || self.local.get_latest_block(),
                |remote| remote.max(self.local.get_latest_block()),
            )
    }

    fn get_earliest_block(&self) -> u64 {
        self.local.get_earliest_block()
    }
}

impl<Local: WriteRepository> WriteRepository for ForkedRepository<Local> {
    async fn populate(
        &self,
        block_output: zksync_os_interface::types::BlockOutput,
        transactions: Vec<ZkTransaction>,
    ) -> RepositoryResult<()> {
        self.local.populate(block_output, transactions).await
    }
}

impl<Local: SubscribeToBlocks> SubscribeToBlocks for ForkedRepository<Local> {
    fn subscribe_to_blocks(
        &self,
    ) -> tokio::sync::broadcast::Receiver<zksync_os_storage_api::notifications::BlockNotification>
    {
        self.local.subscribe_to_blocks()
    }
}

#[derive(Clone, Debug)]
pub struct ForkedReplayStorage<Local> {
    local: Local,
    remote_fork: Option<Arc<dyn RemoteForkSource>>,
}

impl<Local> ForkedReplayStorage<Local> {
    pub fn new(local: Local, remote_fork: Option<Arc<dyn RemoteForkSource>>) -> Self {
        Self { local, remote_fork }
    }

    pub fn local(&self) -> Local
    where
        Local: Clone,
    {
        self.local.clone()
    }
}

impl<Local: ReadReplay> ReadReplay for ForkedReplayStorage<Local> {
    fn get_context(
        &self,
        block_number: BlockNumber,
    ) -> Option<zksync_os_interface::types::BlockContext> {
        self.get_replay_record(block_number)
            .map(|record| record.block_context)
    }

    fn get_replay_record_by_key(
        &self,
        block_number: BlockNumber,
        db_key: Option<Vec<u8>>,
    ) -> Option<ReplayRecord> {
        self.local
            .get_replay_record_by_key(block_number, db_key)
            .or_else(|| {
                self.remote_fork
                    .as_ref()
                    .and_then(|remote| remote.get_replay_record(block_number))
            })
    }

    fn latest_record(&self) -> BlockNumber {
        self.remote_fork
            .as_ref()
            .and_then(|remote| remote.get_latest_replay_record())
            .map_or_else(
                || self.local.latest_record(),
                |remote| remote.max(self.local.latest_record()),
            )
    }
}

impl<Local: WriteReplay> WriteReplay for ForkedReplayStorage<Local> {
    fn write(
        &self,
        record: alloy::primitives::Sealed<ReplayRecord>,
        override_allowed: bool,
    ) -> bool {
        self.local.write(record, override_allowed)
    }
}

#[derive(Clone, Debug)]
pub struct ForkedStateHistory<Local> {
    local: Local,
    remote_fork: Option<Arc<dyn RemoteForkSource>>,
}

impl<Local> ForkedStateHistory<Local> {
    pub fn new(local: Local, remote_fork: Option<Arc<dyn RemoteForkSource>>) -> Self {
        Self { local, remote_fork }
    }

    pub fn local(&self) -> Local
    where
        Local: Clone,
    {
        self.local.clone()
    }
}

impl<Local: ReadStateHistory> ReadStateHistory for ForkedStateHistory<Local> {
    fn state_view_at(
        &self,
        block_number: BlockNumber,
    ) -> StateResult<impl ReadStorage + PreimageSource + Clone> {
        let local_state = match self.local.state_view_at(block_number) {
            Ok(state) => Some(state),
            Err(err) => {
                if self.remote_fork.is_none() {
                    return Err(err);
                }
                match err {
                    StateError::Compacted(_) | StateError::NotFound(_) => None,
                }
            }
        };

        Ok(ForkedStateView {
            local_state,
            block_number,
            remote_fork: self.remote_fork.clone(),
        })
    }

    fn block_range_available(&self) -> RangeInclusive<u64> {
        let local_range = self.local.block_range_available();
        let latest_remote_block = self
            .remote_fork
            .as_ref()
            .and_then(|remote| remote.get_latest_block());
        let range_start = if latest_remote_block.is_some() {
            0
        } else {
            *local_range.start()
        };
        let range_end =
            latest_remote_block.map_or(*local_range.end(), |remote| remote.max(*local_range.end()));
        range_start..=range_end
    }
}

impl<Local: WriteState> WriteState for ForkedStateHistory<Local> {
    fn add_block_result<'a, J>(
        &self,
        block_number: u64,
        storage_diffs: Vec<zksync_os_interface::types::StorageWrite>,
        new_preimages: J,
        override_allowed: bool,
    ) -> anyhow::Result<()>
    where
        J: IntoIterator<Item = (B256, &'a Vec<u8>)>,
    {
        self.local
            .add_block_result(block_number, storage_diffs, new_preimages, override_allowed)
    }
}

#[derive(Debug)]
pub struct RemoteForkClient {
    client: HttpClient,
    runtime: Mutex<tokio::runtime::Runtime>,
}

impl RemoteForkClient {
    pub fn new(url: &str) -> anyhow::Result<Self> {
        let client = HttpClientBuilder::new().build(url)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            client,
            runtime: Mutex::new(runtime),
        })
    }

    fn with_runtime<Output>(
        &self,
        future: impl std::future::Future<Output = Result<Output, jsonrpsee::core::ClientError>>,
        log_error: impl FnOnce(&jsonrpsee::core::ClientError),
    ) -> Option<Output> {
        let runtime = self
            .runtime
            .lock()
            .expect("remote fork runtime lock is poisoned");
        match runtime.block_on(future) {
            Ok(value) => Some(value),
            Err(err) => {
                log_error(&err);
                None
            }
        }
    }
}

impl RemoteForkSource for RemoteForkClient {
    fn read_value(&self, block_number: BlockNumber, key: B256) -> StateResult<Option<B256>> {
        let runtime = self
            .runtime
            .lock()
            .expect("remote fork runtime lock is poisoned");
        runtime
            .block_on(self.client.get_storage_value(block_number, key))
            .map_err(|err| {
                tracing::warn!(block_number, ?key, %err, "failed to fetch remote fork storage value");
                StateError::NotFound(block_number)
            })
    }

    fn read_preimage(&self, hash: B256) -> StateResult<Option<Vec<u8>>> {
        let runtime = self
            .runtime
            .lock()
            .expect("remote fork runtime lock is poisoned");
        runtime
            .block_on(self.client.get_preimage(hash))
            .map_err(|err| {
                tracing::warn!(?hash, %err, "failed to fetch remote fork preimage");
                StateError::NotFound(0)
            })
    }

    fn get_repository_block(&self, block: BlockHashOrNumber) -> Option<RepositoryBlock> {
        self.with_runtime(self.client.get_repository_block(block), |err| {
            tracing::warn!(?block, %err, "failed to fetch remote fork block");
        })
        .flatten()
    }

    fn get_raw_transaction(&self, hash: TxHash) -> Option<Vec<u8>> {
        self.with_runtime(self.client.get_raw_transaction(hash), |err| {
            tracing::warn!(?hash, %err, "failed to fetch remote fork raw transaction");
        })
        .flatten()
    }

    fn get_transaction(&self, hash: TxHash) -> Option<ZkTransaction> {
        self.with_runtime(self.client.get_transaction(hash), |err| {
            tracing::warn!(?hash, %err, "failed to fetch remote fork transaction");
        })
        .flatten()
    }

    fn get_transaction_receipt(&self, hash: TxHash) -> Option<ZkReceiptEnvelope> {
        self.with_runtime(self.client.get_transaction_receipt(hash), |err| {
            tracing::warn!(?hash, %err, "failed to fetch remote fork receipt");
        })
        .flatten()
    }

    fn get_transaction_meta(&self, hash: TxHash) -> Option<TxMeta> {
        self.with_runtime(self.client.get_transaction_meta(hash), |err| {
            tracing::warn!(?hash, %err, "failed to fetch remote fork transaction meta");
        })
        .flatten()
    }

    fn get_transaction_hash_by_sender_nonce(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> Option<TxHash> {
        self.with_runtime(
            self.client
                .get_transaction_hash_by_sender_nonce(sender, nonce),
            |err| {
                tracing::warn!(
                    ?sender,
                    nonce,
                    %err,
                    "failed to fetch remote fork sender nonce mapping"
                );
            },
        )
        .flatten()
    }

    fn get_stored_transaction(&self, hash: TxHash) -> Option<StoredTxData> {
        self.with_runtime(self.client.get_stored_transaction(hash), |err| {
            tracing::warn!(?hash, %err, "failed to fetch remote fork stored transaction");
        })
        .flatten()
    }

    fn get_latest_block(&self) -> Option<BlockNumber> {
        self.with_runtime(self.client.get_latest_block_number(), |err| {
            tracing::warn!(%err, "failed to fetch remote fork latest block number");
        })
    }

    fn get_replay_record(&self, block_number: BlockNumber) -> Option<ReplayRecord> {
        self.with_runtime(self.client.get_replay_record(block_number), |err| {
            tracing::warn!(block_number, %err, "failed to fetch remote fork replay record");
        })
        .flatten()
    }

    fn get_latest_replay_record(&self) -> Option<BlockNumber> {
        self.with_runtime(self.client.get_latest_replay_record_number(), |err| {
            tracing::warn!(%err, "failed to fetch remote fork latest replay record");
        })
    }
}

#[derive(Clone, Debug)]
struct ForkedStateView<Local> {
    local_state: Option<Local>,
    block_number: BlockNumber,
    remote_fork: Option<Arc<dyn RemoteForkSource>>,
}

impl<Local: ViewState> ReadStorage for ForkedStateView<Local> {
    fn read(&mut self, key: B256) -> Option<B256> {
        if let Some(local_state) = &mut self.local_state
            && let Some(value) = local_state.read(key)
        {
            return Some(value);
        }

        self.remote_fork.as_ref().and_then(|remote_fork| {
            remote_fork
                .read_value(self.block_number, key)
                .ok()
                .flatten()
        })
    }
}

impl<Local: ViewState> PreimageSource for ForkedStateView<Local> {
    fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
        if let Some(local_state) = &mut self.local_state
            && let Some(preimage) = local_state.get_preimage(hash)
        {
            return Some(preimage);
        }

        self.remote_fork
            .as_ref()
            .and_then(|remote_fork| remote_fork.read_preimage(hash).ok().flatten())
    }
}

#[cfg(test)]
mod tests {
    use super::{ForkedStateView, RemoteForkSource};
    use alloy::primitives::B256;
    use std::collections::HashMap;
    use zksync_os_interface::traits::{PreimageSource, ReadStorage};
    use zksync_os_storage_api::StateResult;

    #[derive(Clone, Debug)]
    struct TestLocalState {
        storage: HashMap<B256, B256>,
        preimages: HashMap<B256, Vec<u8>>,
    }

    impl ReadStorage for TestLocalState {
        fn read(&mut self, key: B256) -> Option<B256> {
            self.storage.get(&key).copied()
        }
    }

    impl PreimageSource for TestLocalState {
        fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
            self.preimages.get(&hash).cloned()
        }
    }

    #[derive(Debug)]
    struct TestRemoteFork {
        storage: HashMap<(u64, B256), B256>,
        preimages: HashMap<B256, Vec<u8>>,
    }

    impl RemoteForkSource for TestRemoteFork {
        fn read_value(&self, block_number: u64, key: B256) -> StateResult<Option<B256>> {
            Ok(self.storage.get(&(block_number, key)).copied())
        }

        fn read_preimage(&self, hash: B256) -> StateResult<Option<Vec<u8>>> {
            Ok(self.preimages.get(&hash).cloned())
        }

        fn get_repository_block(&self, _: BlockHashOrNumber) -> Option<RepositoryBlock> {
            None
        }

        fn get_raw_transaction(&self, _: TxHash) -> Option<Vec<u8>> {
            None
        }

        fn get_transaction(&self, _: TxHash) -> Option<ZkTransaction> {
            None
        }

        fn get_transaction_receipt(&self, _: TxHash) -> Option<ZkReceiptEnvelope> {
            None
        }

        fn get_transaction_meta(&self, _: TxHash) -> Option<TxMeta> {
            None
        }

        fn get_transaction_hash_by_sender_nonce(&self, _: Address, _: TxNonce) -> Option<TxHash> {
            None
        }

        fn get_stored_transaction(&self, _: TxHash) -> Option<StoredTxData> {
            None
        }

        fn get_latest_block(&self) -> Option<BlockNumber> {
            None
        }

        fn get_replay_record(&self, _: BlockNumber) -> Option<ReplayRecord> {
            None
        }

        fn get_latest_replay_record(&self) -> Option<BlockNumber> {
            None
        }
    }

    #[test]
    fn falls_back_to_remote_storage_when_local_value_is_missing() {
        let key = B256::with_last_byte(1);
        let remote_value = B256::with_last_byte(2);
        let remote = TestRemoteFork {
            storage: HashMap::from([((7, key), remote_value)]),
            preimages: HashMap::new(),
        };
        let mut view = ForkedStateView {
            local_state: Some(TestLocalState {
                storage: HashMap::new(),
                preimages: HashMap::new(),
            }),
            block_number: 7,
            remote_fork: Some(std::sync::Arc::new(remote)),
        };

        assert_eq!(view.read(key), Some(remote_value));
    }

    #[test]
    fn falls_back_to_remote_preimage_without_local_state() {
        let hash = B256::with_last_byte(3);
        let preimage = vec![1, 2, 3];
        let remote = TestRemoteFork {
            storage: HashMap::new(),
            preimages: HashMap::from([(hash, preimage.clone())]),
        };
        let mut view = ForkedStateView::<TestLocalState> {
            local_state: None,
            block_number: 11,
            remote_fork: Some(std::sync::Arc::new(remote)),
        };

        assert_eq!(view.get_preimage(hash), Some(preimage));
    }

    #[test]
    fn preserves_local_values_when_available() {
        let key = B256::with_last_byte(9);
        let local_value = B256::with_last_byte(10);
        let mut view = ForkedStateView {
            local_state: Some(TestLocalState {
                storage: HashMap::from([(key, local_value)]),
                preimages: HashMap::new(),
            }),
            block_number: 0,
            remote_fork: Some(std::sync::Arc::new(TestRemoteFork {
                storage: HashMap::from([((0, key), B256::with_last_byte(11))]),
                preimages: HashMap::new(),
            })),
        };

        assert_eq!(view.read(key), Some(local_value));
    }
}
