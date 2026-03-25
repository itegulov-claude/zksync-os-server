use alloy::eips::BlockHashOrNumber;
use alloy::primitives::{Address, B256, BlockNumber, TxHash, TxNonce};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use std::fmt;
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};
use zksync_os_interface::traits::{PreimageSource, ReadStorage};
use zksync_os_rpc_api::unstable::UnstableApiClient;
use zksync_os_storage_api::notifications::{BlockNotification, SubscribeToBlocks};
use zksync_os_storage_api::{
    ReadReplay, ReadRepository, ReadStateHistory, ReplayRecord, RepositoryBlock, RepositoryResult,
    StateError, StateResult, StoredTxData, TxMeta, ViewState, WriteReplay, WriteRepository,
    WriteState,
};
use zksync_os_types::{ZkReceiptEnvelope, ZkTransaction};

pub trait RemoteForkSource: fmt::Debug + Send + Sync {
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
pub struct ForkedRepository<Local> {
    local: Local,
    remote_fork: Arc<dyn RemoteForkSource>,
}

impl<Local> ForkedRepository<Local> {
    pub fn new(local: Local, remote_fork: Arc<dyn RemoteForkSource>) -> Self {
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
        Ok(self
            .local
            .get_block_by_number(number)?
            .or_else(|| self.remote_fork.get_repository_block(number.into())))
    }

    fn get_block_by_hash(
        &self,
        hash: alloy::primitives::BlockHash,
    ) -> RepositoryResult<Option<RepositoryBlock>> {
        Ok(self
            .local
            .get_block_by_hash(hash)?
            .or_else(|| self.remote_fork.get_repository_block(hash.into())))
    }

    fn get_raw_transaction(&self, hash: TxHash) -> RepositoryResult<Option<Vec<u8>>> {
        Ok(self
            .local
            .get_raw_transaction(hash)?
            .or_else(|| self.remote_fork.get_raw_transaction(hash)))
    }

    fn get_transaction(&self, hash: TxHash) -> RepositoryResult<Option<ZkTransaction>> {
        Ok(self
            .local
            .get_transaction(hash)?
            .or_else(|| self.remote_fork.get_transaction(hash)))
    }

    fn get_transaction_receipt(&self, hash: TxHash) -> RepositoryResult<Option<ZkReceiptEnvelope>> {
        Ok(self
            .local
            .get_transaction_receipt(hash)?
            .or_else(|| self.remote_fork.get_transaction_receipt(hash)))
    }

    fn get_transaction_meta(&self, hash: TxHash) -> RepositoryResult<Option<TxMeta>> {
        Ok(self
            .local
            .get_transaction_meta(hash)?
            .or_else(|| self.remote_fork.get_transaction_meta(hash)))
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
                    .get_transaction_hash_by_sender_nonce(sender, nonce)
            }))
    }

    fn get_stored_transaction(&self, hash: TxHash) -> RepositoryResult<Option<StoredTxData>> {
        Ok(self
            .local
            .get_stored_transaction(hash)?
            .or_else(|| self.remote_fork.get_stored_transaction(hash)))
    }

    fn get_latest_block(&self) -> u64 {
        self.remote_fork.get_latest_block().map_or_else(
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
    fn subscribe_to_blocks(&self) -> tokio::sync::broadcast::Receiver<BlockNotification> {
        self.local.subscribe_to_blocks()
    }
}

#[derive(Clone, Debug)]
pub enum RepositoryStorage<Local> {
    Local(Local),
    Forked(ForkedRepository<Local>),
}

impl<Local> RepositoryStorage<Local> {
    pub fn new(local: Local) -> Self {
        Self::Local(local)
    }

    pub fn forked(local: Local, remote_fork: Arc<dyn RemoteForkSource>) -> Self {
        Self::Forked(ForkedRepository::new(local, remote_fork))
    }

    pub fn local(&self) -> Local
    where
        Local: Clone,
    {
        match self {
            Self::Local(local) => local.clone(),
            Self::Forked(forked) => forked.local(),
        }
    }
}

impl<Local: ReadRepository> ReadRepository for RepositoryStorage<Local> {
    fn get_block_by_number(
        &self,
        number: BlockNumber,
    ) -> RepositoryResult<Option<RepositoryBlock>> {
        match self {
            Self::Local(local) => local.get_block_by_number(number),
            Self::Forked(forked) => forked.get_block_by_number(number),
        }
    }

    fn get_block_by_hash(
        &self,
        hash: alloy::primitives::BlockHash,
    ) -> RepositoryResult<Option<RepositoryBlock>> {
        match self {
            Self::Local(local) => local.get_block_by_hash(hash),
            Self::Forked(forked) => forked.get_block_by_hash(hash),
        }
    }

    fn get_raw_transaction(&self, hash: TxHash) -> RepositoryResult<Option<Vec<u8>>> {
        match self {
            Self::Local(local) => local.get_raw_transaction(hash),
            Self::Forked(forked) => forked.get_raw_transaction(hash),
        }
    }

    fn get_transaction(&self, hash: TxHash) -> RepositoryResult<Option<ZkTransaction>> {
        match self {
            Self::Local(local) => local.get_transaction(hash),
            Self::Forked(forked) => forked.get_transaction(hash),
        }
    }

    fn get_transaction_receipt(&self, hash: TxHash) -> RepositoryResult<Option<ZkReceiptEnvelope>> {
        match self {
            Self::Local(local) => local.get_transaction_receipt(hash),
            Self::Forked(forked) => forked.get_transaction_receipt(hash),
        }
    }

    fn get_transaction_meta(&self, hash: TxHash) -> RepositoryResult<Option<TxMeta>> {
        match self {
            Self::Local(local) => local.get_transaction_meta(hash),
            Self::Forked(forked) => forked.get_transaction_meta(hash),
        }
    }

    fn get_transaction_hash_by_sender_nonce(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> RepositoryResult<Option<TxHash>> {
        match self {
            Self::Local(local) => local.get_transaction_hash_by_sender_nonce(sender, nonce),
            Self::Forked(forked) => forked.get_transaction_hash_by_sender_nonce(sender, nonce),
        }
    }

    fn get_stored_transaction(&self, hash: TxHash) -> RepositoryResult<Option<StoredTxData>> {
        match self {
            Self::Local(local) => local.get_stored_transaction(hash),
            Self::Forked(forked) => forked.get_stored_transaction(hash),
        }
    }

    fn get_latest_block(&self) -> u64 {
        match self {
            Self::Local(local) => local.get_latest_block(),
            Self::Forked(forked) => forked.get_latest_block(),
        }
    }

    fn get_earliest_block(&self) -> u64 {
        match self {
            Self::Local(local) => local.get_earliest_block(),
            Self::Forked(forked) => forked.get_earliest_block(),
        }
    }
}

impl<Local: WriteRepository> WriteRepository for RepositoryStorage<Local> {
    async fn populate(
        &self,
        block_output: zksync_os_interface::types::BlockOutput,
        transactions: Vec<ZkTransaction>,
    ) -> RepositoryResult<()> {
        match self {
            Self::Local(local) => local.populate(block_output, transactions).await,
            Self::Forked(forked) => forked.populate(block_output, transactions).await,
        }
    }
}

impl<Local: SubscribeToBlocks> SubscribeToBlocks for RepositoryStorage<Local> {
    fn subscribe_to_blocks(&self) -> tokio::sync::broadcast::Receiver<BlockNotification> {
        match self {
            Self::Local(local) => local.subscribe_to_blocks(),
            Self::Forked(forked) => forked.subscribe_to_blocks(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ForkedReplayStorage<Local> {
    local: Local,
    remote_fork: Arc<dyn RemoteForkSource>,
}

impl<Local> ForkedReplayStorage<Local> {
    pub fn new(local: Local, remote_fork: Arc<dyn RemoteForkSource>) -> Self {
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
            .or_else(|| self.remote_fork.get_replay_record(block_number))
    }

    fn latest_record(&self) -> BlockNumber {
        self.remote_fork.get_latest_replay_record().map_or_else(
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
pub enum ReplayStorage<Local> {
    Local(Local),
    Forked(ForkedReplayStorage<Local>),
}

impl<Local> ReplayStorage<Local> {
    pub fn new(local: Local) -> Self {
        Self::Local(local)
    }

    pub fn forked(local: Local, remote_fork: Arc<dyn RemoteForkSource>) -> Self {
        Self::Forked(ForkedReplayStorage::new(local, remote_fork))
    }

    pub fn local(&self) -> Local
    where
        Local: Clone,
    {
        match self {
            Self::Local(local) => local.clone(),
            Self::Forked(forked) => forked.local(),
        }
    }
}

impl<Local: ReadReplay> ReadReplay for ReplayStorage<Local> {
    fn get_context(
        &self,
        block_number: BlockNumber,
    ) -> Option<zksync_os_interface::types::BlockContext> {
        match self {
            Self::Local(local) => local.get_context(block_number),
            Self::Forked(forked) => forked.get_context(block_number),
        }
    }

    fn get_replay_record_by_key(
        &self,
        block_number: BlockNumber,
        db_key: Option<Vec<u8>>,
    ) -> Option<ReplayRecord> {
        match self {
            Self::Local(local) => local.get_replay_record_by_key(block_number, db_key),
            Self::Forked(forked) => forked.get_replay_record_by_key(block_number, db_key),
        }
    }

    fn latest_record(&self) -> BlockNumber {
        match self {
            Self::Local(local) => local.latest_record(),
            Self::Forked(forked) => forked.latest_record(),
        }
    }
}

impl<Local: WriteReplay> WriteReplay for ReplayStorage<Local> {
    fn write(
        &self,
        record: alloy::primitives::Sealed<ReplayRecord>,
        override_allowed: bool,
    ) -> bool {
        match self {
            Self::Local(local) => local.write(record, override_allowed),
            Self::Forked(forked) => forked.write(record, override_allowed),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ForkedStateHistory<Local> {
    local: Local,
    remote_fork: Arc<dyn RemoteForkSource>,
}

impl<Local> ForkedStateHistory<Local> {
    pub fn new(local: Local, remote_fork: Arc<dyn RemoteForkSource>) -> Self {
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
            Err(StateError::Compacted(_) | StateError::NotFound(_)) => None,
        };

        Ok(ForkedStateView {
            local_state,
            block_number,
            remote_fork: self.remote_fork.clone(),
        })
    }

    fn block_range_available(&self) -> RangeInclusive<u64> {
        let local_range = self.local.block_range_available();
        0..=self
            .remote_fork
            .get_latest_block()
            .map_or(*local_range.end(), |remote| remote.max(*local_range.end()))
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

#[derive(Clone, Debug)]
pub enum StateHistoryStorage<Local> {
    Local(Local),
    Forked(ForkedStateHistory<Local>),
}

impl<Local> StateHistoryStorage<Local> {
    pub fn new(local: Local) -> Self {
        Self::Local(local)
    }

    pub fn forked(local: Local, remote_fork: Arc<dyn RemoteForkSource>) -> Self {
        Self::Forked(ForkedStateHistory::new(local, remote_fork))
    }

    pub fn local(&self) -> Local
    where
        Local: Clone,
    {
        match self {
            Self::Local(local) => local.clone(),
            Self::Forked(forked) => forked.local(),
        }
    }
}

#[derive(Clone, Debug)]
enum StateViewStorage<Local> {
    Local(Local),
    Forked(ForkedStateView<Local>),
}

impl<Local: ViewState> ReadStorage for StateViewStorage<Local> {
    fn read(&mut self, key: B256) -> Option<B256> {
        match self {
            Self::Local(local) => local.read(key),
            Self::Forked(forked) => forked.read(key),
        }
    }
}

impl<Local: ViewState> PreimageSource for StateViewStorage<Local> {
    fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
        match self {
            Self::Local(local) => local.get_preimage(hash),
            Self::Forked(forked) => forked.get_preimage(hash),
        }
    }
}

impl<Local: ReadStateHistory> ReadStateHistory for StateHistoryStorage<Local> {
    fn state_view_at(
        &self,
        block_number: BlockNumber,
    ) -> StateResult<impl ReadStorage + PreimageSource + Clone> {
        match self {
            Self::Local(local) => local
                .state_view_at(block_number)
                .map(StateViewStorage::Local),
            Self::Forked(forked) => {
                let local_state = match forked.local.state_view_at(block_number) {
                    Ok(state) => Some(state),
                    Err(StateError::Compacted(_) | StateError::NotFound(_)) => None,
                };

                Ok(StateViewStorage::Forked(ForkedStateView {
                    local_state,
                    block_number,
                    remote_fork: forked.remote_fork.clone(),
                }))
            }
        }
    }

    fn block_range_available(&self) -> RangeInclusive<u64> {
        match self {
            Self::Local(local) => local.block_range_available(),
            Self::Forked(forked) => forked.block_range_available(),
        }
    }
}

impl<Local: WriteState> WriteState for StateHistoryStorage<Local> {
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
        match self {
            Self::Local(local) => {
                local.add_block_result(block_number, storage_diffs, new_preimages, override_allowed)
            }
            Self::Forked(forked) => forked.add_block_result(
                block_number,
                storage_diffs,
                new_preimages,
                override_allowed,
            ),
        }
    }
}

#[derive(Clone, Debug)]
struct ForkedStateView<Local> {
    local_state: Option<Local>,
    block_number: BlockNumber,
    remote_fork: Arc<dyn RemoteForkSource>,
}

impl<Local: ViewState> ReadStorage for ForkedStateView<Local> {
    fn read(&mut self, key: B256) -> Option<B256> {
        if let Some(local_state) = &mut self.local_state
            && let Some(value) = local_state.read(key)
        {
            return Some(value);
        }

        self.remote_fork
            .read_value(self.block_number, key)
            .ok()
            .flatten()
    }
}

impl<Local: ViewState> PreimageSource for ForkedStateView<Local> {
    fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
        if let Some(local_state) = &mut self.local_state
            && let Some(preimage) = local_state.get_preimage(hash)
        {
            return Some(preimage);
        }

        self.remote_fork.read_preimage(hash).ok().flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::{ForkedStateView, RemoteForkSource};
    use alloy::eips::BlockHashOrNumber;
    use alloy::primitives::{Address, B256, BlockNumber, TxHash, TxNonce};
    use std::collections::HashMap;
    use zksync_os_interface::traits::{PreimageSource, ReadStorage};
    use zksync_os_storage_api::{ReplayRecord, RepositoryBlock, StateResult, StoredTxData, TxMeta};
    use zksync_os_types::{ZkReceiptEnvelope, ZkTransaction};

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
        fn read_value(&self, block_number: BlockNumber, key: B256) -> StateResult<Option<B256>> {
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
            remote_fork: Arc::new(remote),
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
            remote_fork: Arc::new(remote),
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
            remote_fork: Arc::new(TestRemoteFork {
                storage: HashMap::from([((0, key), B256::with_last_byte(11))]),
                preimages: HashMap::new(),
            }),
        };

        assert_eq!(view.read(key), Some(local_value));
    }
}
