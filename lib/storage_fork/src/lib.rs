use alloy::eips::{BlockHashOrNumber, Encodable2718};
use alloy::primitives::{Address, B256, BlockNumber, TxHash, TxNonce};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use tokio::sync::broadcast;
use zksync_os_interface::traits::{PreimageSource, ReadStorage};
use zksync_os_interface::types::{BlockContext, BlockOutput, StorageWrite};
use zksync_os_rpc_api::unstable::UnstableApiClient;
use zksync_os_storage::in_memory::RepositoryInMemory;
use zksync_os_storage_api::notifications::{BlockNotification, SubscribeToBlocks};
use zksync_os_storage_api::{
    ReadReplay, ReadRepository, ReadStateHistory, ReplayRecord, RepositoryResult, StateError,
    StateResult, StoredTxData, TxMeta, WriteReplay, WriteRepository, WriteState,
};
use zksync_os_types::{ZkReceiptEnvelope, ZkTransaction};

const BLOCK_NOTIFICATION_CHANNEL_SIZE: usize = 256;

#[derive(Clone, Copy, Debug)]
pub struct ForkSnapshot {
    pub latest_block_number: BlockNumber,
    pub latest_replay_record_number: BlockNumber,
}

pub trait RemoteForkSource: fmt::Debug + Send + Sync {
    fn read_value(&self, block_number: BlockNumber, key: B256) -> StateResult<Option<B256>>;
    fn read_preimage(&self, hash: B256) -> StateResult<Option<Vec<u8>>>;
    fn get_repository_block(
        &self,
        block: BlockHashOrNumber,
    ) -> Option<zksync_os_storage_api::RepositoryBlock>;
    fn get_transaction_hash_by_sender_nonce(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> Option<TxHash>;
    fn get_stored_transaction(&self, hash: TxHash) -> Option<StoredTxData>;
    fn get_replay_record(&self, block_number: BlockNumber) -> Option<ReplayRecord>;
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

    pub fn snapshot(&self) -> anyhow::Result<ForkSnapshot> {
        let latest_block_number = self.with_runtime_result(
            self.client.get_latest_block_number(),
            "latest remote fork block number",
        )?;
        let latest_replay_record_number = self.with_runtime_result(
            self.client.get_latest_replay_record_number(),
            "latest remote fork replay record number",
        )?;

        Ok(ForkSnapshot {
            latest_block_number,
            latest_replay_record_number,
        })
    }

    fn with_runtime_result<Output>(
        &self,
        future: impl std::future::Future<Output = Result<Output, jsonrpsee::core::ClientError>>,
        action: &'static str,
    ) -> anyhow::Result<Output> {
        let runtime = self
            .runtime
            .lock()
            .expect("remote fork runtime lock is poisoned");
        runtime
            .block_on(future)
            .map_err(|err| anyhow::anyhow!("failed to fetch {action}: {err}"))
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
        self.with_runtime(self.client.get_storage_value(block_number, key), |err| {
            tracing::warn!(
                block_number,
                ?key,
                %err,
                "failed to fetch remote fork storage value"
            );
        })
        .ok_or(StateError::NotFound(block_number))
    }

    fn read_preimage(&self, hash: B256) -> StateResult<Option<Vec<u8>>> {
        self.with_runtime(self.client.get_preimage(hash), |err| {
            tracing::warn!(?hash, %err, "failed to fetch remote fork preimage");
        })
        .ok_or(StateError::NotFound(0))
    }

    fn get_repository_block(
        &self,
        block: BlockHashOrNumber,
    ) -> Option<zksync_os_storage_api::RepositoryBlock> {
        self.with_runtime(self.client.get_repository_block(block), |err| {
            tracing::warn!(?block, %err, "failed to fetch remote fork block");
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

    fn get_replay_record(&self, block_number: BlockNumber) -> Option<ReplayRecord> {
        self.with_runtime(self.client.get_replay_record(block_number), |err| {
            tracing::warn!(block_number, %err, "failed to fetch remote fork replay record");
        })
        .flatten()
    }
}

#[derive(Clone, Debug)]
struct LocalRepository {
    repository: RepositoryInMemory,
    block_sender: broadcast::Sender<BlockNotification>,
}

impl LocalRepository {
    fn new(genesis_block: zksync_os_storage_api::RepositoryBlock) -> Self {
        let (block_sender, _) = broadcast::channel(BLOCK_NOTIFICATION_CHANNEL_SIZE);
        Self {
            repository: RepositoryInMemory::new(genesis_block),
            block_sender,
        }
    }
}

impl ReadRepository for LocalRepository {
    fn get_block_by_number(
        &self,
        number: BlockNumber,
    ) -> RepositoryResult<Option<zksync_os_storage_api::RepositoryBlock>> {
        self.repository.get_block_by_number(number)
    }

    fn get_block_by_hash(
        &self,
        hash: alloy::primitives::BlockHash,
    ) -> RepositoryResult<Option<zksync_os_storage_api::RepositoryBlock>> {
        self.repository.get_block_by_hash(hash)
    }

    fn get_raw_transaction(&self, hash: TxHash) -> RepositoryResult<Option<Vec<u8>>> {
        self.repository.get_raw_transaction(hash)
    }

    fn get_transaction(&self, hash: TxHash) -> RepositoryResult<Option<ZkTransaction>> {
        self.repository.get_transaction(hash)
    }

    fn get_transaction_receipt(&self, hash: TxHash) -> RepositoryResult<Option<ZkReceiptEnvelope>> {
        self.repository.get_transaction_receipt(hash)
    }

    fn get_transaction_meta(&self, hash: TxHash) -> RepositoryResult<Option<TxMeta>> {
        self.repository.get_transaction_meta(hash)
    }

    fn get_transaction_hash_by_sender_nonce(
        &self,
        sender: Address,
        nonce: TxNonce,
    ) -> RepositoryResult<Option<TxHash>> {
        self.repository
            .get_transaction_hash_by_sender_nonce(sender, nonce)
    }

    fn get_stored_transaction(&self, hash: TxHash) -> RepositoryResult<Option<StoredTxData>> {
        self.repository.get_stored_transaction(hash)
    }

    fn get_latest_block(&self) -> u64 {
        self.repository.get_latest_block()
    }
}

impl WriteRepository for LocalRepository {
    async fn populate(
        &self,
        block_output: BlockOutput,
        transactions: Vec<ZkTransaction>,
    ) -> RepositoryResult<()> {
        let block_number = block_output.header.number;
        let latest_block = self.repository.get_latest_block();
        if block_number <= latest_block {
            return Ok(());
        }
        assert_eq!(
            block_number,
            latest_block + 1,
            "forked repository local view must be contiguous: got {block_number}, expected {}",
            latest_block + 1
        );

        let (block, transactions) = self
            .repository
            .populate_in_memory(block_output, transactions);
        let _ = self.block_sender.send(BlockNotification {
            block,
            transactions,
        });
        Ok(())
    }
}

impl SubscribeToBlocks for LocalRepository {
    fn subscribe_to_blocks(&self) -> broadcast::Receiver<BlockNotification> {
        self.block_sender.subscribe()
    }
}

#[derive(Clone, Debug)]
pub struct ForkedRepository {
    local: LocalRepository,
    remote_fork: Arc<dyn RemoteForkSource>,
    fork_block_number: BlockNumber,
}

impl ForkedRepository {
    pub fn new(
        genesis_block: zksync_os_storage_api::RepositoryBlock,
        fork_block_number: BlockNumber,
        remote_fork: Arc<dyn RemoteForkSource>,
    ) -> Self {
        Self {
            local: LocalRepository::new(genesis_block),
            remote_fork,
            fork_block_number,
        }
    }

    fn remote_block_by_hash(
        &self,
        hash: alloy::primitives::BlockHash,
    ) -> Option<zksync_os_storage_api::RepositoryBlock> {
        self.remote_fork
            .get_repository_block(hash.into())
            .filter(|block| block.number <= self.fork_block_number)
    }

    fn remote_stored_transaction_before_fork(&self, hash: TxHash) -> Option<StoredTxData> {
        self.remote_fork
            .get_stored_transaction(hash)
            .filter(|stored| stored.meta.block_number <= self.fork_block_number)
    }
}

impl ReadRepository for ForkedRepository {
    fn get_block_by_number(
        &self,
        number: BlockNumber,
    ) -> RepositoryResult<Option<zksync_os_storage_api::RepositoryBlock>> {
        Ok(self.local.get_block_by_number(number)?.or_else(|| {
            (number <= self.fork_block_number)
                .then(|| self.remote_fork.get_repository_block(number.into()))
                .flatten()
        }))
    }

    fn get_block_by_hash(
        &self,
        hash: alloy::primitives::BlockHash,
    ) -> RepositoryResult<Option<zksync_os_storage_api::RepositoryBlock>> {
        Ok(self
            .local
            .get_block_by_hash(hash)?
            .or_else(|| self.remote_block_by_hash(hash)))
    }

    fn get_raw_transaction(&self, hash: TxHash) -> RepositoryResult<Option<Vec<u8>>> {
        Ok(self.local.get_raw_transaction(hash)?.or_else(|| {
            self.remote_stored_transaction_before_fork(hash)
                .map(|stored| stored.tx.into_envelope().encoded_2718())
        }))
    }

    fn get_transaction(&self, hash: TxHash) -> RepositoryResult<Option<ZkTransaction>> {
        Ok(self.local.get_transaction(hash)?.or_else(|| {
            self.remote_stored_transaction_before_fork(hash)
                .map(|stored| stored.tx)
        }))
    }

    fn get_transaction_receipt(&self, hash: TxHash) -> RepositoryResult<Option<ZkReceiptEnvelope>> {
        Ok(self.local.get_transaction_receipt(hash)?.or_else(|| {
            self.remote_stored_transaction_before_fork(hash)
                .map(|stored| stored.receipt)
        }))
    }

    fn get_transaction_meta(&self, hash: TxHash) -> RepositoryResult<Option<TxMeta>> {
        Ok(self.local.get_transaction_meta(hash)?.or_else(|| {
            self.remote_stored_transaction_before_fork(hash)
                .map(|stored| stored.meta)
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
                    .get_transaction_hash_by_sender_nonce(sender, nonce)
                    .filter(|hash| self.remote_stored_transaction_before_fork(*hash).is_some())
            }))
    }

    fn get_stored_transaction(&self, hash: TxHash) -> RepositoryResult<Option<StoredTxData>> {
        Ok(self
            .local
            .get_stored_transaction(hash)?
            .or_else(|| self.remote_stored_transaction_before_fork(hash)))
    }

    fn get_latest_block(&self) -> u64 {
        self.local.get_latest_block().max(self.fork_block_number)
    }

    fn get_earliest_block(&self) -> u64 {
        0
    }
}

impl WriteRepository for ForkedRepository {
    async fn populate(
        &self,
        block_output: BlockOutput,
        transactions: Vec<ZkTransaction>,
    ) -> RepositoryResult<()> {
        self.local.populate(block_output, transactions).await
    }
}

impl SubscribeToBlocks for ForkedRepository {
    fn subscribe_to_blocks(&self) -> broadcast::Receiver<BlockNotification> {
        self.local.subscribe_to_blocks()
    }
}

#[derive(Clone, Debug)]
struct LocalReplayStorage {
    records: Arc<RwLock<BTreeMap<BlockNumber, ReplayRecord>>>,
    latest_record: Arc<AtomicU64>,
}

impl LocalReplayStorage {
    fn new(genesis_record: ReplayRecord) -> Self {
        let mut records = BTreeMap::new();
        records.insert(0, genesis_record);
        Self {
            records: Arc::new(RwLock::new(records)),
            latest_record: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ReadReplay for LocalReplayStorage {
    fn get_context(&self, block_number: BlockNumber) -> Option<BlockContext> {
        self.records
            .read()
            .expect("local replay storage lock is poisoned")
            .get(&block_number)
            .map(|record| record.block_context)
    }

    fn get_replay_record_by_key(
        &self,
        block_number: BlockNumber,
        _db_key: Option<Vec<u8>>,
    ) -> Option<ReplayRecord> {
        self.records
            .read()
            .expect("local replay storage lock is poisoned")
            .get(&block_number)
            .cloned()
    }

    fn latest_record(&self) -> BlockNumber {
        self.latest_record.load(Ordering::Relaxed)
    }
}

impl WriteReplay for LocalReplayStorage {
    fn write(
        &self,
        record: alloy::primitives::Sealed<ReplayRecord>,
        override_allowed: bool,
    ) -> bool {
        let block_number = record.block_context.block_number;
        let mut records = self
            .records
            .write()
            .expect("local replay storage lock is poisoned");
        let latest_record = self.latest_record.load(Ordering::Relaxed);

        if override_allowed && block_number <= latest_record {
            records.split_off(&block_number);
            self.latest_record
                .store(block_number.saturating_sub(1), Ordering::Relaxed);
        } else if block_number <= latest_record {
            return false;
        }

        let latest_record = self.latest_record.load(Ordering::Relaxed);
        assert_eq!(
            block_number,
            latest_record + 1,
            "forked replay local view must be contiguous: got {block_number}, expected {}",
            latest_record + 1
        );

        records.insert(block_number, record.into_parts().0);
        self.latest_record.store(block_number, Ordering::Relaxed);
        true
    }
}

#[derive(Clone, Debug)]
pub struct ForkedReplayStorage {
    local: LocalReplayStorage,
    remote_fork: Arc<dyn RemoteForkSource>,
    fork_replay_record_number: BlockNumber,
}

impl ForkedReplayStorage {
    pub fn new(
        genesis_record: ReplayRecord,
        fork_replay_record_number: BlockNumber,
        remote_fork: Arc<dyn RemoteForkSource>,
    ) -> Self {
        Self {
            local: LocalReplayStorage::new(genesis_record),
            remote_fork,
            fork_replay_record_number,
        }
    }
}

impl ReadReplay for ForkedReplayStorage {
    fn get_context(&self, block_number: BlockNumber) -> Option<BlockContext> {
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
                (block_number <= self.fork_replay_record_number)
                    .then(|| self.remote_fork.get_replay_record(block_number))
                    .flatten()
            })
    }

    fn latest_record(&self) -> BlockNumber {
        self.local
            .latest_record()
            .max(self.fork_replay_record_number)
    }
}

impl WriteReplay for ForkedReplayStorage {
    fn write(
        &self,
        record: alloy::primitives::Sealed<ReplayRecord>,
        override_allowed: bool,
    ) -> bool {
        self.local.write(record, override_allowed)
    }
}

#[derive(Clone, Debug)]
struct LocalStateHistory {
    storage: Arc<RwLock<HashMap<B256, Vec<(BlockNumber, B256)>>>>,
    preimages: Arc<RwLock<HashMap<B256, Vec<u8>>>>,
    latest_block: Arc<AtomicU64>,
}

impl LocalStateHistory {
    fn new(
        genesis_storage_logs: impl IntoIterator<Item = (B256, B256)>,
        genesis_preimages: impl IntoIterator<Item = (B256, Vec<u8>)>,
    ) -> Self {
        let mut storage = HashMap::new();
        for (key, value) in genesis_storage_logs.into_iter() {
            storage.entry(key).or_insert_with(Vec::new).push((0, value));
        }

        let preimages = genesis_preimages.into_iter().collect();

        Self {
            storage: Arc::new(RwLock::new(storage)),
            preimages: Arc::new(RwLock::new(preimages)),
            latest_block: Arc::new(AtomicU64::new(0)),
        }
    }

    fn latest_block(&self) -> BlockNumber {
        self.latest_block.load(Ordering::Relaxed)
    }

    fn read_at(&self, block_number: BlockNumber, key: B256) -> Option<B256> {
        let storage = self
            .storage
            .read()
            .expect("local state storage lock is poisoned");
        Self::read_from_storage(&storage, block_number, key)
    }

    fn read_from_storage(
        storage: &HashMap<B256, Vec<(BlockNumber, B256)>>,
        block_number: BlockNumber,
        key: B256,
    ) -> Option<B256> {
        let entries = storage.get(&key)?;
        match entries.binary_search_by_key(&block_number, |(block, _)| *block) {
            Ok(index) => Some(entries[index].1),
            Err(0) => None,
            Err(index) => Some(entries[index - 1].1),
        }
    }
}

#[derive(Clone, Debug)]
struct LocalStateView {
    storage: Arc<RwLock<HashMap<B256, Vec<(BlockNumber, B256)>>>>,
    preimages: Arc<RwLock<HashMap<B256, Vec<u8>>>>,
    block_number: BlockNumber,
}

impl ReadStorage for LocalStateView {
    fn read(&mut self, key: B256) -> Option<B256> {
        let storage = self
            .storage
            .read()
            .expect("local state storage lock is poisoned");
        LocalStateHistory::read_from_storage(&storage, self.block_number, key)
    }
}

impl PreimageSource for LocalStateView {
    fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
        self.preimages
            .read()
            .expect("local state preimages lock is poisoned")
            .get(&hash)
            .cloned()
    }
}

impl ReadStateHistory for LocalStateHistory {
    fn state_view_at(
        &self,
        block_number: BlockNumber,
    ) -> StateResult<impl ReadStorage + PreimageSource + Clone> {
        if block_number > self.latest_block() {
            return Err(StateError::NotFound(block_number));
        }

        Ok(LocalStateView {
            storage: self.storage.clone(),
            preimages: self.preimages.clone(),
            block_number,
        })
    }

    fn block_range_available(&self) -> RangeInclusive<u64> {
        0..=self.latest_block()
    }
}

impl WriteState for LocalStateHistory {
    fn add_block_result<'a, J>(
        &self,
        block_number: u64,
        storage_diffs: Vec<StorageWrite>,
        new_preimages: J,
        override_allowed: bool,
    ) -> anyhow::Result<()>
    where
        J: IntoIterator<Item = (B256, &'a Vec<u8>)>,
    {
        let mut latest_block = self.latest_block();

        if override_allowed && block_number <= latest_block {
            let mut storage = self
                .storage
                .write()
                .expect("local state storage lock is poisoned");
            storage.retain(|_, entries| {
                entries.retain(|(written_at, _)| *written_at < block_number);
                !entries.is_empty()
            });
            latest_block = block_number.saturating_sub(1);
            self.latest_block.store(latest_block, Ordering::Relaxed);
        }

        if !override_allowed && block_number != 0 {
            if block_number <= latest_block {
                for write in storage_diffs {
                    let expected_value = self.read_at(block_number, write.key).unwrap_or_default();
                    assert_eq!(
                        expected_value, write.value,
                        "historical write discrepancy for key={} at block_number={block_number}",
                        write.key
                    );
                }
                return Ok(());
            }

            assert_eq!(
                block_number,
                latest_block + 1,
                "forked state local view must be contiguous: got {block_number}, expected {}",
                latest_block + 1
            );
        }

        let per_key: HashMap<B256, B256> = storage_diffs
            .into_iter()
            .map(|write| (write.key, write.value))
            .collect();
        {
            let mut storage = self
                .storage
                .write()
                .expect("local state storage lock is poisoned");
            for (key, value) in per_key {
                storage.entry(key).or_default().push((block_number, value));
            }
        }
        {
            let mut preimages = self
                .preimages
                .write()
                .expect("local state preimages lock is poisoned");
            for (hash, preimage) in new_preimages {
                preimages.insert(hash, preimage.clone());
            }
        }

        self.latest_block.store(block_number, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct RemoteStateView {
    block_number: BlockNumber,
    remote_fork: Arc<dyn RemoteForkSource>,
}

impl ReadStorage for RemoteStateView {
    fn read(&mut self, key: B256) -> Option<B256> {
        self.remote_fork
            .read_value(self.block_number, key)
            .ok()
            .flatten()
    }
}

impl PreimageSource for RemoteStateView {
    fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
        self.remote_fork.read_preimage(hash).ok().flatten()
    }
}

#[derive(Clone, Debug)]
enum StateViewStorage {
    Local(LocalStateView),
    Remote(RemoteStateView),
}

impl ReadStorage for StateViewStorage {
    fn read(&mut self, key: B256) -> Option<B256> {
        match self {
            Self::Local(local) => local.read(key),
            Self::Remote(remote) => remote.read(key),
        }
    }
}

impl PreimageSource for StateViewStorage {
    fn get_preimage(&mut self, hash: B256) -> Option<Vec<u8>> {
        match self {
            Self::Local(local) => local.get_preimage(hash),
            Self::Remote(remote) => remote.get_preimage(hash),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ForkedStateHistory {
    local: LocalStateHistory,
    remote_fork: Arc<dyn RemoteForkSource>,
    fork_block_number: BlockNumber,
}

impl ForkedStateHistory {
    pub fn new(
        genesis_storage_logs: impl IntoIterator<Item = (B256, B256)>,
        genesis_preimages: impl IntoIterator<Item = (B256, Vec<u8>)>,
        fork_block_number: BlockNumber,
        remote_fork: Arc<dyn RemoteForkSource>,
    ) -> Self {
        Self {
            local: LocalStateHistory::new(genesis_storage_logs, genesis_preimages),
            remote_fork,
            fork_block_number,
        }
    }
}

impl ReadStateHistory for ForkedStateHistory {
    fn state_view_at(
        &self,
        block_number: BlockNumber,
    ) -> StateResult<impl ReadStorage + PreimageSource + Clone> {
        if block_number <= self.local.latest_block() {
            return Ok(StateViewStorage::Local(LocalStateView {
                storage: self.local.storage.clone(),
                preimages: self.local.preimages.clone(),
                block_number,
            }));
        }

        if block_number <= self.fork_block_number {
            return Ok(StateViewStorage::Remote(RemoteStateView {
                block_number,
                remote_fork: self.remote_fork.clone(),
            }));
        }

        Err(StateError::NotFound(block_number))
    }

    fn block_range_available(&self) -> RangeInclusive<u64> {
        0..=self.local.latest_block().max(self.fork_block_number)
    }
}

impl WriteState for ForkedStateHistory {
    fn add_block_result<'a, J>(
        &self,
        block_number: u64,
        storage_diffs: Vec<StorageWrite>,
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

#[cfg(test)]
mod tests {
    use super::{ForkedReplayStorage, ForkedStateHistory, RemoteForkSource, StateViewStorage};
    use alloy::eips::BlockHashOrNumber;
    use alloy::primitives::{Address, B256, TxHash, TxNonce};
    use std::collections::HashMap;
    use std::sync::Arc;
    use zksync_os_interface::traits::{PreimageSource, ReadStorage};
    use zksync_os_interface::types::BlockContext;
    use zksync_os_storage_api::{ReplayRecord, StoredTxData};
    use zksync_os_types::{BlockStartCursors, ProtocolSemanticVersion};

    #[derive(Debug)]
    struct TestRemoteFork {
        storage: HashMap<(u64, B256), B256>,
        preimages: HashMap<B256, Vec<u8>>,
        replays: HashMap<u64, ReplayRecord>,
    }

    impl RemoteForkSource for TestRemoteFork {
        fn read_value(&self, block_number: u64, key: B256) -> super::StateResult<Option<B256>> {
            Ok(self.storage.get(&(block_number, key)).copied())
        }

        fn read_preimage(&self, hash: B256) -> super::StateResult<Option<Vec<u8>>> {
            Ok(self.preimages.get(&hash).cloned())
        }

        fn get_repository_block(
            &self,
            _: BlockHashOrNumber,
        ) -> Option<zksync_os_storage_api::RepositoryBlock> {
            None
        }

        fn get_transaction_hash_by_sender_nonce(&self, _: Address, _: TxNonce) -> Option<TxHash> {
            None
        }

        fn get_stored_transaction(&self, _: TxHash) -> Option<StoredTxData> {
            None
        }

        fn get_replay_record(&self, block_number: u64) -> Option<ReplayRecord> {
            self.replays.get(&block_number).cloned()
        }
    }

    fn test_replay_record(block_number: u64) -> ReplayRecord {
        ReplayRecord {
            block_context: BlockContext {
                block_number,
                timestamp: block_number,
                chain_id: 1,
                coinbase: Address::ZERO,
                block_hashes: Default::default(),
                gas_limit: 0,
                pubdata_limit: 0,
                mix_hash: Default::default(),
                execution_version: Default::default(),
            },
            transactions: vec![],
            previous_block_timestamp: block_number.saturating_sub(1),
            node_version: semver::Version::new(0, 18, 0),
            protocol_version: ProtocolSemanticVersion::legacy_genesis_version(),
            block_output_hash: B256::ZERO,
            force_preimages: vec![],
            starting_cursors: BlockStartCursors::default(),
        }
    }

    #[test]
    fn state_reads_fall_back_to_remote_before_local_catch_up() {
        let key = B256::with_last_byte(1);
        let remote_value = B256::with_last_byte(2);
        let mut view = ForkedStateHistory::new(
            vec![],
            vec![],
            7,
            Arc::new(TestRemoteFork {
                storage: HashMap::from([((7, key), remote_value)]),
                preimages: HashMap::new(),
                replays: HashMap::new(),
            }),
        )
        .state_view_at(7)
        .unwrap();

        assert_eq!(view.read(key), Some(remote_value));
    }

    #[test]
    fn state_reads_use_local_values_after_replay() {
        let key = B256::with_last_byte(3);
        let local_value = B256::with_last_byte(4);
        let state = ForkedStateHistory::new(
            vec![],
            vec![],
            5,
            Arc::new(TestRemoteFork {
                storage: HashMap::new(),
                preimages: HashMap::new(),
                replays: HashMap::new(),
            }),
        );
        state
            .add_block_result(
                1,
                vec![StorageWrite {
                    key,
                    value: local_value,
                    account: Default::default(),
                    account_key: Default::default(),
                }],
                std::iter::empty::<(B256, &Vec<u8>)>(),
                false,
            )
            .unwrap();

        let mut view = state.state_view_at(1).unwrap();
        assert_eq!(view.read(key), Some(local_value));
    }

    #[test]
    fn replay_reads_remote_until_written_locally() {
        let remote_record = test_replay_record(1);
        let replay_storage = ForkedReplayStorage::new(
            test_replay_record(0),
            1,
            Arc::new(TestRemoteFork {
                storage: HashMap::new(),
                preimages: HashMap::new(),
                replays: HashMap::from([(1, remote_record.clone())]),
            }),
        );

        assert_eq!(
            replay_storage
                .get_replay_record(1)
                .unwrap()
                .block_context
                .block_number,
            1
        );
        assert_eq!(replay_storage.latest_record(), 1);
    }
}
