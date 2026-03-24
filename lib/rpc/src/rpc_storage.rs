use alloy::eips::{BlockHashOrNumber, BlockId, BlockNumberOrTag};
use alloy::primitives::{B256, BlockNumber};
use jsonrpsee::http_client::{HttpClient, HttpClientBuilder};
use std::sync::Mutex;
use std::{ops::RangeInclusive, sync::Arc};
use zksync_os_interface::traits::{PreimageSource, ReadStorage};
use zksync_os_merkle_tree_api::MerkleTreeProver;
use zksync_os_rpc_api::unstable::UnstableApiClient;
use zksync_os_storage_api::{
    ReadBatch, ReadFinality, ReadReplay, ReadRepository, ReadStateHistory, RepositoryBlock,
    RepositoryError, RepositoryResult, StateError, StateResult, ViewState,
    notifications::SubscribeToBlocks,
};

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
    remote_fork: Option<Arc<dyn RemoteForkSource>>,
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
        Self::with_remote_fork(
            repository,
            replay_storage,
            finality,
            batch,
            state,
            tree,
            None,
        )
    }

    pub fn with_remote_fork(
        repository: Repository,
        replay_storage: Replay,
        finality: Finality,
        batch: Batch,
        state: StateHistory,
        tree: Arc<dyn MerkleTreeProver>,
        remote_fork: Option<Arc<dyn RemoteForkSource>>,
    ) -> Self {
        Self {
            repository,
            replay_storage,
            finality,
            batch,
            state,
            tree,
            remote_fork,
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
        let local_state = match self.state.state_view_at(block_number) {
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
