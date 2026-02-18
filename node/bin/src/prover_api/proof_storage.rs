use crate::prover_api::fri_job_manager::FailedFriProof;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use smart_config::{DescribeConfig, DeserializeConfig};
use std::cmp::Reverse;
use std::path::PathBuf;
use std::time::SystemTime;
use tokio::fs;
use zksync_os_l1_sender::batcher_model::{FriProof, SignedBatchEnvelope};

#[derive(Debug, Clone, DescribeConfig, DeserializeConfig)]
pub struct ProofStorageConfig {
    #[config(default_t = "./db/fri_proofs/".into())]
    pub path: PathBuf,
    //1GB by default
    #[config(default_t = 1073741824)]
    pub batch_capacity: u64,
    #[config(default_t = 1073741824)]
    pub failed_batch_capacity: u64,
}

impl Default for ProofStorageConfig {
    fn default() -> Self {
        Self {
            path: "./db/fri_proofs/".into(),
            batch_capacity: 1 << 30,
            failed_batch_capacity: 1 << 30,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ProofStorage {
    storage: BoundedFileStore,
    failed_storage: BoundedFileStore,
}
impl ProofStorage {
    pub fn new(config: ProofStorageConfig) -> Self {
        tracing::info!(
            path = config.path.to_str().unwrap(),
            batch_capacity = config.batch_capacity,
            failed_batch_capacity = config.failed_batch_capacity,
            "Initializing proof storage"
        );
        Self {
            storage: BoundedFileStore::new(config.path.join("fri_batches"), config.batch_capacity),
            failed_storage: BoundedFileStore::new(
                config.path.join("failed_proofs"),
                config.failed_batch_capacity,
            ),
        }
    }

    /// Persist a BatchWithProof. Overwrites any existing entry for the same batch.
    pub async fn save_batch_with_proof(&self, batch: &StoredBatch) -> anyhow::Result<()> {
        let key = format!("batch_{}.json", batch.batch_number());
        self.storage.store(&key, batch).await
    }

    /// Loads a BatchWithProof for `batch_number`, if present
    pub async fn get_batch_with_proof(
        &self,
        batch_num: u64,
    ) -> anyhow::Result<Option<SignedBatchEnvelope<FriProof>>> {
        let key = format!("batch_{batch_num}.json");
        match self.storage.load::<StoredBatch>(&key).await {
            Ok(o) => Ok(o.map(|o| o.batch_envelope())),
            Err(err) => Err(err),
        }
    }

    /// Save a failed FRI proof with batch metadata for debugging.
    pub async fn save_failed_proof(&self, proof: &FailedFriProof) -> anyhow::Result<()> {
        let key = format!("failed_{}.json", proof.batch_number);
        self.failed_storage.store(&key, proof).await
    }

    /// Get the failed proof for a given batch number.
    /// Returns None if no failed proof exists for this batch.
    pub async fn get_failed_proof(&self, batch_num: u64) -> anyhow::Result<Option<FailedFriProof>> {
        let key = format!("failed_{batch_num}.json");
        self.failed_storage.load(&key).await
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[non_exhaustive]
pub enum StoredBatch {
    V1(SignedBatchEnvelope<FriProof>),
}

impl StoredBatch {
    pub fn batch_number(&self) -> u64 {
        match self {
            StoredBatch::V1(envelope) => envelope.batch_number(),
        }
    }

    pub fn batch_envelope(self) -> SignedBatchEnvelope<FriProof> {
        match self {
            StoredBatch::V1(envelope) => envelope,
        }
    }
}

/// Storage for data blobs that
/// automatically removes old files to keep disk usage within capacity_bytes
/// TODO: Clone???
#[derive(Clone, Debug)]
struct BoundedFileStore {
    base_dir: PathBuf,
    capacity_bytes: u64,
    capacity_files: u64,
}

impl BoundedFileStore {
    const CAPACITY_FILES: u64 = 1000;
    fn new(base_dir: PathBuf, capacity_bytes: u64) -> Self {
        Self {
            base_dir,
            capacity_bytes,
            capacity_files: BoundedFileStore::CAPACITY_FILES,
        }
    }

    async fn store<T: Serialize>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        fs::create_dir_all(&self.base_dir).await?;

        let file_path = self.base_dir.join(key);
        let data = serde_json::to_vec(value).expect("Failed to serialize value");
        self.enforce_capacity(data.len() as u64).await?;
        if (data.len() as u64) <= self.capacity_bytes {
            fs::write(file_path, data).await?;
        } else {
            tracing::warn!(
                data_len = data.len(),
                capacity = self.capacity_bytes,
                "Entry size is larger than the limit. Not saving.",
            );
        }
        Ok(())
    }

    async fn load<T: DeserializeOwned>(&self, key: &str) -> anyhow::Result<Option<T>> {
        let path = self.base_dir.join(key);
        if !path.exists() {
            return Ok(None);
        }

        let data = fs::read(path).await.expect("Failed to read file");
        let decoded = serde_json::from_slice(&data).expect("Deserialization failed");
        Ok(Some(decoded))
    }

    /// Delete old files to make space for the new file
    async fn enforce_capacity(&self, new_file_size: u64) -> anyhow::Result<()> {
        // List all files sorted by timestamp (descending)
        let mut entries = fs::read_dir(&self.base_dir).await?;
        let mut files = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let meta = entry.metadata().await?;
            if meta.is_file() {
                files.push((entry.path(), meta));
            }
        }
        files.sort_by_cached_key(|(_, meta)| {
            Reverse(meta.modified().unwrap_or(SystemTime::UNIX_EPOCH))
        });

        //Delete old files to satisfy capacity constraints
        let mut current_size = new_file_size;
        let mut current_count = 1;
        let files_to_delete = files.into_iter().skip_while(|(_, meta)| {
            current_size += meta.len();
            current_count += 1;
            current_count <= self.capacity_files && current_size <= self.capacity_bytes
        });
        for (path, _) in files_to_delete {
            fs::remove_file(path).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_bounded_storage_capacity() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let path = dir.path().to_owned();
        let storage = BoundedFileStore::new(path, 1 << 20);

        //verify file capacity
        let capacity_files = BoundedFileStore::CAPACITY_FILES;
        for i in 0..2000 {
            let str: String = i.to_string();
            storage.store(&str, &str).await?;
            assert_eq!(storage.load::<String>(str.as_str()).await?, Some(str));
            if (i >= capacity_files) {
                assert!(
                    storage
                        .load::<String>(&(i - capacity_files + 1).to_string())
                        .await?
                        .is_some()
                );
                assert!(
                    storage
                        .load::<String>(&(i - capacity_files).to_string())
                        .await?
                        .is_none()
                );
            }
        }

        //verify size capacity
        let big_str = "a".repeat((1 << 20) - 500);
        storage.store("key", &big_str).await?;
        //This removes most entries but not all
        assert!(storage.load::<String>(&1200.to_string()).await?.is_none());
        assert!(storage.load::<String>(&1999.to_string()).await?.is_some());
        //This should remove all the old entries
        storage.store("key2", &big_str).await?;
        assert!(storage.load::<String>("key").await?.is_none());
        //Can't store huge files -
        let very_big = "a".repeat(1 << 21);
        storage.store("key", &very_big).await?;
        assert!(storage.load::<String>("key").await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_bounded_storage_overwrites() -> anyhow::Result<()> {
        const limit: u64 = 1 << 20;
        let dir = TempDir::new()?;
        let path = dir.path().to_owned();
        let storage = BoundedFileStore::new(path, limit);
        let big_str_a = "a".repeat((limit * 2 / 3) as usize);
        storage.store("key", &big_str_a).await?;
        assert_eq!(storage.load("key").await?, Some(big_str_a));
        let big_str_b = "b".repeat((limit * 2 / 3) as usize);
        storage.store("key", &big_str_b).await?;
        assert_eq!(storage.load("key").await?, Some(big_str_b));

        Ok(())
    }
}
