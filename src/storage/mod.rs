use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crc32fast::Hasher;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::task;

use crate::protocol::{FetchedRecord, Offset, Record};

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("topic '{topic}' partition {partition} not found")]
    UnknownPartition { topic: String, partition: u32 },
    #[error("data corruption: {0}")]
    Corruption(String),
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Clone)]
pub struct Storage {
    inner: Arc<StorageInner>,
}

struct StorageInner {
    root: PathBuf,
    segment_bytes: u64,
    retention_bytes: Option<u64>,
    retention_segments: Option<usize>,
    index_stride: usize,
    fsync: bool,
    logs: RwLock<HashMap<TopicPartition, Arc<RwLock<Log>>>>,
}

#[derive(Hash, Eq, PartialEq, Clone, Debug)]
struct TopicPartition {
    topic: String,
    partition: u32,
}

impl Storage {
    pub fn open(
        root: impl AsRef<Path>,
        segment_bytes: u64,
        retention_bytes: Option<u64>,
        retention_segments: Option<usize>,
        index_stride: usize,
        fsync: bool,
    ) -> StorageResult<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root)?;
        let storage = Self {
            inner: Arc::new(StorageInner {
                root,
                segment_bytes,
                retention_bytes,
                retention_segments,
                index_stride: index_stride.max(1),
                fsync,
                logs: RwLock::new(HashMap::new()),
            }),
        };

        storage.load_existing()?;
        Ok(storage)
    }

    fn load_existing(&self) -> StorageResult<()> {
        for topic_entry in fs::read_dir(&self.inner.root)? {
            let topic_entry = topic_entry?;
            if !topic_entry.file_type()?.is_dir() {
                continue;
            }
            let topic = match topic_entry.file_name().into_string() {
                Ok(t) => t,
                Err(_) => continue,
            };

            for partition_entry in fs::read_dir(topic_entry.path())? {
                let partition_entry = partition_entry?;
                if !partition_entry.file_type()?.is_dir() {
                    continue;
                }
                let name = match partition_entry.file_name().into_string() {
                    Ok(n) => n,
                    Err(_) => continue,
                };
                if !name.starts_with("partition-") {
                    continue;
                }
                let id_str = &name[10..];
                let partition: u32 = match id_str.parse() {
                    Ok(p) => p,
                    Err(_) => continue,
                };

                let key = TopicPartition {
                    topic: topic.clone(),
                    partition,
                };

                let dir = self.partition_dir(&key);
                let log = Log::recover(
                    dir,
                    self.inner.segment_bytes,
                    self.inner.retention_bytes,
                    self.inner.retention_segments,
                    self.inner.index_stride,
                    self.inner.fsync,
                )?;

                self.inner
                    .logs
                    .write()
                    .insert(key, Arc::new(RwLock::new(log)));
            }
        }

        Ok(())
    }

    pub async fn append_async(
        &self,
        topic: &str,
        partition: u32,
        records: Vec<Record>,
    ) -> StorageResult<(Offset, Offset)> {
        let topic = topic.to_string();
        let this = self.clone();
        task::spawn_blocking(move || this.append(&topic, partition, records))
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    pub fn append(
        &self,
        topic: &str,
        partition: u32,
        records: Vec<Record>,
    ) -> StorageResult<(Offset, Offset)> {
        let key = TopicPartition {
            topic: topic.to_string(),
            partition,
        };
        let log = self.get_or_create_log(&key)?;
        let mut guard = log.write();
        guard.append_batch(records)
    }

    pub async fn append_with_offsets_async(
        &self,
        topic: &str,
        partition: u32,
        entries: Vec<(Offset, Record)>,
    ) -> StorageResult<(Offset, Offset)> {
        let topic = topic.to_string();
        let this = self.clone();
        task::spawn_blocking(move || this.append_with_offsets(&topic, partition, entries))
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    pub fn append_with_offsets(
        &self,
        topic: &str,
        partition: u32,
        entries: Vec<(Offset, Record)>,
    ) -> StorageResult<(Offset, Offset)> {
        let key = TopicPartition {
            topic: topic.to_string(),
            partition,
        };
        let log = self.get_or_create_log(&key)?;
        let mut guard = log.write();
        guard.append_with_offsets(&entries)
    }

    pub async fn fetch_async(
        &self,
        topic: &str,
        partition: u32,
        offset: Offset,
        max_bytes: u32,
    ) -> StorageResult<Vec<FetchedRecord>> {
        let topic = topic.to_string();
        let this = self.clone();
        task::spawn_blocking(move || this.fetch(&topic, partition, offset, max_bytes))
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    pub fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: Offset,
        max_bytes: u32,
    ) -> StorageResult<Vec<FetchedRecord>> {
        let key = TopicPartition {
            topic: topic.to_string(),
            partition,
        };

        let log = self.get_log(&key)?;
        let guard = log.read();
        guard.fetch(offset, max_bytes)
    }

    pub async fn compact_async(&self, topic: &str, partition: u32) -> StorageResult<()> {
        let topic = topic.to_string();
        let this = self.clone();
        task::spawn_blocking(move || this.compact(&topic, partition))
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    pub fn compact(&self, topic: &str, partition: u32) -> StorageResult<()> {
        let key = TopicPartition {
            topic: topic.to_string(),
            partition,
        };

        let log = self.get_log(&key)?;
        let mut guard = log.write();
        guard.compact()
    }

    pub async fn run_retention_async(&self) -> StorageResult<()> {
        let this = self.clone();
        task::spawn_blocking(move || this.run_retention())
            .await
            .map_err(|err| StorageError::Io(std::io::Error::other(err)))?
    }

    pub fn run_retention(&self) -> StorageResult<()> {
        let logs: Vec<Arc<RwLock<Log>>> = {
            let guard = self.inner.logs.read();
            guard.values().cloned().collect()
        };

        for log in logs {
            let mut guard = log.write();
            guard.apply_retention()?;
        }
        Ok(())
    }

    fn partition_dir(&self, key: &TopicPartition) -> PathBuf {
        self.inner
            .root
            .join(&key.topic)
            .join(format!("partition-{}", key.partition))
    }

    fn get_or_create_log(&self, key: &TopicPartition) -> StorageResult<Arc<RwLock<Log>>> {
        if let Some(log) = self.inner.logs.read().get(key).cloned() {
            return Ok(log);
        }

        let mut guard = self.inner.logs.write();
        if let Some(log) = guard.get(key).cloned() {
            return Ok(log);
        }

        let dir = self.partition_dir(key);
        let log = Log::recover_or_create(
            dir,
            self.inner.segment_bytes,
            self.inner.retention_bytes,
            self.inner.retention_segments,
            self.inner.index_stride,
            self.inner.fsync,
        )?;
        let log = Arc::new(RwLock::new(log));
        guard.insert(key.clone(), log.clone());
        Ok(log)
    }

    fn get_log(&self, key: &TopicPartition) -> StorageResult<Arc<RwLock<Log>>> {
        self.inner
            .logs
            .read()
            .get(key)
            .cloned()
            .ok_or_else(|| StorageError::UnknownPartition {
                topic: key.topic.clone(),
                partition: key.partition,
            })
    }
}

struct Log {
    dir: PathBuf,
    segment_bytes: u64,
    retention_bytes: Option<u64>,
    retention_segments: Option<usize>,
    index_stride: usize,
    fsync: bool,
    commit_path: PathBuf,
    active: Segment,
    sealed: Vec<SegmentMeta>,
    sealed_bytes: u64,
}

impl Log {
    fn recover(
        dir: PathBuf,
        segment_bytes: u64,
        retention_bytes: Option<u64>,
        retention_segments: Option<usize>,
        index_stride: usize,
        fsync: bool,
    ) -> StorageResult<Self> {
        std::fs::create_dir_all(&dir)?;

        let mut segments = discover_segments(&dir)?;
        segments.sort_by_key(|(base, _)| *base);

        let committed = load_commit(&dir)?;

        if segments.is_empty() {
            return Self::create(
                dir,
                0,
                segment_bytes,
                retention_bytes,
                retention_segments,
                index_stride,
                fsync,
            );
        }

        let mut metas = Vec::new();
        for (base, path) in segments.iter() {
            let idx_path = index_path(path);
            let scanned = scan_segment(path, *base, index_stride, committed)?;
            let index = load_index(&idx_path)?.unwrap_or(scanned.index.clone());

            if !idx_path.exists() {
                write_index(&idx_path, &index)?;
            }
            metas.push(SegmentMeta {
                path: path.clone(),
                base_offset: *base,
                last_offset: scanned.last_offset,
                size: scanned.size,
                index,
            });
        }

        let mut metas: Vec<SegmentMeta> = metas
            .into_iter()
            .filter(|m| committed.map(|c| m.base_offset <= c).unwrap_or(true))
            .collect();

        let active_meta = metas.pop().expect("non-empty");
        let sealed_bytes: u64 = metas.iter().map(|m| m.size).sum();
        let commit_path = commit_path(&dir);
        let mut log = Self {
            dir,
            segment_bytes,
            retention_bytes,
            retention_segments,
            index_stride,
            fsync,
            commit_path,
            active: Segment::recover(active_meta.clone(), index_stride)?,
            sealed: metas,
            sealed_bytes,
        };

        if log.active.size >= log.segment_bytes {
            log.rotate()?;
        }

        // Ensure next_offset is at least committed+1 if commit exists.
        if let Some(c) = committed {
            log.active.next_offset = (c + 1).max(log.active.next_offset);
        }

        log.apply_retention()?;
        Ok(log)
    }

    fn create(
        dir: PathBuf,
        base_offset: Offset,
        segment_bytes: u64,
        retention_bytes: Option<u64>,
        retention_segments: Option<usize>,
        index_stride: usize,
        fsync: bool,
    ) -> StorageResult<Self> {
        std::fs::create_dir_all(&dir)?;
        let path = segment_path(&dir, base_offset);
        let commit_path = commit_path(&dir);
        let active = Segment::create(path, base_offset, index_stride)?;

        Ok(Self {
            dir,
            segment_bytes,
            retention_bytes,
            retention_segments,
            index_stride,
            fsync,
            commit_path,
            active,
            sealed: Vec::new(),
            sealed_bytes: 0,
        })
    }

    fn recover_or_create(
        dir: PathBuf,
        segment_bytes: u64,
        retention_bytes: Option<u64>,
        retention_segments: Option<usize>,
        index_stride: usize,
        fsync: bool,
    ) -> StorageResult<Self> {
        let has_segments = !discover_segments(&dir)?.is_empty();
        if has_segments {
            Self::recover(
                dir,
                segment_bytes,
                retention_bytes,
                retention_segments,
                index_stride,
                fsync,
            )
        } else {
            Self::create(
                dir,
                0,
                segment_bytes,
                retention_bytes,
                retention_segments,
                index_stride,
                fsync,
            )
        }
    }

    fn append_batch(&mut self, records: Vec<Record>) -> StorageResult<(Offset, Offset)> {
        if records.is_empty() {
            let last = self.active.next_offset.saturating_sub(1);
            return Ok((self.active.next_offset, last));
        }

        let batch_start = self.active.next_offset;
        for record in records {
            let payload = bincode::serialize(&record)?;
            if self
                .active
                .will_overflow(payload.len() as u64, self.segment_bytes)
            {
                self.rotate()?;
            }
            let offset = self.active.next_offset;
            self.active.append(offset, &payload)?;
            self.active.next_offset += 1;
        }

        self.active.flush()?;
        if self.fsync {
            self.active.sync()?;
        }
        let last_offset = self.active.next_offset.saturating_sub(1);
        persist_commit(&self.commit_path, last_offset)?;
        let batch_end = self.active.next_offset.saturating_sub(1);
        Ok((batch_start, batch_end))
    }

    fn fetch(&self, offset: Offset, max_bytes: u32) -> StorageResult<Vec<FetchedRecord>> {
        let mut out = Vec::new();

        for segment in &self.sealed {
            if segment.last_offset < offset {
                continue;
            }
            let records = segment.read_from(offset, max_bytes)?;
            out.extend(records);
            if over_budget(&out, max_bytes) {
                return Ok(out);
            }
        }

        let active_meta = self.active.meta();
        if active_meta.last_offset >= offset {
            let records = self.active.read_from(offset, max_bytes)?;
            out.extend(records);
        }

        Ok(out)
    }

    fn append_with_offsets(
        &mut self,
        records: &[(Offset, Record)],
    ) -> StorageResult<(Offset, Offset)> {
        if records.is_empty() {
            let last = self.active.next_offset.saturating_sub(1);
            return Ok((self.active.next_offset, last));
        }

        let batch_start = records
            .first()
            .map(|(o, _)| *o)
            .unwrap_or(self.active.next_offset);
        let mut batch_end = batch_start;

        for (offset, record) in records.iter() {
            if *offset != self.active.next_offset {
                return Err(StorageError::Corruption(format!(
                    "non-contiguous offset: expected {} got {}",
                    self.active.next_offset, offset
                )));
            }
            let payload = bincode::serialize(record)?;
            if self
                .active
                .will_overflow(payload.len() as u64, self.segment_bytes)
            {
                self.rotate()?;
            }
            self.active.append(*offset, &payload)?;
            self.active.next_offset = offset.saturating_add(1);
            batch_end = *offset;
        }

        self.active.flush()?;
        if self.fsync {
            self.active.sync()?;
        }
        persist_commit(&self.commit_path, batch_end)?;
        Ok((batch_start, batch_end))
    }

    fn rotate(&mut self) -> StorageResult<()> {
        let next_base = self.active.next_offset;
        let new_active = Segment::create(
            segment_path(&self.dir, next_base),
            next_base,
            self.index_stride,
        )?;
        let sealed = std::mem::replace(&mut self.active, new_active).seal()?;
        self.sealed_bytes = self.sealed_bytes.saturating_add(sealed.size);
        self.sealed.push(sealed);
        self.apply_retention()?;
        Ok(())
    }

    fn apply_retention(&mut self) -> StorageResult<()> {
        let mut removed = Vec::new();
        loop {
            let over_bytes = self
                .retention_bytes
                .map(|limit| self.sealed_bytes > limit)
                .unwrap_or(false);
            let over_segments = self
                .retention_segments
                .map(|limit| self.sealed.len() > limit)
                .unwrap_or(false);

            if !over_bytes && !over_segments {
                break;
            }

            if let Some(oldest) = self.sealed.first() {
                removed.push(oldest.path.clone());
                self.sealed_bytes = self.sealed_bytes.saturating_sub(oldest.size);
                self.sealed.remove(0);
            } else {
                break;
            }
        }

        for path in removed {
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(index_path(&path));
        }
        Ok(())
    }

    fn compact(&mut self) -> StorageResult<()> {
        // Collect latest record per key.
        let mut latest: HashMap<Vec<u8>, (Offset, Record)> = HashMap::new();

        for segment in &self.sealed {
            let records = segment.read_from(0, u32::MAX)?;
            for r in records {
                latest.insert(r.record.key.clone(), (r.offset, r.record));
            }
        }

        {
            let active_records = self.active.read_from(0, u32::MAX)?;
            for r in active_records {
                latest.insert(r.record.key.clone(), (r.offset, r.record));
            }
        }

        if latest.is_empty() {
            return Ok(());
        }

        let mut compacted: Vec<(Offset, Record)> = latest.into_values().collect();
        compacted.sort_by_key(|(o, _)| *o);

        // Drop tombstones (empty value) entirely.
        compacted.retain(|(_, rec)| !rec.value.is_empty());

        if compacted.is_empty() {
            return Ok(());
        }

        let base = compacted.first().map(|(o, _)| *o).unwrap_or(0);
        let new_dir = self.dir.with_extension("compact");
        let _ = fs::remove_dir_all(&new_dir);

        let mut new_log = Log::create(
            new_dir.clone(),
            base,
            self.segment_bytes,
            self.retention_bytes,
            self.retention_segments,
            self.index_stride,
            self.fsync,
        )?;

        new_log.append_with_offsets(&compacted)?;
        new_log.apply_retention()?;
        sync_dir(&new_dir)?;

        let backup_dir = self.dir.with_extension("old");
        let _ = fs::remove_dir_all(&backup_dir);
        let parent_dir = self
            .dir
            .parent()
            .ok_or_else(|| {
                StorageError::Corruption("partition directory has no parent".to_string())
            })?
            .to_path_buf();

        // Rename existing dir aside and swap in compacted log with rollback.
        fs::rename(&self.dir, &backup_dir)?;
        sync_dir(&parent_dir)?;

        if let Err(err) = fs::rename(&new_dir, &self.dir) {
            let rollback = fs::rename(&backup_dir, &self.dir);
            let _ = sync_dir(&parent_dir);
            return match rollback {
                Ok(_) => Err(StorageError::Io(err)),
                Err(rb_err) => Err(StorageError::Corruption(format!(
                    "compaction swap failed: {err}; rollback failed: {rb_err}"
                ))),
            };
        }
        sync_dir(&parent_dir)?;

        let rebuilt = Log::recover(
            self.dir.clone(),
            self.segment_bytes,
            self.retention_bytes,
            self.retention_segments,
            self.index_stride,
            self.fsync,
        )?;

        *self = rebuilt;
        let _ = fs::remove_dir_all(&backup_dir);
        let _ = sync_dir(&parent_dir);
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IndexEntry {
    offset: Offset,
    position: u64,
}

struct Segment {
    path: PathBuf,
    base_offset: Offset,
    next_offset: Offset,
    size: u64,
    records: u64,
    index_stride: usize,
    index: Vec<IndexEntry>,
    writer: BufWriter<File>,
}

impl Segment {
    fn create(path: PathBuf, base_offset: Offset, index_stride: usize) -> StorageResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let size = file.metadata()?.len();

        Ok(Self {
            path,
            base_offset,
            next_offset: base_offset,
            size,
            records: 0,
            index_stride,
            index: Vec::new(),
            writer: BufWriter::new(file),
        })
    }

    fn recover(meta: SegmentMeta, index_stride: usize) -> StorageResult<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&meta.path)?;
        let size = file.metadata()?.len();
        let records = if meta.last_offset >= meta.base_offset {
            meta.last_offset - meta.base_offset + 1
        } else {
            0
        };

        Ok(Self {
            path: meta.path,
            base_offset: meta.base_offset,
            next_offset: meta.last_offset.saturating_add(1),
            size,
            records,
            index_stride,
            index: meta.index,
            writer: BufWriter::new(file),
        })
    }

    fn will_overflow(&self, record_len: u64, max_bytes: u64) -> bool {
        let frame_overhead = frame_overhead();
        self.size + frame_overhead + record_len > max_bytes
    }

    fn append(&mut self, offset: Offset, payload: &[u8]) -> StorageResult<()> {
        let frame_pos = self.size;
        if self.records.is_multiple_of(self.index_stride as u64) {
            self.index.push(IndexEntry {
                offset,
                position: frame_pos,
            });
        }

        self.writer.write_all(&offset.to_le_bytes())?;
        let len = payload.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        let mut hasher = Hasher::new();
        hasher.update(payload);
        let crc = hasher.finalize();
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(payload)?;
        self.size += frame_overhead() + payload.len() as u64;
        self.records += 1;
        Ok(())
    }

    fn flush(&mut self) -> StorageResult<()> {
        self.writer.flush()?;
        Ok(())
    }

    fn sync(&mut self) -> StorageResult<()> {
        sync_writer(&mut self.writer)
    }

    fn meta(&self) -> SegmentMeta {
        SegmentMeta {
            path: self.path.clone(),
            base_offset: self.base_offset,
            last_offset: self.next_offset.saturating_sub(1),
            size: self.size,
            index: self.index.clone(),
        }
    }

    fn seal(mut self) -> StorageResult<SegmentMeta> {
        self.flush()?;
        sync_writer(&mut self.writer)?;
        let meta = self.meta();
        write_index(&index_path(&self.path), &meta.index)?;
        Ok(meta)
    }

    fn start_position(&self, from_offset: Offset) -> u64 {
        start_position(&self.index, from_offset)
    }

    fn read_from(&self, from_offset: Offset, max_bytes: u32) -> StorageResult<Vec<FetchedRecord>> {
        let mut file = File::open(&self.path)?;
        let mut reader = BufReader::new(&mut file);
        let mut out = Vec::new();
        let mut offset_buf = [0u8; 8];
        let mut len_buf = [0u8; 4];
        let mut crc_buf = [0u8; 4];
        let mut budget = max_bytes as u64;

        let start = self.start_position(from_offset);
        reader.seek(SeekFrom::Start(start))?;

        loop {
            if reader.read_exact(&mut offset_buf).is_err() {
                break;
            }
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let offset = u64::from_le_bytes(offset_buf);
            let len = u32::from_le_bytes(len_buf) as usize;
            if reader.read_exact(&mut crc_buf).is_err() {
                break;
            }
            let mut payload = vec![0u8; len];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }

            if offset < from_offset {
                continue;
            }

            let frame_size = len as u64 + frame_overhead();
            if frame_size > budget {
                break;
            }

            let expected_crc = u32::from_le_bytes(crc_buf);
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let actual_crc = hasher.finalize();
            if expected_crc != actual_crc {
                return Err(StorageError::Corruption("crc mismatch".into()));
            }

            let record: Record = bincode::deserialize(&payload)?;
            out.push(FetchedRecord { offset, record });
            budget = budget.saturating_sub(frame_size);
        }

        Ok(out)
    }
}

#[derive(Clone)]
struct SegmentMeta {
    path: PathBuf,
    base_offset: Offset,
    last_offset: Offset,
    size: u64,
    index: Vec<IndexEntry>,
}

impl SegmentMeta {
    fn read_from(&self, offset: Offset, max_bytes: u32) -> StorageResult<Vec<FetchedRecord>> {
        let mut file = File::open(&self.path)?;
        let mut reader = BufReader::new(&mut file);
        let mut offset_buf = [0u8; 8];
        let mut len_buf = [0u8; 4];
        let mut crc_buf = [0u8; 4];
        let mut budget = max_bytes as u64;

        let start = start_position(&self.index, offset);
        reader.seek(SeekFrom::Start(start))?;

        let mut out = Vec::new();
        loop {
            if reader.read_exact(&mut offset_buf).is_err() {
                break;
            }
            if reader.read_exact(&mut len_buf).is_err() {
                break;
            }
            let off = u64::from_le_bytes(offset_buf);
            let len = u32::from_le_bytes(len_buf) as usize;
            if reader.read_exact(&mut crc_buf).is_err() {
                break;
            }
            let mut payload = vec![0u8; len];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }

            if off < offset {
                continue;
            }

            let frame_size = len as u64 + frame_overhead();
            if frame_size > budget {
                break;
            }

            let expected_crc = u32::from_le_bytes(crc_buf);
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let actual_crc = hasher.finalize();
            if expected_crc != actual_crc {
                return Err(StorageError::Corruption("crc mismatch".into()));
            }

            let record: Record = bincode::deserialize(&payload)?;
            out.push(FetchedRecord {
                offset: off,
                record,
            });
            budget = budget.saturating_sub(frame_size);
        }

        Ok(out)
    }
}

fn segment_path(dir: &Path, base_offset: Offset) -> PathBuf {
    dir.join(format!("{base_offset:020}.seg"))
}

fn index_path(segment_path: &Path) -> PathBuf {
    segment_path.with_extension("idx")
}

fn commit_path(dir: &Path) -> PathBuf {
    dir.join("commit.meta")
}

fn write_index(path: &Path, index: &[IndexEntry]) -> StorageResult<()> {
    let bytes = bincode::serialize(index)?;
    fs::write(path, bytes)?;
    if let Ok(f) = File::open(path) {
        let _ = f.sync_all();
    }
    Ok(())
}

fn load_commit(dir: &Path) -> StorageResult<Option<Offset>> {
    let path = commit_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    if bytes.len() < 8 {
        return Ok(None);
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    Ok(Some(u64::from_le_bytes(buf)))
}

fn persist_commit(path: &Path, last_offset: Offset) -> StorageResult<()> {
    let mut file = File::create(path)?;
    file.write_all(&last_offset.to_le_bytes())?;
    file.sync_all()?;
    Ok(())
}

fn load_index(path: &Path) -> StorageResult<Option<Vec<IndexEntry>>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(path)?;
    let index: Vec<IndexEntry> = bincode::deserialize(&bytes)?;
    Ok(Some(index))
}

fn start_position(index: &[IndexEntry], from_offset: Offset) -> u64 {
    let mut pos = 0u64;
    for entry in index {
        if entry.offset <= from_offset {
            pos = entry.position;
        } else {
            break;
        }
    }
    pos
}

fn frame_overhead() -> u64 {
    std::mem::size_of::<Offset>() as u64 + std::mem::size_of::<u32>() as u64 * 2
}

fn sync_writer(writer: &mut BufWriter<File>) -> StorageResult<()> {
    writer.flush()?;
    writer.get_mut().sync_all()?;
    Ok(())
}

fn sync_dir(path: &Path) -> StorageResult<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Debug)]
struct ScannedSegment {
    last_offset: Offset,
    size: u64,
    index: Vec<IndexEntry>,
}

fn discover_segments(dir: &Path) -> StorageResult<Vec<(Offset, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = match entry.file_name().into_string() {
            Ok(n) => n,
            Err(_) => continue,
        };
        if !name.ends_with(".seg") {
            continue;
        }
        let trimmed = name.trim_end_matches(".seg");
        let base: Offset = match trimmed.parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        out.push((base, entry.path()));
    }

    Ok(out)
}

fn scan_segment(
    path: &Path,
    base_offset: Offset,
    index_stride: usize,
    max_offset: Option<Offset>,
) -> StorageResult<ScannedSegment> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut offset_buf = [0u8; 8];
    let mut len_buf = [0u8; 4];
    let mut crc_buf = [0u8; 4];
    let mut last_offset = base_offset.saturating_sub(1);
    let mut index = Vec::new();
    let mut records = 0u64;
    let mut position = 0u64;

    loop {
        let frame_start = position;
        match file.read_exact(&mut offset_buf) {
            Ok(_) => position += offset_buf.len() as u64,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                file.set_len(frame_start)?;
                break;
            }
            Err(err) => return Err(StorageError::Io(err)),
        }

        match file.read_exact(&mut len_buf) {
            Ok(_) => position += len_buf.len() as u64,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                file.set_len(frame_start)?;
                break;
            }
            Err(err) => return Err(StorageError::Io(err)),
        }

        let len = u32::from_le_bytes(len_buf) as usize;
        match file.read_exact(&mut crc_buf) {
            Ok(_) => position += crc_buf.len() as u64,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                file.set_len(frame_start)?;
                break;
            }
            Err(err) => return Err(StorageError::Io(err)),
        }

        let mut payload = vec![0u8; len];
        match file.read_exact(&mut payload) {
            Ok(_) => position += len as u64,
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                file.set_len(frame_start)?;
                break;
            }
            Err(err) => return Err(StorageError::Io(err)),
        }

        let offset = u64::from_le_bytes(offset_buf);

        if let Some(max) = max_offset {
            if offset > max {
                file.set_len(frame_start)?;
                break;
            }
        }

        let expected_crc = u32::from_le_bytes(crc_buf);
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let actual_crc = hasher.finalize();
        if expected_crc != actual_crc {
            file.set_len(frame_start)?;
            break;
        }

        last_offset = offset;

        if records.is_multiple_of(index_stride as u64) {
            index.push(IndexEntry {
                offset,
                position: frame_start,
            });
        }

        // Validate payload via deserialize to drop corrupt trailing data.
        if bincode::deserialize::<Record>(&payload).is_err() {
            file.set_len(frame_start)?;
            break;
        }

        records += 1;
    }

    let size = file.metadata()?.len();

    Ok(ScannedSegment {
        last_offset,
        size,
        index,
    })
}

fn over_budget(records: &[FetchedRecord], max_bytes: u32) -> bool {
    let mut size = 0u64;
    for r in records.iter() {
        size += frame_overhead();
        size += r.record.key.len() as u64
            + r.record.value.len() as u64
            + std::mem::size_of::<i64>() as u64;
        if size > max_bytes as u64 {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn append_and_fetch_roundtrip() {
        let dir = tempdir().unwrap();
        let storage = Storage::open(dir.path(), 1024 * 1024, None, None, 16, false).unwrap();

        let records = vec![Record {
            key: b"k1".to_vec(),
            value: b"v1".to_vec(),
            timestamp: 1,
        }];

        let (base, last) = storage.append("test", 0, records.clone()).unwrap();
        assert_eq!(base, 0);
        assert_eq!(last, 0);

        let fetched = storage.fetch("test", 0, 0, 1024).unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].offset, 0);
        assert_eq!(fetched[0].record.value, b"v1".to_vec());

        // Add more records to force rotation
        let big_records = (0..10)
            .map(|i| Record {
                key: format!("k{i}").into_bytes(),
                value: vec![b'x'; 128],
                timestamp: i,
            })
            .collect();

        storage.append("test", 0, big_records).unwrap();
        let fetched_all = storage.fetch("test", 0, 0, 64 * 1024).unwrap();
        assert!(fetched_all.len() >= 5);
    }

    #[test]
    fn recovery_rebuilds_segments() {
        let dir = tempdir().unwrap();
        {
            let storage = Storage::open(dir.path(), 1024 * 16, None, None, 4, false).unwrap();
            let records = (0..20)
                .map(|i| Record {
                    key: format!("k{i}").into_bytes(),
                    value: vec![b'v'; 64],
                    timestamp: i,
                })
                .collect();
            storage.append("topic", 0, records).unwrap();
        }

        // Restart and fetch from rebuilt offsets.
        let storage = Storage::open(dir.path(), 1024 * 16, None, None, 4, false).unwrap();
        let fetched = storage.fetch("topic", 0, 0, 64 * 1024).unwrap();
        assert_eq!(fetched.len(), 20);
        assert_eq!(fetched.first().unwrap().offset, 0);
        assert_eq!(fetched.last().unwrap().offset, 19);
    }
}
