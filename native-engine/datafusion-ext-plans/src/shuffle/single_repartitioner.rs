// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fs::File,
    io::{Seek, Write},
    sync::Arc,
};

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use crc32fast::Hasher as CrcHasher;
use datafusion::{common::Result, physical_plan::metrics::Time};
use datafusion_ext_commons::io::ipc_compression::IpcCompressionWriter;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex;

use crate::{
    common::timer_helper::{TimedWriter, TimerHelper},
    shuffle::{ShuffleRepartitioner, open_shuffle_file},
};

// Wraps a Write impl and simultaneously feeds written bytes to a shared CRC32
// hasher. The hasher is shared via Arc<SyncMutex> so the final value can be
// read after the writer has been dropped (which happens inside the opaque
// IpcCompressionWriter that has no into_inner() method).
//
// Placed between IpcCompressionWriter and File so CRC32 is computed on the
// compressed bytes — fewer bytes to hash, cheaper than uncompressed.
struct CrcWriter<W: Write> {
    inner: W,
    hasher: Arc<SyncMutex<CrcHasher>>,
}

impl<W: Write> CrcWriter<W> {
    fn new(inner: W, hasher: Arc<SyncMutex<CrcHasher>>) -> Self {
        Self { inner, hasher }
    }
}

impl<W: Write> Write for CrcWriter<W> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.hasher.lock().update(&buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

// Writer state inside the async Mutex.
// The IpcCompressionWriter layer stack:
//   checksum disabled: IpcCompressionWriter<TimedWriter<File>>
//   checksum enabled:  IpcCompressionWriter<CrcWriter<TimedWriter<File>>>
//
// In both cases the File is accessible via inner_mut() for stream_position().
// The CRC32 value is extracted from the shared Arc<SyncMutex<CrcHasher>>.
enum WriterState {
    Uninit,
    Plain(IpcCompressionWriter<TimedWriter<File>>),
    WithCrc(
        IpcCompressionWriter<CrcWriter<TimedWriter<File>>>,
        Arc<SyncMutex<CrcHasher>>,
    ),
}

impl Default for WriterState {
    fn default() -> Self {
        WriterState::Uninit
    }
}

pub struct SingleShuffleRepartitioner {
    output_data_file: String,
    output_index_file: String,
    output_checksum_file: Option<String>,
    output_data: Arc<Mutex<WriterState>>,
    output_io_time: Time,
}

impl SingleShuffleRepartitioner {
    pub fn new(
        output_data_file: String,
        output_index_file: String,
        output_checksum_file: Option<String>,
        output_io_time: Time,
    ) -> Self {
        Self {
            output_data_file,
            output_index_file,
            output_checksum_file,
            output_data: Arc::new(Mutex::default()),
            output_io_time,
        }
    }

    fn open_writer(&self) -> Result<WriterState> {
        let file = open_shuffle_file(&self.output_data_file)?;
        let timed = self.output_io_time.wrap_writer(file);
        if self.output_checksum_file.is_some() {
            let hasher = Arc::new(SyncMutex::new(CrcHasher::new()));
            let crc_writer = CrcWriter::new(timed, hasher.clone());
            Ok(WriterState::WithCrc(
                IpcCompressionWriter::new(crc_writer),
                hasher,
            ))
        } else {
            Ok(WriterState::Plain(IpcCompressionWriter::new(timed)))
        }
    }
}

#[async_trait]
impl ShuffleRepartitioner for SingleShuffleRepartitioner {
    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let mut state = self.output_data.lock().await;
        if matches!(*state, WriterState::Uninit) {
            *state = self.open_writer()?;
        }
        match &mut *state {
            WriterState::Plain(w) => w.write_batch(input.num_rows(), input.columns()),
            WriterState::WithCrc(w, _) => w.write_batch(input.num_rows(), input.columns()),
            WriterState::Uninit => unreachable!(),
        }
    }

    async fn shuffle_write(&self) -> Result<()> {
        let state = std::mem::take(&mut *self.output_data.lock().await);

        match state {
            WriterState::Uninit => {
                // no batches were written: create empty data file and 16-byte index
                let _data = self
                    .output_io_time
                    .wrap_writer(open_shuffle_file(&self.output_data_file)?);
                let mut index = self
                    .output_io_time
                    .wrap_writer(open_shuffle_file(&self.output_index_file)?);
                index.write_all(&[0u8; 16])?;
            }
            WriterState::Plain(mut w) => {
                let mut index = self
                    .output_io_time
                    .wrap_writer(open_shuffle_file(&self.output_index_file)?);
                w.finish_current_buf()?;
                // TimedWriter<File>: field .0 is the File
                let offset = w.inner_mut().0.stream_position()?;
                index.write_all(&[0u8; 8])?;
                index.write_all(&(offset as i64).to_le_bytes())?;
            }
            WriterState::WithCrc(mut w, hasher) => {
                let mut index = self
                    .output_io_time
                    .wrap_writer(open_shuffle_file(&self.output_index_file)?);
                w.finish_current_buf()?;
                // CrcWriter<TimedWriter<File>>: .inner is TimedWriter<File>, .inner.0 is File
                let offset = w.inner_mut().inner.0.stream_position()?;
                // Clone the hasher state to get the final CRC32 without consuming it.
                let crc_value = hasher.lock().clone().finalize();

                index.write_all(&[0u8; 8])?;
                index.write_all(&(offset as i64).to_le_bytes())?;

                if let Some(checksum_file) = &self.output_checksum_file {
                    let mut out = self
                        .output_io_time
                        .wrap_writer(open_shuffle_file(checksum_file)?);
                    // single partition → exactly one checksum value (i64, little-endian)
                    out.write_all(&(crc_value as i64).to_le_bytes())?;
                }
            }
        }

        Ok(())
    }
}
