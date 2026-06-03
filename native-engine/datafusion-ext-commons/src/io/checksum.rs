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

use crc32fast::Hasher;

/// Per-partition CRC32 checksums, computed incrementally as data is written.
/// Each partition maintains its own hasher so that the final checksum covers
/// all bytes written to that partition (across multiple batches / spills).
pub struct PartitionChecksums {
    hashers: Vec<Hasher>,
}

impl PartitionChecksums {
    pub fn new(num_partitions: usize) -> Self {
        Self {
            hashers: (0..num_partitions).map(|_| Hasher::new()).collect(),
        }
    }

    /// Feed `data` bytes into the checksum for the given partition.
    pub fn update(&mut self, partition_id: usize, data: &[u8]) {
        self.hashers[partition_id].update(data);
    }

    /// Consume the struct and return one checksum value per partition.
    /// Values are returned as `u64` (widened from `u32`) to match Spark's
    /// `long[]` checksum array.
    pub fn finalize(self) -> Vec<u64> {
        self.hashers
            .into_iter()
            .map(|h| h.finalize() as u64)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_empty() {
        let mut cs = PartitionChecksums::new(1);
        cs.update(0, &[]);
        let result = cs.finalize();
        // CRC32 of empty bytes is 0
        assert_eq!(result[0], 0u64);
    }

    #[test]
    fn test_crc32_known_value() {
        let mut cs = PartitionChecksums::new(1);
        cs.update(0, b"hello");
        let result = cs.finalize();
        // CRC32 of "hello" is 0x3610a686
        assert_eq!(result[0], 0x3610a686u64);
    }

    #[test]
    fn test_crc32_incremental() {
        // incremental update must equal one-shot update
        let mut cs1 = PartitionChecksums::new(1);
        cs1.update(0, b"hel");
        cs1.update(0, b"lo");

        let mut cs2 = PartitionChecksums::new(1);
        cs2.update(0, b"hello");

        assert_eq!(cs1.finalize(), cs2.finalize());
    }

    #[test]
    fn test_multiple_partitions() {
        let mut cs = PartitionChecksums::new(3);
        cs.update(0, b"part0");
        cs.update(1, b"part1");
        cs.update(2, b"part2");
        let result = cs.finalize();

        let mut expected = vec![Hasher::new(); 3];
        expected[0].update(b"part0");
        expected[1].update(b"part1");
        expected[2].update(b"part2");

        for (i, h) in expected.into_iter().enumerate() {
            assert_eq!(result[i], h.finalize() as u64);
        }
    }
}
