/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cross-version compatibility test: every HFile written by Hudi's native
 * {@link HFileWriterImpl} must remain openable and fully readable by HBase
 * 2.4.x {@code HFile.createReader()} for the key/value shapes that the
 * Metadata Table (MDT) produces in production.
 *
 * <p>Why this test exists: Hudi 1.x replaced HBase's {@code HFile.WriterFactory}
 * with the native {@code HFileWriterImpl} that hand-rolls the trailer. The
 * trailer layout, fixed at 4096 bytes with {@code HFILE_VERSION = 3}, must stay
 * structurally identical to what HBase's {@code FixedFileTrailer.deserialize()}
 * expects, or every reader still running an older Hudi release will hit
 * {@code CorruptHFileException} on file open. This breaks MDT reads, which
 * blocks both ingestion and queries.
 *
 * <p>The existing {@code TestHFileCompatibility#testHbaseReaderSucceedsWhenKeyValueVersionIsSetTo1}
 * exercises the happy path with 5 records (a single data block), which does not
 * stress the multi-block load-on-open data structures (data-block index,
 * meta-block index) that the trailer points at. This test forces enough records
 * across MDT-shaped key/value sizes to populate those indexes:
 *
 * <ul>
 *   <li>{@code FILES} / {@code COLUMN_STATS} — small keys, medium values</li>
 *   <li>{@code RECORD_INDEX} — record-key-shaped keys (~40 B), small fixed values</li>
 *   <li>{@code SECONDARY_INDEX} — long composite keys (~120 B), small values</li>
 *   <li>{@code BLOOM_FILTERS} — small keys, large values (~64 KB)</li>
 * </ul>
 *
 * <p>For each shape the test writes ~5000 records (≥ 5 data blocks at the
 * default 64 KB block size), reopens with HBase 2.4.x {@code HFile.createReader},
 * asserts that:
 * <ol>
 *   <li>The trailer parses (no exception from {@code createReader}).</li>
 *   <li>{@code getEntries()} matches the count written — proves the trailer
 *       carries the right entry count, not just that it parses.</li>
 *   <li>A full forward scan returns every key in the order written — proves the
 *       data-block index loaded by the trailer points at the right offsets.</li>
 * </ol>
 *
 * <p>If a future change to {@code HFileWriterImpl} reorders, drops, or
 * resizes any trailer field, at least one of these assertions fires.
 */
class TestHudiHFileMdtHbaseReadCompatibility {

  private static final int MDT_RECORDS = 5_000;
  private static final int HFILE_BLOCK_SIZE = 64 * 1024;
  private static final long RNG_SEED = 0xC0DEBA5EL;

  private static Stream<Arguments> mdtShapes() {
    return Stream.of(
        // shape-name, keyLen, valueLen — keyLen must be >= shape-prefix length ("<shape>::") + 10 (idx)
        Arguments.of("FILES", 32, 96),
        Arguments.of("COLUMN_STATS", 32, 256),
        Arguments.of("RECORD_INDEX", 40, 24),
        Arguments.of("SECONDARY_INDEX", 120, 24),
        Arguments.of("BLOOM_FILTERS", 32, 64 * 1024)
    );
  }

  @ParameterizedTest(name = "MDT shape {0} ({1}B key / {2}B value)")
  @MethodSource("mdtShapes")
  void hbaseReaderOpensHudiNativeMdtHFile(String shape, int keyLen, int valueLen)
      throws IOException {
    java.nio.file.Path tempFile = Files.createTempFile(
        "hudi-native-mdt-" + shape.toLowerCase(Locale.ROOT) + "-", ".hfile");
    try {
      Record[] records = generateRecords(MDT_RECORDS, keyLen, valueLen, shape);
      writeWithHudiNative(tempFile, records);

      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      try (HFile.Reader reader = HFile.createReader(
          fs, new Path(tempFile.toString()), conf)) {
        assertEquals(records.length, reader.getEntries(),
            "Trailer entry count mismatch for MDT shape " + shape
                + ". A trailer-layout change in HFileWriterImpl would surface here.");

        HFileScanner scanner = reader.getScanner(true, true);
        assertTrue(scanner.seekTo(),
            "Scanner failed to position at first key for MDT shape " + shape);
        int i = 0;
        do {
          Cell cell = scanner.getCell();
          byte[] key = Arrays.copyOfRange(
              cell.getRowArray(),
              cell.getRowOffset(),
              cell.getRowOffset() + cell.getRowLength());
          assertArrayEquals(records[i].key, key,
              "Key #" + i + " mismatch for MDT shape " + shape
                  + ". Data-block index loaded from the trailer may be pointing at the "
                  + "wrong offsets.");
          i++;
        } while (scanner.next());
        assertEquals(records.length, i,
            "Forward-scan returned fewer keys than written for MDT shape " + shape);
      }
    } finally {
      Files.deleteIfExists(tempFile);
    }
  }

  private static void writeWithHudiNative(java.nio.file.Path filePath, Record[] records)
      throws IOException {
    HFileContext context = HFileContext.builder()
        .blockSize(HFILE_BLOCK_SIZE)
        .build();
    try (DataOutputStream out = new DataOutputStream(Files.newOutputStream(filePath));
         HFileWriter writer = new HFileWriterImpl(context, out)) {
      for (Record r : records) {
        writer.append(new String(r.key, java.nio.charset.StandardCharsets.UTF_8), r.value);
      }
    }
  }

  /**
   * Generate sorted, fixed-width keys with a deterministic prefix per shape and
   * deterministic-but-noisy values. Sorted keys are required by HFile (writer
   * fails on out-of-order append) and match how MDT records actually arrive.
   *
   * <p>The key layout is: {@code <prefix><10-digit-idx><'x' padding>}, then
   * zero-padded on the right up to {@code keyLen}. Truncation is forbidden —
   * if the prefix + idx would not fit in {@code keyLen}, that signals the
   * shape parameter is too small and the test bails immediately rather than
   * silently emit duplicate keys (which would let a broken writer pass).
   */
  private static Record[] generateRecords(int count, int keyLen, int valueLen, String shape) {
    String prefix = shape + "::";
    int minKeyLen = prefix.length() + 10;
    if (keyLen < minKeyLen) {
      throw new IllegalArgumentException(
          "keyLen (" + keyLen + ") for shape " + shape + " is too small; "
              + "must be >= prefix length (" + prefix.length() + ") + 10 idx digits = "
              + minKeyLen + ". Truncating the idx suffix would produce duplicate keys.");
    }
    Random rng = new Random(RNG_SEED);
    Record[] out = new Record[count];
    int padLen = keyLen - minKeyLen;
    for (int i = 0; i < count; i++) {
      String idx = String.format(Locale.ROOT, "%010d", i);
      StringBuilder key = new StringBuilder(keyLen).append(prefix).append(idx);
      for (int j = 0; j < padLen; j++) {
        key.append('x');
      }
      byte[] keyBytes = key.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
      // Invariant: keyBytes.length == keyLen (ASCII-only chars in prefix + idx + 'x').
      byte[] value = new byte[valueLen];
      rng.nextBytes(value);
      out[i] = new Record(keyBytes, value);
    }
    return out;
  }

  private static final class Record {
    final byte[] key;
    final byte[] value;

    Record(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }
  }
}
