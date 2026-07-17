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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage;

import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.storage.HoodieRangeReadHandle.ByteRange;
import org.apache.hudi.storage.HoodieRangeReadHandle.RangeReadResult;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.parquet.io.SeekableInputStream;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.hudi.io.storage.HoodiePrefetchedParquetInputFile.RegionKind.DICTIONARY;
import static org.apache.hudi.io.storage.HoodiePrefetchedParquetInputFile.RegionKind.PAGE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHoodiePrefetchedParquetInputFile {

  @Test
  void servesContainedReadsAndClassifiesFallbacks() throws Exception {
    byte[] file = new byte[64];
    for (int i = 0; i < file.length; i++) {
      file[i] = (byte) i;
    }
    HoodieStorage storage = mock(HoodieStorage.class);
    when(storage.getDefaultBufferSize()).thenReturn(4096);
    when(storage.openSeekable(any(), anyInt(), anyBoolean())).thenAnswer(ignored ->
        new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(file)));

    byte[] cached = Arrays.copyOfRange(file, 10, 30);
    RangeReadResult prefetched = new RangeReadResult(
        new ByteRange(10, cached.length), ByteBuffer.wrap(cached));
    HoodiePrefetchedParquetInputFile inputFile = new HoodiePrefetchedParquetInputFile(
        storage,
        new StoragePath("file:///vectors.parquet"),
        file.length,
        Collections.singletonList(prefetched),
        Arrays.asList(
            new HoodiePrefetchedParquetInputFile.ReadRegion(0, 40, PAGE),
            new HoodiePrefetchedParquetInputFile.ReadRegion(50, 8, DICTIONARY)));

    try (SeekableInputStream stream = inputFile.newStream()) {
      byte[] headerAndPageFragment = new byte[8];
      stream.seek(12);
      stream.readFully(headerAndPageFragment);
      assertArrayEquals(Arrays.copyOfRange(file, 12, 20), headerAndPageFragment);

      byte[] partialOverlap = new byte[5];
      stream.seek(8);
      stream.readFully(partialOverlap);
      assertArrayEquals(Arrays.copyOfRange(file, 8, 13), partialOverlap);

      byte[] metadata = new byte[4];
      stream.seek(42);
      stream.readFully(metadata);

      byte[] dictionary = new byte[4];
      stream.seek(52);
      stream.readFully(dictionary);
    }

    HoodiePrefetchedParquetInputFile.Metrics metrics = inputFile.getMetrics();
    assertEquals(8, metrics.getPrefetchHitBytes(), "contained reads must hit regardless of alignment");
    assertEquals(5, metrics.getPartialOverlapBytes());
    assertEquals(5, metrics.getUncoveredPageMissBytes());
    assertEquals(4, metrics.getMetadataMissBytes());
    assertEquals(4, metrics.getDictionaryMissBytes());
  }
}
