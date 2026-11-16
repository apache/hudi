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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests {@link LogFileIterator}.
 */
public class TestLogFileIterator {

  private static final int NUM_RECORDS = 5;

  @TempDir
  Path tempDir;

  @Test
  public void testIteratorWithPlainHashMap() {
    Map<String, HoodieRecord> records = new HashMap<>();
    records.put("key1", new HoodieAvroRecord(new HoodieKey("key1", "p1"), null));
    records.put("key2", new HoodieAvroRecord(new HoodieKey("key2", "p1"), null));
    LogFileIterator<HoodieRecord> iterator = new LogFileIterator<>(mockScanner(records));

    assertEquals("key1", iterator.removeLogRecord("key1").get().getRecordKey());

    assertEquals(Collections.singleton("key2"), readKeys(iterator));
  }

  /**
   * The log records left after the base file is merged are read from a spilled map in place:
   * {@link ExternalSpillableMap#values()} would copy every record, spilled ones included, into the heap.
   */
  @ParameterizedTest
  @EnumSource(ExternalSpillableMap.DiskMapType.class)
  public void testIteratorReadsSpilledRecordsInPlace(ExternalSpillableMap.DiskMapType diskMapType) throws Exception {
    ExternalSpillableMap<String, HoodieRecord> spillableMap = Mockito.spy(getSpillableRecordMap(diskMapType));
    try {
      assertTrue(spillableMap.getDiskBasedMapNumEntries() >= NUM_RECORDS - 1, "the records are expected to spill to disk");
      LogFileIterator<HoodieRecord> iterator = new LogFileIterator<>(mockScanner(spillableMap));

      // Base-file records with the same keys are merged first, which removes their log records
      assertEquals("key0", iterator.removeLogRecord("key0").get().getRecordKey());
      assertEquals("key3", iterator.removeLogRecord("key3").get().getRecordKey());

      Set<String> expectedKeys = IntStream.range(0, NUM_RECORDS)
          .filter(i -> i != 0 && i != 3).mapToObj(i -> "key" + i).collect(Collectors.toSet());
      assertEquals(expectedKeys, readKeys(iterator));
      verify(spillableMap, never()).values();
    } finally {
      spillableMap.close();
    }
  }

  private static HoodieMergedLogRecordScanner mockScanner(Map<String, HoodieRecord> records) {
    HoodieMergedLogRecordScanner scanner = Mockito.mock(HoodieMergedLogRecordScanner.class);
    when(scanner.getRecords()).thenReturn(records);
    return scanner;
  }

  private static Set<String> readKeys(LogFileIterator<HoodieRecord> iterator) {
    Set<String> keys = new HashSet<>();
    while (iterator.hasNext()) {
      keys.add(iterator.next().getRecordKey());
    }
    return keys;
  }

  private ExternalSpillableMap<String, HoodieRecord> getSpillableRecordMap(ExternalSpillableMap.DiskMapType diskMapType) throws IOException {
    ExternalSpillableMap<String, HoodieRecord> spillableMap = new ExternalSpillableMap<>(
        1L, // maxInMemorySizeInBytes: at most the first record stays in memory
        tempDir.toString(), new DefaultSizeEstimator<>(), new HoodieRecordSizeEstimator(HoodieTestDataGenerator.AVRO_SCHEMA), diskMapType);
    for (int i = 0; i < NUM_RECORDS; i++) {
      String key = "key" + i;
      spillableMap.put(key, new HoodieAvroRecord(new HoodieKey(key, "p1"), null));
    }
    return spillableMap;
  }
}
