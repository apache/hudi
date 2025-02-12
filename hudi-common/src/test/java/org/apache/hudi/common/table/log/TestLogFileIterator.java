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
import org.apache.hudi.common.serialization.DefaultSerializer;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestLogFileIterator {

  @Test
  public void testIteratorWithPlainHashMap() {
    // 1) Create a simple in-memory Map of HoodieRecords
    Map<String, HoodieRecord> inMemoryMap = new HashMap<>();
    HoodieRecord record1 = new HoodieAvroRecord(new HoodieKey("key1", "p1"), null);
    HoodieRecord record2 = new HoodieAvroRecord(new HoodieKey("key2", "p2"), null);
    inMemoryMap.put("key1", record1);
    inMemoryMap.put("key2", record2);

    // 2) Mock a scanner that returns the inMemoryMap
    HoodieMergedLogRecordScanner scanner = Mockito.mock(HoodieMergedLogRecordScanner.class);
    when(scanner.getRecords()).thenReturn(inMemoryMap);

    // 3) Create the LogFileIterator using the new fix
    LogFileIterator<HoodieRecord> iterator = new LogFileIterator<>(scanner);

    // 4) Validate iteration (should use records.values().iterator())
    assertTrue(iterator.hasNext());
    HoodieRecord next1 = iterator.next();
    assertNotNull(next1);

    assertTrue(iterator.hasNext());
    HoodieRecord next2 = iterator.next();
    assertNotNull(next2);

    assertFalse(iterator.hasNext());
    iterator.close();
  }

  @Test
  public void testIteratorWithExternalSpillableMap() throws Exception {
    // 1) Create ExternalSpillableMap
    ExternalSpillableMap<String, HoodieRecord> spillableMap = getSpillableRecordMap();

    // 3) Mock a scanner that returns the ExternalSpillableMap
    HoodieMergedLogRecordScanner scanner = Mockito.mock(HoodieMergedLogRecordScanner.class);
    when(scanner.getRecords()).thenReturn(spillableMap);

    // 4) Create the LogFileIterator (with the fix)
    LogFileIterator<HoodieRecord> iterator = new LogFileIterator<>(scanner);

    // 5) Validate iteration (should use ExternalSpillableMap.iterator(), not values().iterator())
    int count = 0;
    while (iterator.hasNext()) {
      HoodieRecord rec = iterator.next();
      assertNotNull(rec);
      count++;
    }
    // We expect 2 records
    assertEquals(2, count);

    iterator.close();
  }

  private static ExternalSpillableMap<String, HoodieRecord> getSpillableRecordMap() throws IOException {
    ExternalSpillableMap<String, HoodieRecord> spillableMap =
        new ExternalSpillableMap<>(
            1L, // maxInMemorySizeInBytes
            "/tmp",                        // basePathForSpill
            new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(HoodieTestDataGenerator.AVRO_SCHEMA),
            ExternalSpillableMap.DiskMapType.BITCASK,
            new DefaultSerializer<>(),
            false
        );

    // 2) Put some records in the spillable map
    HoodieRecord recordA = new HoodieAvroRecord(new HoodieKey("keyA", "p1"), null);
    HoodieRecord recordB = new HoodieAvroRecord(new HoodieKey("keyB", "p2"), null);
    spillableMap.put("keyA", recordA);
    spillableMap.put("keyB", recordB);
    return spillableMap;
  }
}
