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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.hadoop.HoodieAvroHFileWriter;
import org.apache.hudi.io.hadoop.TestHoodieOrcReaderWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.function.Supplier;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieNativeAvroHFileReaderCaching {

  @TempDir
  public static Path tempDir;

  // Fixed seed for reproducible tests
  private static final Random RANDOM = new Random(42);
  private static final int KEYS_TO_LOOKUP = 10_000;
  private static final List<String> EXISTING_KEYS = new ArrayList<>();
  private static final List<String> MISSING_KEYS = new ArrayList<>();
  private static HoodieStorage storage;

  @BeforeAll
  public static void setup() throws Exception {
    storage = HoodieTestUtils.getStorage(getFilePath());
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchemaWithMetaFields.avsc");
    HoodieAvroHFileWriter writer = createWriter(avroSchema, true);

    // Write records with for realistic testing
    final int numRecords = 50_000;
    System.out.println("Creating HFile with " + numRecords + " records...");

    for (int i = 0; i < numRecords; i++) {
      String key = String.format("key_%08d", i);
      EXISTING_KEYS.add(key);

      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("_row_key", key);
      record.put("time", Integer.toString(RANDOM.nextInt()));
      record.put("number", i);

      writer.writeAvroWithMetadata(
          new HoodieAvroRecord(new HoodieKey(key, "partition_" + (i % 10)),
              new EmptyHoodieRecordPayload()).getKey(), record);
    }
    writer.close();

    // Generate missing keys that don't exist in the HFile
    for (int i = 0; i < KEYS_TO_LOOKUP; i++) {
      String missingKey = String.format("missing_key_%08d", i + numRecords);
      MISSING_KEYS.add(missingKey);
    }

    System.out.println("HFile created with " + EXISTING_KEYS.size() + " existing keys");
    System.out.println("Generated " + MISSING_KEYS.size() + " missing keys for testing");
  }

  @Test
  @Disabled("Enable this for local performance tests")
  public void testBlockCachePerformanceOnRecordLevelIndex() throws Exception {
    System.out.println("\n=== HFile BlockCache Performance Test ===");

    // Test existing keys lookup performance
    testExistingKeysLookup();

    // Test missing keys lookup performance 
    testMissingKeysLookup();

    System.out.println("================================================================\n");
  }

  private void testExistingKeysLookup() throws Exception {
    System.out.println("\n--- Testing " + KEYS_TO_LOOKUP + " Existing Key Lookups ---");

    // Select 10K random existing keys
    Collections.shuffle(EXISTING_KEYS, RANDOM);
    List<String> testKeys = EXISTING_KEYS.subList(0, KEYS_TO_LOOKUP);
    testKeys.sort(String::compareTo);

    // Warm up JVM
    warmupReads(testKeys.subList(0, 1000));

    // Test without cache
    long cacheTime = measureLookupTime(testKeys, true);
    long noCacheTime = measureLookupTime(testKeys, false);

    double speedup = (double) noCacheTime / cacheTime;

    System.out.printf(KEYS_TO_LOOKUP + " Existing Key Lookups:\n");
    System.out.printf("  - Without BlockCache: %d ms\n", noCacheTime);
    System.out.printf("  - With BlockCache: %d ms\n", cacheTime);
    System.out.printf("  - Speedup: %.2fx\n", speedup);
    System.out.printf("  - Performance Improvement: %.1f%%\n", (speedup - 1) * 100);

    assertTrue(speedup > 1.0, "BlockCache should provide speedup for existing key lookups");
  }

  private void testMissingKeysLookup() throws Exception {
    System.out.println("\n--- Testing " + KEYS_TO_LOOKUP + " Missing Key Lookups ---");

    // Use all 1k missing keys
    List<String> testKeys = new ArrayList<>(MISSING_KEYS);

    // Warm up JVM 
    warmupReads(testKeys.subList(0, 1000));

    // Test without cache
    long noCacheTime = measureLookupTime(testKeys, false, false);

    // Test with cache  
    long cacheTime = measureLookupTime(testKeys, true, false);

    double speedup = (double) noCacheTime / cacheTime;

    System.out.printf(KEYS_TO_LOOKUP + " Missing Key Lookups:\n");
    System.out.printf("  - Without BlockCache: %d ms\n", noCacheTime);
    System.out.printf("  - With BlockCache: %d ms\n", cacheTime);
    System.out.printf("  - Speedup: %.2fx\n", speedup);
    System.out.printf("  - Performance Improvement: %.1f%%\n", (speedup - 1) * 100);

    // Missing keys may not benefit as much from caching but should not be slower
    assertTrue(speedup >= 0.8, "BlockCache should not significantly slow down missing key lookups");
  }

  private void warmupReads(List<String> keys) throws Exception {
    // Warm up JVM to reduce noise in measurements
    try (HoodieAvroHFileReaderImplBase reader = createReader(storage, true, true)) {
      ClosableIterator<IndexedRecord> iter = reader.getIndexedRecordsByKeysIterator(keys, reader.getSchema());
      // Consume all records
      toStream(iter).forEach(record -> {
      });
    }
  }

  private long measureLookupTime(List<String> keys, boolean enableCache) throws Exception {
    return measureLookupTime(keys, enableCache, false);
  }

  private long measureLookupTime(List<String> keys, boolean enableCache, boolean useBloomFilter) throws Exception {
    // Force garbage collection before measurement
    System.gc();
    Thread.sleep(100); // Allow GC to complete

    long startTime = System.nanoTime();
    int totalRecordCount = 0;

    // Look up each key individually to test realistic usage pattern

    for (String key : keys) {
      try (HoodieAvroHFileReaderImplBase reader = createReader(storage, useBloomFilter, enableCache)) {
        List<String> singleKey = Collections.singletonList(key);
        ClosableIterator<IndexedRecord> iter = reader.getIndexedRecordsByKeysIterator(singleKey, reader.getSchema());

        // Count records for this key
        while (iter.hasNext()) {
          iter.next();
          totalRecordCount++;
        }
        iter.close();
      }
    }
    long endTime = System.nanoTime();

    // Validate we found the expected number of records
    boolean isExistingKeysTest = keys.size() <= EXISTING_KEYS.size()
        && EXISTING_KEYS.containsAll(keys);
    if (isExistingKeysTest) {
      assertTrue(totalRecordCount > 0, "Should find existing records");
    }

    return (endTime - startTime) / 1_000_000; // Convert to milliseconds
  }

  // Helper methods

  private static HoodieAvroHFileWriter createWriter(Schema avroSchema, boolean populateMetaFields) throws Exception {
    String instantTime = "000";
    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.POPULATE_META_FIELDS.key(), Boolean.toString(populateMetaFields));
    TaskContextSupplier mockTaskContextSupplier = mock(TaskContextSupplier.class);
    Supplier<Integer> partitionSupplier = mock(Supplier.class);
    when(mockTaskContextSupplier.getPartitionIdSupplier()).thenReturn(partitionSupplier);
    when(partitionSupplier.get()).thenReturn(10);

    return (HoodieAvroHFileWriter) HoodieFileWriterFactory.getFileWriter(
        instantTime, getFilePath(), storage, HoodieStorageConfig.newBuilder().fromProperties(props).build(), HoodieSchema.fromAvroSchema(avroSchema),
        mockTaskContextSupplier, HoodieRecord.HoodieRecordType.AVRO);
  }

  private static StoragePath getFilePath() {
    return new StoragePath(tempDir.toString() + "/perf_test.hfile");
  }

  private HoodieAvroHFileReaderImplBase createReader(HoodieStorage storage, boolean useBloomFilter, boolean enableCache) throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(), String.valueOf(enableCache));
    // Use a cache that can hold 100 blocks
    props.setProperty(HoodieReaderConfig.HFILE_BLOCK_CACHE_SIZE.key(), String.valueOf(100));

    HFileReaderFactory readerFactory = HFileReaderFactory.builder()
        .withStorage(storage)
        .withPath(getFilePath())
        .withProps(props)
        .build();
    return HoodieNativeAvroHFileReader.builder()
        .readerFactory(readerFactory).path(getFilePath()).useBloomFilter(useBloomFilter).build();
  }
}
