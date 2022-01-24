/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.bloom.BloomFilterTypeCode;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.io.storage.HoodieHFileConfig.CACHE_DATA_IN_L1;
import static org.apache.hudi.io.storage.HoodieHFileConfig.DROP_BEHIND_CACHE_COMPACTION;
import static org.apache.hudi.io.storage.HoodieHFileConfig.HFILE_COMPARATOR;
import static org.apache.hudi.io.storage.HoodieHFileConfig.PREFETCH_ON_OPEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.when;

public class TestHoodieHFileReaderWriter {
  @TempDir File tempDir;
  private Path filePath;

  @BeforeEach
  public void setup() throws IOException {
    filePath = new Path(tempDir.toString() + "tempFile.txt");
  }

  @AfterEach
  public void clearTempFile() {
    File file = new File(filePath.toString());
    if (file.exists()) {
      file.delete();
    }
  }

  private static Stream<Arguments> populateMetaFieldsAndTestAvroWithMeta() {
    return Arrays.stream(new Boolean[][] {
        {true, true},
        {false, true},
        {true, false},
        {false, false}
    }).map(Arguments::of);
  }

  private HoodieHFileWriter createHFileWriter(Schema avroSchema, boolean populateMetaFields) throws Exception {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(1000, 0.00001, -1, BloomFilterTypeCode.SIMPLE.name());
    Configuration conf = new Configuration();
    TaskContextSupplier mockTaskContextSupplier = Mockito.mock(TaskContextSupplier.class);
    Supplier<Integer> partitionSupplier = Mockito.mock(Supplier.class);
    when(mockTaskContextSupplier.getPartitionIdSupplier()).thenReturn(partitionSupplier);
    when(partitionSupplier.get()).thenReturn(10);
    String instantTime = "000";

    HoodieHFileConfig hoodieHFileConfig = new HoodieHFileConfig(conf, Compression.Algorithm.GZ, 1024 * 1024, 120 * 1024 * 1024,
        PREFETCH_ON_OPEN, CACHE_DATA_IN_L1, DROP_BEHIND_CACHE_COMPACTION, filter, HFILE_COMPARATOR);
    return new HoodieHFileWriter(instantTime, filePath, hoodieHFileConfig, avroSchema, mockTaskContextSupplier, populateMetaFields);
  }

  @ParameterizedTest
  @MethodSource("populateMetaFieldsAndTestAvroWithMeta")
  public void testWriteReadHFile(boolean populateMetaFields, boolean testAvroWithMeta) throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchemaWithMetaFields.avsc");
    HoodieHFileWriter writer = createHFileWriter(avroSchema, populateMetaFields);
    List<String> keys = new ArrayList<>();
    Map<String, GenericRecord> recordMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      GenericRecord record = new GenericData.Record(avroSchema);
      String key = String.format("%s%04d", "key", i);
      record.put("_row_key", key);
      keys.add(key);
      record.put("time", Integer.toString(RANDOM.nextInt()));
      record.put("number", i);
      if (testAvroWithMeta) {
        writer.writeAvroWithMetadata(record, new HoodieRecord(new HoodieKey((String) record.get("_row_key"),
            Integer.toString((Integer) record.get("number"))), new EmptyHoodieRecordPayload())); // payload does not matter. GenericRecord passed in is what matters
        // only HoodieKey will be looked up from the 2nd arg(HoodieRecord).
      } else {
        writer.writeAvro(key, record);
      }
      recordMap.put(key, record);
    }
    writer.close();

    Configuration conf = new Configuration();
    CacheConfig cacheConfig = new CacheConfig(conf);
    HoodieHFileReader hoodieHFileReader = new HoodieHFileReader(conf, filePath, cacheConfig, filePath.getFileSystem(conf));
    List<Pair<String, IndexedRecord>> records = hoodieHFileReader.readAllRecords();
    records.forEach(entry -> assertEquals(entry.getSecond(), recordMap.get(entry.getFirst())));
    hoodieHFileReader.close();

    for (int i = 0; i < 2; i++) {
      int randomRowstoFetch = 5 + RANDOM.nextInt(10);
      Set<String> rowsToFetch = getRandomKeys(randomRowstoFetch, keys);
      List<String> rowsList = new ArrayList<>(rowsToFetch);
      Collections.sort(rowsList);
      hoodieHFileReader = new HoodieHFileReader(conf, filePath, cacheConfig, filePath.getFileSystem(conf));
      List<Pair<String, GenericRecord>> result = hoodieHFileReader.readRecords(rowsList);
      assertEquals(result.size(), randomRowstoFetch);
      result.forEach(entry -> {
        assertEquals(entry.getSecond(), recordMap.get(entry.getFirst()));
        if (populateMetaFields && testAvroWithMeta) {
          assertNotNull(entry.getSecond().get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
        } else {
          assertNull(entry.getSecond().get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
        }
      });
      hoodieHFileReader.close();
    }
  }

  private Set<String> getRandomKeys(int count, List<String> keys) {
    Set<String> rowKeys = new HashSet<>();
    int totalKeys = keys.size();
    while (rowKeys.size() < count) {
      int index = RANDOM.nextInt(totalKeys);
      if (!rowKeys.contains(index)) {
        rowKeys.add(keys.get(index));
      }
    }
    return rowKeys;
  }
}