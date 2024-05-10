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

package org.apache.hudi.io.hadoop;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.testutils.FileSystemTestUtils.RANDOM;
import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.io.hfile.TestHFileReader.KEY_CREATOR;
import static org.apache.hudi.io.hfile.TestHFileReader.VALUE_CREATOR;
import static org.apache.hudi.io.storage.TestHoodieReaderWriterUtils.writeHFileForTesting;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieHBaseHFileReaderWriter extends TestHoodieHFileReaderWriterBase {
  @Override
  protected HoodieAvroFileReader createReader(
      StorageConfiguration<?> conf) throws Exception {
    return new HoodieHBaseAvroHFileReader(conf, getFilePath(), Option.empty());
  }

  @Override
  protected HoodieAvroHFileReaderImplBase createHFileReader(StorageConfiguration<?> conf,
                                                            byte[] content) throws IOException {
    FileSystem fs = HadoopFSUtils.getFs(getFilePath().toString(), new Configuration());
    return new HoodieHBaseAvroHFileReader(conf, new StoragePath(DUMMY_BASE_PATH),
        HoodieStorageUtils.getStorage(getFilePath(), conf), content, Option.empty());
  }

  @Override
  protected void verifyHFileReader(byte[] content,
                                   String hfileName,
                                   boolean mayUseDefaultComparator,
                                   Class<?> expectedComparatorClazz,
                                   int count) throws IOException {
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        getFilePath(), HadoopFSUtils.getStorageConf(new Configuration()));
    try (HFile.Reader reader =
             HoodieHFileUtils.createHFileReader(storage, new StoragePath(DUMMY_BASE_PATH), content)) {
      // HFile version is 3
      assertEquals(3, reader.getTrailer().getMajorVersion());
      if (mayUseDefaultComparator && hfileName.contains("hudi_0_9")) {
        // Pre Hudi 0.10, the default comparator is used for metadata table HFiles
        // For bootstrap index HFiles, the custom comparator is always used
        assertEquals(CellComparatorImpl.class, reader.getComparator().getClass());
      } else {
        assertEquals(expectedComparatorClazz, reader.getComparator().getClass());
      }
      assertEquals(count, reader.getEntries());
    }
  }

  @Test
  public void testReaderGetRecordIteratorByKeysWithBackwardSeek() throws Exception {
    writeFileWithSimpleSchema();
    try (HoodieAvroHFileReaderImplBase hfileReader = (HoodieAvroHFileReaderImplBase)
        createReader(HadoopFSUtils.getStorageConf(new Configuration()))) {
      Schema avroSchema =
          getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
      List<GenericRecord> allRecords = toStream(hfileReader.getRecordIterator())
          .map(r -> (GenericRecord) r.getData()).collect(Collectors.toList());
      // Filter for "key00001, key05, key24, key16, key31, key61". Valid entries should be matched.
      // Even though key16 exists, it's a backward seek not in order. So, will not return the matched entry.
      List<GenericRecord> expectedKey1s = allRecords.stream().filter(entry -> (
          (entry.get("_row_key").toString()).contains("key05")
              || (entry.get("_row_key").toString()).contains("key24")
              || (entry.get("_row_key").toString()).contains("key31"))).collect(Collectors.toList());
      Iterator<IndexedRecord> iterator =
          hfileReader.getIndexedRecordsByKeysIterator(
              Arrays.asList("key00001", "key05", "key24", "key16", "key31", "key61"),
              avroSchema);
      List<GenericRecord> recordsByKeys =
          StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
              .map(r -> (GenericRecord) r)
              .collect(Collectors.toList());
      assertEquals(expectedKey1s, recordsByKeys);
    }
  }

  @Disabled("This is used for generating testing HFile only")
  @ParameterizedTest
  @CsvSource({
      "512,GZ,20000,true", "16,GZ,20000,true",
      "64,NONE,5000,true", "16,NONE,5000,true",
      "16,GZ,200,false"
  })
  void generateHFileForTesting(int blockSizeKB,
                               String compressionCodec,
                               int numEntries,
                               boolean uniqueKeys) throws IOException {
    writeHFileForTesting(
        String.format("/tmp/hudi_1_0_hbase_2_4_9_%sKB_%s_%s.hfile",
            blockSizeKB, compressionCodec, numEntries),
        blockSizeKB * 1024,
        Compression.Algorithm.valueOf(compressionCodec),
        numEntries,
        KEY_CREATOR,
        VALUE_CREATOR,
        uniqueKeys);
  }

  /**
   * Test HFile reader with duplicates.
   * HFile can have duplicates in case of secondary index for instance.
   */
  @Test
  public void testHFileReaderWriterWithDuplicates() throws Exception {
    Schema avroSchema = getSchemaFromResource(TestHoodieOrcReaderWriter.class, "/exampleSchema.avsc");
    HoodieAvroHFileWriter writer = createWriter(avroSchema, false);
    List<String> keys = new ArrayList<>();
    Map<String, List<GenericRecord>> recordMap = new TreeMap<>();
    for (int i = 0; i < 50; i++) {
      // If i is a multiple of 10, select the previous key for duplication
      String key = i != 0 && i % 10 == 0 ? String.format("%s%04d", "key", i - 1) : String.format("%s%04d", "key", i);

      // Create a list of records for each key to handle duplicates
      if (!recordMap.containsKey(key)) {
        recordMap.put(key, new ArrayList<>());
      }

      // Create the record
      GenericRecord record = new GenericData.Record(avroSchema);
      record.put("_row_key", key);
      record.put("time", Integer.toString(RANDOM.nextInt()));
      record.put("number", i);
      writer.writeAvro(key, record);

      // Add to the record map and key list
      recordMap.get(key).add(record);
      keys.add(key);
    }
    writer.close();

    try (HoodieAvroHFileReaderImplBase hFileReader = (HoodieAvroHFileReaderImplBase)
        createReader(HadoopFSUtils.getStorageConf(new Configuration()))) {
      List<IndexedRecord> records = HoodieAvroHFileReaderImplBase.readAllRecords(hFileReader);
      assertEquals(recordMap.values().stream().flatMap(List::stream).collect(Collectors.toList()), records);
    }

    for (int i = 0; i < 2; i++) {
      int randomRowstoFetch = 5 + RANDOM.nextInt(10);
      Set<String> rowsToFetch = getRandomKeys(randomRowstoFetch, keys);

      List<String> rowsList = new ArrayList<>(rowsToFetch);
      Collections.sort(rowsList);

      List<GenericRecord> expectedRecords = rowsList.stream().flatMap(row -> recordMap.get(row).stream()).collect(Collectors.toList());

      try (HoodieAvroHFileReaderImplBase hFileReader = (HoodieAvroHFileReaderImplBase)
          createReader(HadoopFSUtils.getStorageConf(new Configuration()))) {
        List<GenericRecord> result = HoodieAvroHFileReaderImplBase.readRecords(hFileReader, rowsList).stream().map(r -> (GenericRecord) r).collect(Collectors.toList());
        assertEquals(expectedRecords, result);
      }
    }
  }
}
