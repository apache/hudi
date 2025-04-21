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

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.common.util.CollectionUtils.toStream;
import static org.apache.hudi.io.hfile.TestHFileReader.KEY_CREATOR;
import static org.apache.hudi.io.hfile.TestHFileReader.VALUE_CREATOR;
import static org.apache.hudi.io.storage.TestHoodieReaderWriterUtils.writeHFileForTesting;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieHBaseHFileReaderWriter extends TestHoodieHFileReaderWriterBase {
  @Override
  protected HoodieAvroFileReader createReader(
      HoodieStorage storage) throws Exception {
    return new HoodieHBaseAvroHFileReader(storage.getConf(), getFilePath(), Option.empty());
  }

  @Override
  protected HoodieAvroHFileReaderImplBase createHFileReader(HoodieStorage storage,
                                                            byte[] content) throws IOException {
    FileSystem fs = HadoopFSUtils.getFs(getFilePath().toString(), new Configuration());
    return new HoodieHBaseAvroHFileReader(storage.getConf(), new StoragePath(DUMMY_BASE_PATH), storage, content, Option.empty());
  }

  @Override
  protected void verifyHFileReader(byte[] content,
                                   String hfileName,
                                   boolean mayUseDefaultComparator,
                                   Class<?> expectedComparatorClazz,
                                   int count) throws IOException {
    HoodieStorage storage = HoodieTestUtils.getStorage(getFilePath());
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
        createReader(HoodieTestUtils.getStorage(getFilePath()))) {
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
}
