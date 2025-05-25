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
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieAvroHFileReaderImplBase;
import org.apache.hudi.io.storage.HoodieNativeAvroHFileReader;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestHoodieHFileReaderWriter extends TestHoodieHFileReaderWriterBase {

  @Override
  protected HoodieAvroFileReader createReader(
      HoodieStorage storage) throws Exception {
    return new HoodieNativeAvroHFileReader(storage, getFilePath(), Option.empty());
  }

  @Override
  protected HoodieAvroHFileReaderImplBase createHFileReader(HoodieStorage storage,
                                                            byte[] content) throws IOException {
    return new HoodieNativeAvroHFileReader(storage, content, Option.empty());
  }

  @Override
  protected void verifyHFileReader(byte[] content,
                                   String hfileName,
                                   boolean mayUseDefaultComparator,
                                   Class<?> expectedComparatorClazz,
                                   int count) throws IOException {
    try (HoodieAvroHFileReaderImplBase hfileReader = createHFileReader(HoodieTestUtils.getStorage(hfileName), content)) {
      assertEquals(count, hfileReader.getTotalRecords());
    }
  }

  @Test
  public void testReaderGetRecordIteratorByKeysWithBackwardSeek() throws Exception {
    writeFileWithSimpleSchema();
    try (HoodieAvroHFileReaderImplBase hfileReader = (HoodieAvroHFileReaderImplBase)
        createReader(HoodieTestUtils.getStorage(getFilePath()))) {
      Schema avroSchema =
          getSchemaFromResource(TestHoodieReaderWriterBase.class, "/exampleSchema.avsc");
      // Filter for "key00001, key05, key24, key16, key31, key61".
      // Even though key16 exists, it's a backward seek not in order.
      // Our native HFile reader does not allow backward seek, and throws an exception
      // Note that backward seek is not expected to happen in production code
      try (ClosableIterator<IndexedRecord> iterator =
          hfileReader.getIndexedRecordsByKeysIterator(
              Arrays.asList("key00001", "key05", "key24", "key16", "key31", "key61"),
              avroSchema)) {
        assertThrows(
            IllegalStateException.class,
            () -> StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .collect(Collectors.toList()));
      }
    }
  }
}
