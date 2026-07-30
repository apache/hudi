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

package org.apache.hudi.table.format;

import org.apache.hudi.common.bloom.SimpleBloomFilter;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.RowDataQueryContexts;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.lance.file.LanceFileReader;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MAX_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.avro.HoodieBloomFilterWriteSupport.HOODIE_MIN_RECORD_KEY_FOOTER;
import static org.apache.hudi.common.util.hash.Hash.MURMUR_HASH;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link HoodieRowDataLanceReader}.
 */
class TestHoodieRowDataLanceReader {
  private static final StoragePath PATH = new StoragePath("/tmp/test.lance");

  @Test
  void testReadsMetadataAndClosesIdempotently() throws Exception {
    SimpleBloomFilter bloomFilter = new SimpleBloomFilter(100, 0.01, MURMUR_HASH);
    bloomFilter.add("key1");
    Map<String, String> metadata = new HashMap<>();
    metadata.put(HOODIE_MIN_RECORD_KEY_FOOTER, "key1");
    metadata.put(HOODIE_MAX_RECORD_KEY_FOOTER, "key9");
    metadata.put(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, bloomFilter.serializeToString());
    LanceFileReader metadataReader = mock(LanceFileReader.class);
    when(metadataReader.schema()).thenReturn(new Schema(Collections.emptyList(), metadata));
    when(metadataReader.numRows()).thenReturn(9L);

    try (MockedStatic<LanceFileReader> mocked = mockLanceOpen(metadataReader)) {
      HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(PATH, new HoodieConfig());
      assertArrayEquals(new String[] {"key1", "key9"}, reader.readMinMaxRecordKeys());
      assertInstanceOf(SimpleBloomFilter.class, reader.readBloomFilter());
      assertTrue(reader.readBloomFilter().mightContain("key1"));
      assertEquals(9L, reader.getTotalRecords());
      reader.close();
      reader.close();
    }
    verify(metadataReader, times(1)).close();
  }

  @Test
  void testMissingMetadataAndRowCountFailure() throws Exception {
    LanceFileReader metadataReader = mock(LanceFileReader.class);
    when(metadataReader.schema()).thenReturn(new Schema(Collections.emptyList(), null));
    when(metadataReader.numRows()).thenThrow(new IOException("failed"));

    try (MockedStatic<LanceFileReader> mocked = mockLanceOpen(metadataReader)) {
      HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(PATH, new HoodieConfig());
      assertNull(reader.readBloomFilter());
      assertThrows(HoodieException.class, reader::readMinMaxRecordKeys);
      assertThrows(HoodieException.class, reader::getTotalRecords);
      reader.close();
    }
  }

  @Test
  void testFilterRowKeysTracksPhysicalPositions() throws Exception {
    LanceFileReader metadataReader = metadataReader();
    try (MockedStatic<LanceFileReader> mocked = mockLanceOpen(metadataReader)) {
      HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(PATH, new HoodieConfig()) {
        @Override
        public ClosableIterator<String> getRecordKeyIterator() {
          return ClosableIterator.wrap(List.of("key1", "key2", "key3").iterator());
        }
      };

      assertEquals(
          Set.of(Pair.of("key2", 1L)),
          reader.filterRowKeys(Set.of("key2")));
      assertEquals(3, reader.filterRowKeys(Collections.emptySet()).size());
      reader.close();
    }
  }

  @Test
  void testRejectsSchemaEvolution() throws Exception {
    LanceFileReader metadataReader = metadataReader();
    InternalSchemaManager schemaManager = mock(InternalSchemaManager.class);
    InternalSchema mergeSchema = mock(InternalSchema.class);
    when(mergeSchema.isEmptySchema()).thenReturn(false);
    when(schemaManager.getMergeSchema(PATH.getName())).thenReturn(mergeSchema);
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();

    try (MockedStatic<LanceFileReader> mocked = mockLanceOpen(metadataReader)) {
      HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(PATH, new HoodieConfig());
      assertThrows(HoodieValidationException.class, () -> reader.getRowDataIterator(
          schema, schema, schemaManager, Collections.emptyList()));
      reader.close();
    }
  }

  @Test
  void testRecordKeyIteratorReadsProjectedBatch() throws Exception {
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    DataType dataType = RowDataQueryContexts.fromSchema(schema).getRowType();
    RowType rowType = (RowType) dataType.getLogicalType();
    String fieldName = rowType.getFieldNames().get(0);
    LanceFileReader metadataReader = metadataReader();
    LanceFileReader dataReader = mock(LanceFileReader.class);
    ArrowReader arrowReader = mock(ArrowReader.class);
    VectorSchemaRoot batch = mock(VectorSchemaRoot.class);

    try (RootAllocator vectorAllocator = new RootAllocator();
         VarCharVector vector = new VarCharVector(fieldName, vectorAllocator);
         MockedStatic<LanceFileReader> mocked = mockLanceOpen(metadataReader, dataReader)) {
      vector.allocateNew();
      vector.setSafe(0, "key1".getBytes(StandardCharsets.UTF_8));
      vector.setValueCount(1);
      when(dataReader.readAll(eq(List.of(fieldName)), eq(null), eq(512))).thenReturn(arrowReader);
      when(arrowReader.loadNextBatch()).thenReturn(true, false);
      when(arrowReader.getVectorSchemaRoot()).thenReturn(batch);
      when(batch.getFieldVectors()).thenReturn(List.of(vector));
      when(batch.getRowCount()).thenReturn(1);

      HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(PATH, new HoodieConfig());
      try (ClosableIterator<String> iterator = reader.getRecordKeyIterator()) {
        assertTrue(iterator.hasNext());
        assertEquals("key1", iterator.next());
        assertFalse(iterator.hasNext());
      }
      verify(arrowReader).close();
      verify(dataReader).close();
      verify(metadataReader).close();
    }
  }

  @Test
  void testIteratorCreationFailureClosesDataReader() throws Exception {
    HoodieSchema schema = HoodieSchemaUtils.getRecordKeySchema();
    LanceFileReader metadataReader = metadataReader();
    LanceFileReader dataReader = mock(LanceFileReader.class);
    when(dataReader.readAll(any(), eq(null), eq(512))).thenThrow(new IOException("failed"));

    try (MockedStatic<LanceFileReader> mocked = mockLanceOpen(metadataReader, dataReader)) {
      HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(PATH, new HoodieConfig());
      assertThrows(HoodieException.class, () -> reader.getRowDataIterator(
          RowDataQueryContexts.fromSchema(schema).getRowType(), schema));
      verify(dataReader).close();
      reader.close();
    }
  }

  private static LanceFileReader metadataReader() throws Exception {
    LanceFileReader reader = mock(LanceFileReader.class);
    when(reader.schema()).thenReturn(new Schema(Collections.emptyList()));
    return reader;
  }

  private static MockedStatic<LanceFileReader> mockLanceOpen(LanceFileReader... readers) {
    MockedStatic<LanceFileReader> mocked = mockStatic(LanceFileReader.class);
    AtomicInteger readerIndex = new AtomicInteger();
    mocked.when(() -> LanceFileReader.open(eq(PATH.toString()), any(BufferAllocator.class)))
        .thenAnswer(invocation -> readers[readerIndex.getAndIncrement()]);
    return mocked;
  }
}
