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
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.storage.row.HoodieRowDataLanceWriter;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.RowDataQueryContexts;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.file.LanceFileReader;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
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

  @TempDir
  Path tempDir;

  @Test
  void testRestoresMixedCaseVectorFieldName() throws Exception {
    HoodieSchema hoodieSchema = HoodieSchema.createRecord(
        "mixed_case_record",
        null,
        null,
        Collections.singletonList(HoodieSchemaField.of(
            "Embedding",
            HoodieSchema.createNullable(HoodieSchema.createVector(
                2, HoodieSchema.Vector.VectorElementType.FLOAT)),
            null,
            HoodieSchema.NULL_VALUE)));
    StoragePath path = new StoragePath(tempDir.resolve("mixed-case-vector.lance").toUri());

    try (HoodieRowDataLanceWriter writer = new HoodieRowDataLanceWriter(
        path,
        hoodieSchema,
        "001",
        mock(TaskContextSupplier.class),
        Option.empty(),
        128 * 1024 * 1024L,
        64 * 1024 * 1024L,
        16 * 1024 * 1024L,
        true,
        false,
        false)) {
      writer.writeRow("key1", GenericRowData.of(
          new GenericArrayData(new Object[] {1.25F, 2.5F})));
    }

    try (HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(path, new HoodieConfig())) {
      HoodieSchema restoredSchema = reader.getSchema().getNonNullType();
      assertTrue(restoredSchema.getField("Embedding").isPresent());
      assertFalse(restoredSchema.getField("embedding").isPresent());
      HoodieSchema.Vector vector = (HoodieSchema.Vector) restoredSchema.getField("Embedding")
          .get().schema().getNonNullType();
      assertEquals(2, vector.getDimension());
      assertEquals(HoodieSchema.Vector.VectorElementType.FLOAT, vector.getVectorElementType());
    }
  }

  @Test
  void testReadsVectorsAndRestoresSchemaIdentity() throws Exception {
    RowType rowType = RowType.of(
        new LogicalType[] {
            new IntType(false),
            new ArrayType(true, new FloatType(false)),
            new ArrayType(false, new DoubleType(false)),
            new ArrayType(false, new IntType(false))
        },
        new String[] {"id", "embedding", "features", "values"});
    HoodieSchema hoodieSchema = HoodieSchemaConverter.convertToSchema(
        rowType, "vector_record", "embedding:2,features:3");
    StoragePath path = new StoragePath(tempDir.resolve("vectors.lance").toUri());

    try (HoodieRowDataLanceWriter writer = new HoodieRowDataLanceWriter(
        path,
        hoodieSchema,
        "001",
        mock(TaskContextSupplier.class),
        Option.empty(),
        128 * 1024 * 1024L,
        64 * 1024 * 1024L,
        16 * 1024 * 1024L,
        true,
        false,
        false)) {
      writer.writeRow("key1", GenericRowData.of(
          1,
          new GenericArrayData(new Object[] {1.25F, 2.5F}),
          new GenericArrayData(new Object[] {3.5D, 4.5D, 5.5D}),
          new GenericArrayData(new Object[] {10, 20})));
      writer.writeRow("key2", GenericRowData.of(
          2,
          null,
          new GenericArrayData(new Object[] {6.5D, 7.5D, 8.5D}),
          new GenericArrayData(new Object[] {30})));
    }

    try (HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(path, new HoodieConfig())) {
      HoodieSchema readSchema = reader.getSchema().getNonNullType();
      HoodieSchema.Vector floatVector = (HoodieSchema.Vector) readSchema.getField("embedding")
          .get().schema().getNonNullType();
      HoodieSchema.Vector doubleVector = (HoodieSchema.Vector) readSchema.getField("features")
          .get().schema().getNonNullType();
      assertEquals(HoodieSchemaType.VECTOR, floatVector.getType());
      assertEquals(2, floatVector.getDimension());
      assertEquals(HoodieSchema.Vector.VectorElementType.FLOAT, floatVector.getVectorElementType());
      assertEquals(3, doubleVector.getDimension());
      assertEquals(HoodieSchema.Vector.VectorElementType.DOUBLE, doubleVector.getVectorElementType());
      assertEquals(HoodieSchemaType.ARRAY,
          readSchema.getField("values").get().schema().getNonNullType().getType());

      try (ClosableIterator<RowData> rows = reader.getRowDataIterator(hoodieSchema)) {
        RowData first = rows.next();
        assertEquals(1, first.getInt(0));
        assertFloatArray(first.getArray(1), 1.25F, 2.5F);
        assertDoubleArray(first.getArray(2), 3.5D, 4.5D, 5.5D);
        assertIntArray(first.getArray(3), 10, 20);

        RowData second = rows.next();
        assertEquals(2, second.getInt(0));
        assertTrue(second.isNullAt(1));
        assertDoubleArray(second.getArray(2), 6.5D, 7.5D, 8.5D);
        assertIntArray(second.getArray(3), 30);
        assertFalse(rows.hasNext());
      }
    }

    RowType projectedRowType = RowType.of(
        new LogicalType[] {
            new ArrayType(false, new IntType(false)),
            new ArrayType(false, new DoubleType(false)),
            new IntType(false),
            new ArrayType(true, new FloatType(false))
        },
        new String[] {"values", "features", "id", "embedding"});
    HoodieSchema projectedSchema = HoodieSchemaConverter.convertToSchema(
        projectedRowType, "projected_record", "features:3,embedding:2");
    try (HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(path, new HoodieConfig());
         ClosableIterator<RowData> rows = reader.getRowDataIterator(projectedSchema)) {
      RowData first = rows.next();
      assertIntArray(first.getArray(0), 10, 20);
      assertDoubleArray(first.getArray(1), 3.5D, 4.5D, 5.5D);
      assertEquals(1, first.getInt(2));
      assertFloatArray(first.getArray(3), 1.25F, 2.5F);
    }

    RowType incompatibleRowType = RowType.of(
        new LogicalType[] {new ArrayType(true, new FloatType(false))},
        new String[] {"embedding"});
    HoodieSchema incompatibleSchema = HoodieSchemaConverter.convertToSchema(
        incompatibleRowType, "incompatible_record", "embedding:3");
    try (HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(path, new HoodieConfig())) {
      HoodieValidationException exception = assertThrows(
          HoodieValidationException.class,
          () -> reader.getRowDataIterator(incompatibleSchema));
      assertTrue(exception.getMessage().contains("requested VECTOR(3)"));
      assertTrue(exception.getMessage().contains("file contains VECTOR(2)"));
    }
  }

  @Test
  void testRejectsInvalidVectorArrowTypes() throws Exception {
    RowType rowType = RowType.of(
        new LogicalType[] {new ArrayType(false, new FloatType(false))},
        new String[] {"embedding"});
    HoodieSchema requestedSchema = HoodieSchemaConverter.convertToSchema(
        rowType, "vector_record", "embedding:2");

    Field element = new Field(
        "element", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null);
    assertInvalidVectorEncoding(
        requestedSchema,
        new Field("embedding", FieldType.nullable(new ArrowType.List()), Collections.singletonList(element)),
        "expected FixedSizeList<Float32|Float64>");

    Field halfElement = new Field(
        "element", FieldType.notNullable(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)), null);
    assertInvalidVectorEncoding(
        requestedSchema,
        new Field(
            "embedding",
            FieldType.nullable(new ArrowType.FixedSizeList(2)),
            Collections.singletonList(halfElement)),
        "expected Float32 or Float64 elements");
  }

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
      assertThrows(HoodieException.class, () -> reader.getRowDataIterator(schema));
      verify(dataReader).close();
      reader.close();
    }
  }

  private static void assertFloatArray(ArrayData array, float... expected) {
    assertEquals(expected.length, array.size());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], array.getFloat(i));
    }
  }

  private static void assertDoubleArray(ArrayData array, double... expected) {
    assertEquals(expected.length, array.size());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], array.getDouble(i));
    }
  }

  private static void assertIntArray(ArrayData array, int... expected) {
    assertEquals(expected.length, array.size());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], array.getInt(i));
    }
  }

  private static LanceFileReader metadataReader() throws Exception {
    LanceFileReader reader = mock(LanceFileReader.class);
    when(reader.schema()).thenReturn(new Schema(Collections.emptyList()));
    return reader;
  }

  private static void assertInvalidVectorEncoding(
      HoodieSchema requestedSchema, Field field, String expectedMessage) throws Exception {
    LanceFileReader metadataReader = mock(LanceFileReader.class);
    when(metadataReader.schema()).thenReturn(new Schema(Collections.singletonList(field)));
    try (MockedStatic<LanceFileReader> mocked = mockLanceOpen(metadataReader);
         HoodieRowDataLanceReader reader = new HoodieRowDataLanceReader(PATH, new HoodieConfig())) {
      HoodieValidationException exception = assertThrows(
          HoodieValidationException.class,
          () -> reader.getRowDataIterator(requestedSchema));
      assertTrue(exception.getMessage().contains(expectedMessage));
    }
  }

  private static MockedStatic<LanceFileReader> mockLanceOpen(LanceFileReader... readers) {
    MockedStatic<LanceFileReader> mocked = mockStatic(LanceFileReader.class);
    AtomicInteger readerIndex = new AtomicInteger();
    mocked.when(() -> LanceFileReader.open(eq(PATH.toString()), any(BufferAllocator.class)))
        .thenAnswer(invocation -> readers[readerIndex.getAndIncrement()]);
    return mocked;
  }
}
