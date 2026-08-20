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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestHiveHoodieReaderContext {
  private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
  private final HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator = mock(HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator.class);
  private final StorageConfiguration<?> storageConfiguration = new HadoopStorageConfiguration(false);
  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("TestRecord", null, null,
      Arrays.asList(
          HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
          HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
          HoodieSchemaField.of("active", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
      ));

  @Test
  void getRecordKeyWithSingleKey() {
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"field_1"}));
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("value1", avroReaderContext.getRecordContext().getRecordKey(row, getBaseSchema()));
  }

  @Test
  void getRecordKeyWithMultipleKeys() {
    when(tableConfig.populateMetaFields()).thenReturn(false);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"field_1", "field_3.nested_field"}));
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("field_1:value1,field_3.nested_field:value3", avroReaderContext.getRecordContext().getRecordKey(row, getBaseSchema()));
  }

  @Test
  void getNestedField() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    ArrayWritable row = new ArrayWritable(Writable.class, new Writable[]{new Text("value1"), new Text("value2"), new ArrayWritable(new String[]{"value3"})});

    assertEquals("value3", avroReaderContext.getRecordContext().getValue(row, getBaseSchema(), "field_3.nested_field").toString());
  }

  @Test
  void testConstructEngineRecordWithFieldValues() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    Object[] fieldVals = new Writable[]{
        new IntWritable(1),
        new Text("Alice"),
        new BooleanWritable(true)};
    ArrayWritable row = avroReaderContext.getRecordContext().constructEngineRecord(SCHEMA, fieldVals);
    Writable[] values = row.get();
    assertEquals(fieldVals[0], values[0]);
    assertEquals(fieldVals[1], values[1]);
    assertEquals(fieldVals[2], values[2]);
  }

  @Test
  void testConstructEngineRecordWithNoUpdates() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    ArrayWritable base = createBaseRecord(new Writable[]{
        new IntWritable(1),
        new Text("Alice"),
        new BooleanWritable(true)});
    BufferedRecord<ArrayWritable> buffered = new BufferedRecord<>("anyKey", 1, base, 1, null);

    Map<Integer, Object> updates = new HashMap<>();
    ArrayWritable result = avroReaderContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, buffered);
    Writable[] values = result.get();

    assertEquals(1, ((IntWritable) values[0]).get());
    assertEquals("Alice", values[1].toString());
    assertTrue(((BooleanWritable) values[2]).get());
  }

  @Test
  void testConstructEngineRecordWithUpdates() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext avroReaderContext = new HiveHoodieReaderContext(
        readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);

    ArrayWritable base = createBaseRecord(new Writable[]{
        new IntWritable(1),
        new Text("Alice"),
        new BooleanWritable(true)});
    BufferedRecord<ArrayWritable> buffered = new BufferedRecord<>("anyKey", 1, base, 1, null);

    Map<Integer, Object> updates = new HashMap<>();
    updates.put(0, new IntWritable(2));
    updates.put(1, new Text("Bob"));
    ArrayWritable result = avroReaderContext.getRecordContext().mergeWithEngineRecord(SCHEMA, updates, buffered);
    Writable[] values = result.get();

    assertEquals(2, ((IntWritable) values[0]).get());
    assertEquals("Bob", values[1].toString());
    assertTrue(((BooleanWritable) values[2]).get());
  }

  private static HoodieSchema getBaseSchema() {
    HoodieSchema nestedSchema = HoodieSchema.createRecord("nested", null, null,
        Collections.singletonList(HoodieSchemaField.of("nested_field", HoodieSchema.create(HoodieSchemaType.STRING))));

    return HoodieSchema.createRecord("test", null, null,
        Arrays.asList(
            HoodieSchemaField.of("field_1", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("field_2", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("field_3", nestedSchema)
        ));
  }

  private ArrayWritable createBaseRecord(Writable[] values) {
    return new ArrayWritable(Writable.class, values);
  }

  @Test
  void getFileRecordIteratorFailsFastOnShreddedVariantColumn(@TempDir java.nio.file.Path tempDir) throws Exception {
    // The Hive reader hands base files to a plain parquet-avro read at the requested
    // {metadata, value} projection; a shredded file would come back with silent nulls (the
    // payload of typed rows lives in typed_value, which the projection drops). The footer is
    // already read for schema pruning, so the shredded shape must fail fast instead.
    HoodieSchema.Variant shreddedVariant = HoodieSchema.createVariantShreddedObject(
        Collections.singletonMap("key", HoodieSchema.create(HoodieSchemaType.STRING)));
    HoodieSchema writeSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("v", shreddedVariant)));
    StoragePath filePath = writeShreddedVariantFile(tempDir, writeSchema, shreddedVariant);

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("v", HoodieSchema.createVariant())));
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext readerContext =
        new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    readerContext.setNeedsBootstrapMerge(false);
    HoodieStorage storage = HoodieStorageUtils.getStorage(filePath, storageConfiguration);

    HoodieException failure = assertThrows(HoodieException.class, () ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
    assertTrue(failure.getMessage().contains("shredded variant") && failure.getMessage().contains("'v'"),
        "The error must name the shredded variant column, got: " + failure.getMessage());

    // Queries that do not project the variant column (e.g. count(*)) stay readable.
    HoodieSchema withoutVariant = HoodieSchema.createRecord("TestRecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT))));
    when(readerCreator.getRecordReader(any(), any(), any()))
        .thenReturn((RecordReader<NullWritable, ArrayWritable>) mock(RecordReader.class));
    assertDoesNotThrow(() ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, withoutVariant, storage));
  }

  @Test
  void getFileRecordIteratorAcceptsUnshreddedVariantColumn(@TempDir java.nio.file.Path tempDir) throws Exception {
    // The unshredded twin: a plain {metadata, value} variant group must keep reading through
    // the ordinary path; the guard is anchored on typed_value being present in the file.
    HoodieSchema.Variant unshredded = HoodieSchema.createVariant();
    HoodieSchema writeSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("v", unshredded)));
    java.nio.file.Path file = tempDir.resolve("unshredded.parquet");
    try (AvroParquetWriter<GenericRecord> writer =
             new AvroParquetWriter<>(new Path(file.toString()), writeSchema.toAvroSchema())) {
      GenericRecord variant = new GenericData.Record(unshredded.toAvroSchema());
      variant.put("metadata", ByteBuffer.wrap(new byte[] {1}));
      variant.put("value", ByteBuffer.wrap(new byte[] {0}));
      GenericRecord record = new GenericData.Record(writeSchema.toAvroSchema());
      record.put("id", 1);
      record.put("v", variant);
      writer.write(record);
    }
    StoragePath filePath = new StoragePath(file.toUri().toString());

    HoodieSchema tableSchema = HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of("v", HoodieSchema.createVariant())));
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext readerContext =
        new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    readerContext.setNeedsBootstrapMerge(false);
    HoodieStorage storage = HoodieStorageUtils.getStorage(filePath, storageConfiguration);
    when(readerCreator.getRecordReader(any(), any(), any()))
        .thenReturn((RecordReader<NullWritable, ArrayWritable>) mock(RecordReader.class));
    assertDoesNotThrow(() ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
  }

  private StoragePath writeShreddedVariantFile(java.nio.file.Path tempDir, HoodieSchema writeSchema,
                                               HoodieSchema.Variant shreddedVariant) throws Exception {
    java.nio.file.Path file = tempDir.resolve("shredded.parquet");
    try (AvroParquetWriter<GenericRecord> writer =
             new AvroParquetWriter<>(new Path(file.toString()), writeSchema.toAvroSchema())) {
      org.apache.avro.Schema typedValueSchema =
          shreddedVariant.getTypedValueField().get().toAvroSchema();
      GenericRecord keyWrapper = new GenericData.Record(
          org.apache.hudi.common.avro.HoodieAvroUtils.unwrapNullable(typedValueSchema.getField("key").schema()));
      keyWrapper.put("typed_value", "k1");
      GenericRecord typedValue = new GenericData.Record(typedValueSchema);
      typedValue.put("key", keyWrapper);
      GenericRecord variant = new GenericData.Record(shreddedVariant.toAvroSchema());
      variant.put("metadata", ByteBuffer.wrap(new byte[] {1}));
      variant.put("typed_value", typedValue);
      GenericRecord record = new GenericData.Record(writeSchema.toAvroSchema());
      record.put("id", 1);
      record.put("v", variant);
      writer.write(record);
    }
    return new StoragePath(file.toUri().toString());
  }
}
