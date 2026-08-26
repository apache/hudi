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
import org.apache.hudi.hadoop.testutils.InputFormatTestUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
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
    StoragePath filePath = InputFormatTestUtil.writeVariantParquetFile(tempDir, "shredded.parquet", true);
    HoodieSchema tableSchema = tableSchemaWithVariant();
    HiveHoodieReaderContext readerContext = newReaderContext();
    HoodieStorage storage = HoodieStorageUtils.getStorage(filePath, storageConfiguration);
    requestColumns("id", "v");

    HoodieException failure = assertThrows(HoodieException.class, () ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
    assertTrue(failure.getMessage().contains("shredded variant") && failure.getMessage().contains("'v'"),
        "The error must name the shredded variant column, got: " + failure.getMessage());

    // A query that does not project the variant column (`select id`) stays readable.
    HoodieSchema withoutVariant = HoodieSchema.createRecord("TestRecord", null, null, Collections.singletonList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT))));
    requestColumns("id");
    when(readerCreator.getRecordReader(any(), any(), any()))
        .thenReturn((RecordReader<NullWritable, ArrayWritable>) mock(RecordReader.class));
    assertDoesNotThrow(() ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, withoutVariant, storage));
  }

  @Test
  void getFileRecordIteratorFlagsOnlyColumnsHiveReads(@TempDir java.nio.file.Path tempDir) throws Exception {
    // The required schema can be wider than the query: a CUSTOM merge (no merger overrides
    // isProjectionCompatible) reads the whole table schema for merging, so `select id` reaches the
    // context asking for the variant column too. What Hive hands back to the query is its read
    // column names, so those decide: the full list `select *` carries fails, `select id` reads,
    // and count(*), which names no column, reads.
    StoragePath filePath = InputFormatTestUtil.writeVariantParquetFile(tempDir, "shredded.parquet", true);
    HoodieSchema tableSchema = tableSchemaWithVariant();
    HiveHoodieReaderContext readerContext = newReaderContext();
    HoodieStorage storage = HoodieStorageUtils.getStorage(filePath, storageConfiguration);
    when(readerCreator.getRecordReader(any(), any(), any()))
        .thenReturn((RecordReader<NullWritable, ArrayWritable>) mock(RecordReader.class));

    requestColumns("id", "v");
    HoodieException failure = assertThrows(HoodieException.class, () ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
    assertTrue(failure.getMessage().contains("'v'"),
        "select * must still fail on the shredded variant column, got: " + failure.getMessage());

    requestColumns("id");
    assertDoesNotThrow(() ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
    verify(readerCreator).getRecordReader(any(), any(), any());

    requestColumns();
    assertDoesNotThrow(() ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
  }

  @Test
  void getFileRecordIteratorAcceptsUnshreddedVariantColumn(@TempDir java.nio.file.Path tempDir) throws Exception {
    // The unshredded twin: a plain {metadata, value} variant group must keep reading through
    // the ordinary path; the guard is anchored on typed_value being present in the file.
    StoragePath filePath = InputFormatTestUtil.writeVariantParquetFile(tempDir, "unshredded.parquet", false);
    HoodieSchema tableSchema = tableSchemaWithVariant();
    HiveHoodieReaderContext readerContext = newReaderContext();
    HoodieStorage storage = HoodieStorageUtils.getStorage(filePath, storageConfiguration);
    requestColumns("id", "v");
    when(readerCreator.getRecordReader(any(), any(), any()))
        .thenReturn((RecordReader<NullWritable, ArrayWritable>) mock(RecordReader.class));
    assertDoesNotThrow(() ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
  }

  @ParameterizedTest
  @MethodSource("nestedShreddedVariantFixtures")
  void getFileRecordIteratorFailsFastOnNestedShreddedVariant(String columnName, HoodieSchema tableSchema,
                                                             VariantFixtureWriter fixture,
                                                             @TempDir java.nio.file.Path tempDir) throws Exception {
    // The row writer shreds nested variants too (HoodieRowParquetWriteSupport recurses into
    // structs, array elements and map values), so the guard must look below the top level on every
    // shape toShreddedReadSchema walks: a struct member, an array element and a map value. Each
    // fails naming the top-level column that holds the shredded group.
    StoragePath filePath = fixture.write(tempDir, "nested_shredded.parquet");
    HiveHoodieReaderContext readerContext = newReaderContext();
    HoodieStorage storage = HoodieStorageUtils.getStorage(filePath, storageConfiguration);
    requestColumns("id", columnName);

    HoodieException failure = assertThrows(HoodieException.class, () ->
        readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
    assertTrue(failure.getMessage().contains("shredded variant") && failure.getMessage().contains("'" + columnName + "'"),
        "The error must name the column holding the nested shredded variant, got: " + failure.getMessage());
  }

  @Test
  void getFileRecordIteratorHonoursNestedColumnPruning(@TempDir java.nio.file.Path tempDir) throws Exception {
    // Hive's read column names are top-level only; nested column pruning arrives separately, as
    // dotted paths, which the context has to consult: `select s.other` names s but materializes
    // nothing of s.inner, so that read has to reach the record reader while a path that does hit
    // the group still fails. Which paths reach a shredded group is pinned exhaustively by
    // TestHoodieColumnProjectionUtils.testColumnsReadingShreddedPaths.
    StoragePath filePath = InputFormatTestUtil.writeNestedShreddedVariantParquetFile(tempDir, "nested_shredded.parquet");
    HoodieSchema tableSchema = nestedVariantTableSchema();
    HiveHoodieReaderContext readerContext = newReaderContext();
    HoodieStorage storage = HoodieStorageUtils.getStorage(filePath, storageConfiguration);
    requestColumns("id", "s");
    Configuration conf = storageConfiguration.unwrapAs(Configuration.class);
    when(readerCreator.getRecordReader(any(), any(), any()))
        .thenReturn((RecordReader<NullWritable, ArrayWritable>) mock(RecordReader.class));
    try {
      conf.set(HoodieColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR, "s.other");
      assertDoesNotThrow(() ->
          readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
      verify(readerCreator).getRecordReader(any(), any(), any());

      conf.set(HoodieColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR, "s.inner");
      HoodieException innerFailure = assertThrows(HoodieException.class, () ->
          readerContext.getFileRecordIterator(filePath, 0, Long.MAX_VALUE, tableSchema, tableSchema, storage));
      assertTrue(innerFailure.getMessage().contains("'s'"),
          "A nested path reaching the shredded group must still fail, got: " + innerFailure.getMessage());
    } finally {
      conf.unset(HoodieColumnProjectionUtils.READ_NESTED_COLUMN_PATH_CONF_STR);
    }
  }

  private static Stream<Arguments> nestedShreddedVariantFixtures() {
    return Stream.of(
        arguments("s", nestedVariantTableSchema(),
            (VariantFixtureWriter) InputFormatTestUtil::writeNestedShreddedVariantParquetFile),
        arguments("a", tableSchemaWith("a", HoodieSchema.createArray(HoodieSchema.createVariant())),
            (VariantFixtureWriter) InputFormatTestUtil::writeArrayShreddedVariantParquetFile),
        arguments("m", tableSchemaWith("m", HoodieSchema.createMap(HoodieSchema.createVariant())),
            (VariantFixtureWriter) InputFormatTestUtil::writeMapShreddedVariantParquetFile));
  }

  /** The shape {@link InputFormatTestUtil}'s shredded-variant fixture writers share. */
  @FunctionalInterface
  interface VariantFixtureWriter {
    StoragePath write(java.nio.file.Path dir, String fileName) throws IOException;
  }

  private static HoodieSchema tableSchemaWithVariant() {
    return tableSchemaWith("v", HoodieSchema.createVariant());
  }

  private static HoodieSchema nestedVariantTableSchema() {
    return tableSchemaWith("s", HoodieSchema.createRecord("s_t", null, null,
        Collections.singletonList(HoodieSchemaField.of("inner", HoodieSchema.createVariant()))));
  }

  /** An {@code id} column plus {@code columnName}, matching what the fixture writers put on disk. */
  private static HoodieSchema tableSchemaWith(String columnName, HoodieSchema columnSchema) {
    return HoodieSchema.createRecord("TestRecord", null, null, Arrays.asList(
        HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.INT)),
        HoodieSchemaField.of(columnName, columnSchema)));
  }

  /** Hive's read column names for the query, as HiveInputFormat.pushProjection sets them (none for count(*)). */
  private void requestColumns(String... columns) {
    storageConfiguration.unwrapAs(Configuration.class)
        .set(HoodieColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, String.join(",", columns));
  }

  private HiveHoodieReaderContext newReaderContext() {
    when(tableConfig.populateMetaFields()).thenReturn(true);
    HiveHoodieReaderContext readerContext =
        new HiveHoodieReaderContext(readerCreator, Collections.emptyList(), storageConfiguration, tableConfig);
    readerContext.setNeedsBootstrapMerge(false);
    return readerContext;
  }
}
