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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.NativeTableFormat;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.core.io.storage.HoodieFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Tests the Parquet list layout {@link HoodieAvroFileWriterFactory} writes: the standard three-level layout for a
 * table whose format is not native unless a layout is set explicitly, and the parquet-avro default otherwise.
 */
class TestHoodieAvroParquetListLayout {

  private static final String NON_NATIVE_FORMAT = "test-format";
  private static final Schema SCHEMA = SchemaBuilder.record("rec").fields()
      .requiredString("_row_key")
      .name("values").type().array().items().nullable().intType().noDefault()
      .endRecord();

  @TempDir
  java.nio.file.Path tmpDir;

  private static Stream<Arguments> listLayouts() {
    Map<String, String> none = Collections.emptyMap();
    return Stream.of(
        // table format, storage configuration, write config, whether the layout is three-level
        Arguments.of(null, none, none, false),
        Arguments.of(NativeTableFormat.TABLE_FORMAT, none, none, false),
        Arguments.of(NativeTableFormat.TABLE_FORMAT, Collections.singletonMap(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "false"), none, true),
        Arguments.of(NON_NATIVE_FORMAT, none, none, true),
        Arguments.of(NON_NATIVE_FORMAT, Collections.singletonMap(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "true"), none, false),
        Arguments.of(NON_NATIVE_FORMAT, none, Collections.singletonMap(AvroWriteSupport.WRITE_OLD_LIST_STRUCTURE, "true"), false),
        Arguments.of(NON_NATIVE_FORMAT, none, Collections.singletonMap(HoodieStorageConfig.PARQUET_WRITE_LEGACY_FORMAT_ENABLED.key(), "true"), false));
  }

  @ParameterizedTest
  @MethodSource("listLayouts")
  void testListLayout(String tableFormat, Map<String, String> storageConf, Map<String, String> writeConfig,
                      boolean threeLevel) throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    storageConf.forEach(storage.getConf()::set);
    HoodieConfig config = HoodieStorageConfig.newBuilder().build();
    config.setValue(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME, "gzip");
    if (tableFormat != null) {
      config.setValue(HoodieTableConfig.TABLE_FORMAT, tableFormat);
    }
    writeConfig.forEach(config::setValue);
    StoragePath path = new StoragePath(tmpDir.resolve("list_layout.parquet").toString());
    // The legacy two-level layout cannot hold a null element
    List<List<Integer>> values = threeLevel
        ? Arrays.asList(Arrays.asList(1, null, 3), Collections.<Integer>singletonList(null), Collections.<Integer>emptyList())
        : Arrays.asList(Arrays.asList(1, 2, 3), Collections.<Integer>emptyList());

    HoodieFileWriter writer = new HoodieAvroFileWriterFactory(storage)
        .newParquetFileWriter("100", path, config, HoodieSchema.fromAvroSchema(SCHEMA), new LocalTaskContextSupplier());
    for (int i = 0; i < values.size(); i++) {
      ((HoodieAvroParquetWriter) writer).writeAvro(String.valueOf(i), record(i, values.get(i)));
    }
    writer.close();

    assertEquals(threeLevel ? "list" : "array", repeatedListFieldName(path));
    assertEquals(values, readValues(storage, path));
  }

  @Test
  void testListLayoutOfStreamWriter() throws Exception {
    HoodieStorage storage = HoodieTestUtils.getStorage(tmpDir.toString());
    HoodieConfig config = HoodieStorageConfig.newBuilder().build();
    config.setValue(HoodieTableConfig.TABLE_FORMAT, NON_NATIVE_FORMAT);
    StoragePath path = new StoragePath(tmpDir.resolve("list_layout_stream.parquet").toString());
    List<Integer> values = Arrays.asList(1, null, 3);

    try (OutputStream outputStream = new FileOutputStream(path.toString());
         HoodieParquetStreamWriter writer = (HoodieParquetStreamWriter) new HoodieAvroFileWriterFactory(storage)
             .newParquetFileWriter(outputStream, config, HoodieSchema.fromAvroSchema(SCHEMA))) {
      writer.writeAvro("0", record(0, values));
    }

    assertEquals("list", repeatedListFieldName(path));
    assertEquals(Collections.singletonList(values), readValues(storage, path));
  }

  private static GenericRecord record(int key, List<Integer> values) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("_row_key", String.valueOf(key));
    record.put("values", values);
    return record;
  }

  /**
   * Returns the name of the repeated field of the list column: "list" in the three-level layout, "array" in
   * the legacy two-level layout parquet-avro writes.
   */
  private static String repeatedListFieldName(StoragePath path) throws IOException {
    try (ParquetFileReader reader = ParquetFileReader.open(
        HadoopInputFile.fromPath(new Path(path.toUri()), new Configuration()))) {
      GroupType list = reader.getFooter().getFileMetaData().getSchema().getType("values").asGroupType();
      assertEquals(1, list.getFieldCount());
      return list.getType(0).getName();
    }
  }

  private static List<List<Integer>> readValues(HoodieStorage storage, StoragePath path) throws IOException {
    List<List<Integer>> values = new ArrayList<>();
    try (HoodieAvroParquetReader reader = new HoodieAvroParquetReader(storage, path);
         ClosableIterator<IndexedRecord> iterator = reader.getIndexedRecordIterator(HoodieSchema.fromAvroSchema(SCHEMA))) {
      while (iterator.hasNext()) {
        values.add(new ArrayList<>((List<Integer>) iterator.next().get(1)));
      }
    }
    assertFalse(values.isEmpty());
    return values;
  }
}
