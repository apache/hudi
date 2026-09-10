/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader.function;

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.cdc.HoodieCDCFileSplit;
import org.apache.hudi.common.table.cdc.HoodieCDCInferenceCase;
import org.apache.hudi.common.table.cdc.HoodieCDCSupplementalLoggingMode;
import org.apache.hudi.common.table.cdc.HoodieCDCUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.io.storage.row.HoodieRowDataFileWriter;
import org.apache.hudi.io.storage.row.HoodieRowDataFileWriterFactory;
import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.split.HoodieCdcSourceSplit;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.cdc.CdcInputFormat;
import org.apache.hudi.table.format.cdc.CdcInputSplit;
import org.apache.hudi.table.format.mor.MergeOnReadTableState;
import org.apache.hudi.util.DataTypeUtils;
import org.apache.hudi.util.FlinkWriteClients;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/** Real base-file and CDC-log coverage for both Flink CDC source implementations. */
class TestCdcVectorRead {
  private static final String INSTANT = "20260910000000000";
  private static final DataType DATA_TYPE = DataTypes.ROW(
      DataTypes.FIELD("id", DataTypes.STRING().notNull()),
      DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull())),
      DataTypes.FIELD("features", DataTypes.ARRAY(DataTypes.DOUBLE().notNull()).notNull()),
      DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT()))).notNull();

  @TempDir
  Path tempDir;

  static Stream<Arguments> readCases() {
    List<Arguments> cases = new ArrayList<>();
    for (HoodieFileFormat format : Arrays.asList(HoodieFileFormat.PARQUET, HoodieFileFormat.LANCE)) {
      for (boolean sourceV2 : new boolean[] {false, true}) {
        for (HoodieTableType tableType : HoodieTableType.values()) {
          for (HoodieCDCInferenceCase inference : Arrays.asList(
              HoodieCDCInferenceCase.BASE_FILE_INSERT, HoodieCDCInferenceCase.BASE_FILE_DELETE,
              HoodieCDCInferenceCase.REPLACE_COMMIT)) {
            cases.add(Arguments.of(format, sourceV2, tableType, inference));
          }
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @MethodSource("readCases")
  void testReadVectors(HoodieFileFormat format, boolean sourceV2, HoodieTableType tableType,
                       HoodieCDCInferenceCase inference) throws Exception {
    readVectors(format, sourceV2, tableType, inference, 2);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testLanceVectorDimensionMismatch(boolean sourceV2) {
    Exception exception = assertThrows(Exception.class, () -> readVectors(
        HoodieFileFormat.LANCE, sourceV2, HoodieTableType.COPY_ON_WRITE,
        HoodieCDCInferenceCase.BASE_FILE_INSERT, 3));
    Throwable cause = exception;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    assertTrue(cause.getMessage().contains("requested VECTOR(3)"), cause.getMessage());
  }

  private void readVectors(HoodieFileFormat format, boolean sourceV2, HoodieTableType tableType,
                           HoodieCDCInferenceCase inference, int requestedDimension) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.toString());
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.CDC_ENABLED, true);
    conf.set(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE, HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY.name());
    conf.setString(HoodieTableConfig.BASE_FILE_FORMAT.key(), format.name());
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(FlinkOptions.ORDERING_FIELDS, FlinkOptions.NO_PRE_COMBINE);
    conf.set(FlinkOptions.VECTOR_COLUMNS, "embedding:2,features:3");
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, HoodieSchemaConverter.convertToSchema(
        DATA_TYPE.getLogicalType(), "vector_record", conf.get(FlinkOptions.VECTOR_COLUMNS)).toString());
    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(conf.get(FlinkOptions.SOURCE_AVRO_SCHEMA)));
    RowType rowType = HoodieSchemaConverter.convertToRowType(schema);
    RowType projected = (RowType) DataTypes.ROW(
        DataTypes.FIELD("values", DataTypes.ARRAY(DataTypes.INT())),
        DataTypes.FIELD("features", DataTypes.ARRAY(DataTypes.DOUBLE().notNull()).notNull()),
        DataTypes.FIELD("id", DataTypes.STRING().notNull()),
        DataTypes.FIELD("embedding", DataTypes.ARRAY(DataTypes.FLOAT().notNull()))).notNull().getLogicalType();
    HoodieSchema requestedTableSchema = HoodieSchemaUtils.addMetadataFields(HoodieSchemaConverter.convertToSchema(
        DATA_TYPE.getLogicalType(), "vector_record", "embedding:" + requestedDimension + ",features:3"));
    MergeOnReadTableState tableState = new MergeOnReadTableState(rowType, projected,
        requestedTableSchema.toString(), DataTypeUtils.toHoodieSchema(projected, requestedTableSchema).toString(), Collections.emptyList());
    StoragePath path = new StoragePath(tempDir.resolve("file-1_0-0-0_" + INSTANT + format.getFileExtension()).toUri());
    HoodieRowDataFileWriterFactory factory = new HoodieRowDataFileWriterFactory(metaClient.getStorage());
    try (HoodieRowDataFileWriter writer = (HoodieRowDataFileWriter) (format == HoodieFileFormat.PARQUET
        ? factory.newParquetFileWriter(INSTANT, path, FlinkWriteClients.getHoodieClientConfig(conf), schema, mock(TaskContextSupplier.class))
        : factory.newLanceFileWriter(INSTANT, path, FlinkWriteClients.getHoodieClientConfig(conf), schema, mock(TaskContextSupplier.class)))) {
      for (int i = 0; i < 2; i++) {
        StringData id = StringData.fromString("id" + i);
        writer.writeRow(id.toString(), GenericRowData.of(
            StringData.fromString(INSTANT), StringData.fromString("seq" + i), id,
            StringData.fromString(""), StringData.fromString(path.getName()), id,
            i == 0 ? new GenericArrayData(new float[] {1.25F, 2.5F}) : null,
            new GenericArrayData(new double[] {3.5D, 4.5D, 5.5D}),
            new GenericArrayData(new Integer[] {10 + i, null, 20 + i})));
      }
    }
    FileSlice slice = new FileSlice("", INSTANT, "file-1");
    slice.setBaseFile(new HoodieBaseFile(path.toString()));
    HoodieCDCFileSplit change = new HoodieCDCFileSplit(INSTANT, inference,
        Collections.singletonList(path.getName()), Option.of(slice), Option.empty());
    HoodieCDCFileSplit[] changes = {change};
    RowKind kind = inference == HoodieCDCInferenceCase.BASE_FILE_INSERT ? RowKind.INSERT : RowKind.DELETE;
    List<DataType> fieldTypes = DataTypes.of(rowType).getChildren();
    if (sourceV2) {
      HoodieCdcSplitReaderFunction function = new HoodieCdcSplitReaderFunction(
          conf, tableState, InternalSchemaManager.DISABLED, fieldTypes, Collections.emptyList(), false);
      HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(
          0, tempDir.toString(), 1024 * 1024, "file-1", "", changes,
          FlinkOptions.REALTIME_PAYLOAD_COMBINE, INSTANT);
      try {
        function.open(split);
        BatchRecords<RowData> batch = function.readBatch(split, 10, () -> false);
        assertNotNull(batch);
        assertRow(batch.nextRecordFromSplit().record(), 0, kind);
        assertRow(batch.nextRecordFromSplit().record(), 1, kind);
        assertNull(batch.nextRecordFromSplit());
        assertNull(function.readBatch(split, 10, () -> false));
      } finally {
        function.close();
      }
    } else {
      CdcInputFormat inputFormat = CdcInputFormat.builder().config(conf).tableState(tableState)
          .fieldTypes(fieldTypes).predicates(Collections.emptyList()).limit(-1).emitDelete(false).build();
      try {
        inputFormat.open(new CdcInputSplit(0, tempDir.toString(), 1024 * 1024, "file-1", "", changes));
        int count = 0;
        while (!inputFormat.reachedEnd()) {
          assertRow(inputFormat.nextRecord(null), count++, kind);
        }
        assertEquals(2, count);
      } finally {
        inputFormat.close();
      }
    }
  }

  static Stream<Arguments> asIsCases() {
    List<Arguments> cases = new ArrayList<>();
    for (HoodieFileFormat format : Arrays.asList(HoodieFileFormat.PARQUET, HoodieFileFormat.LANCE)) {
      for (boolean sourceV2 : new boolean[] {false, true}) {
        for (HoodieTableType tableType : HoodieTableType.values()) {
          cases.add(Arguments.of(format, sourceV2, tableType));
        }
      }
    }
    return cases.stream();
  }

  @ParameterizedTest(name = "{0}, sourceV2={1}, {2}")
  @MethodSource("asIsCases")
  void testAsIsOpKeyOnlyVectors(HoodieFileFormat format, boolean sourceV2, HoodieTableType tableType) throws Exception {
    HoodieCDCSupplementalLoggingMode mode = HoodieCDCSupplementalLoggingMode.OP_KEY_ONLY;
    String afterInstant = "20260910000001000";
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.toString());
    conf.set(FlinkOptions.WRITE_TABLE_VERSION, HoodieTableVersion.TEN.versionCode());
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.CDC_ENABLED, true);
    conf.set(FlinkOptions.SUPPLEMENTAL_LOGGING_MODE, mode.name());
    conf.setString(HoodieTableConfig.BASE_FILE_FORMAT.key(), format.name());
    conf.set(FlinkOptions.PARTITION_PATH_FIELD, "");
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "id");
    conf.set(FlinkOptions.ORDERING_FIELDS, FlinkOptions.NO_PRE_COMBINE);
    conf.set(FlinkOptions.VECTOR_COLUMNS, "embedding:2,features:3");
    HoodieSchema dataSchema = HoodieSchemaConverter.convertToSchema(
        DATA_TYPE.getLogicalType(), "vector_record", conf.get(FlinkOptions.VECTOR_COLUMNS));
    conf.set(FlinkOptions.SOURCE_AVRO_SCHEMA, dataSchema.toString());
    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    HoodieSchema tableSchema = HoodieSchemaUtils.addMetadataFields(dataSchema);
    RowType rowType = HoodieSchemaConverter.convertToRowType(tableSchema);
    // Reorder the data fields and omit all metadata fields from the CDC output.
    RowType projected = new RowType(false, Arrays.asList(rowType.getFields().get(8),
        rowType.getFields().get(7), rowType.getFields().get(5), rowType.getFields().get(6)));
    MergeOnReadTableState tableState = new MergeOnReadTableState(rowType, projected,
        tableSchema.toString(), DataTypeUtils.toHoodieSchema(projected, tableSchema).toString(), Collections.emptyList());
    GenericRowData old1 = image("id1", 1, false);
    GenericRowData old2 = image("id2", 2, true);
    GenericRowData deleted = image("id3", 3, false);
    GenericRowData new1 = image("id1", 4, true);
    GenericRowData new2 = image("id2", 5, false);
    GenericRowData inserted1 = image("id4", 6, false);
    GenericRowData inserted2 = image("id5", 7, true);
    FileSlice before = writeImageSlice(conf, metaClient, tableSchema, format, INSTANT, old1, old2, deleted);
    FileSlice after = writeImageSlice(conf, metaClient, tableSchema, format, afterInstant, new1, new2, inserted1, inserted2);
    HoodieSchema cdcSchema = HoodieCDCUtils.schemaBySupplementalLoggingMode(mode, dataSchema);
    List<RowData> cdcRows = Arrays.asList(
        GenericRowData.of(StringData.fromString("u"), StringData.fromString("id1")),
        GenericRowData.of(StringData.fromString("u"), StringData.fromString("id2")),
        GenericRowData.of(StringData.fromString("d"), StringData.fromString("id3")),
        GenericRowData.of(StringData.fromString("i"), StringData.fromString("id4")),
        GenericRowData.of(StringData.fromString("i"), StringData.fromString("id5")));
    String cdcFile = writeCdcFile(conf, metaClient, cdcSchema, format, afterInstant, cdcRows);
    HoodieCDCFileSplit change = new HoodieCDCFileSplit(afterInstant, HoodieCDCInferenceCase.AS_IS,
        cdcFile, Option.of(before), Option.of(after));
    List<RowData> expected = Arrays.asList(
        projectedImage(old1, RowKind.UPDATE_BEFORE), projectedImage(new1, RowKind.UPDATE_AFTER),
        projectedImage(old2, RowKind.UPDATE_BEFORE), projectedImage(new2, RowKind.UPDATE_AFTER),
        projectedImage(deleted, RowKind.DELETE), projectedImage(inserted1, RowKind.INSERT),
        projectedImage(inserted2, RowKind.INSERT));
    RowDataSerializer serializer = new RowDataSerializer(projected);
    List<RowData> actual = new ArrayList<>();
    if (sourceV2) {
      HoodieCdcSplitReaderFunction function = new HoodieCdcSplitReaderFunction(conf, tableState,
          InternalSchemaManager.DISABLED, DataTypes.of(rowType).getChildren(), Collections.emptyList(), false);
      HoodieCdcSourceSplit split = new HoodieCdcSourceSplit(0, tempDir.toString(), 1024 * 1024,
          "file-1", "", new HoodieCDCFileSplit[] {change}, FlinkOptions.REALTIME_PAYLOAD_COMBINE, afterInstant);
      try {
        function.open(split);
        BatchRecords<RowData> batch;
        // A one-row batch also checks that UPDATE_AFTER survives across batch boundaries.
        while ((batch = function.readBatch(split, 1, () -> false)) != null) {
          actual.add(batch.nextRecordFromSplit().record());
          assertNull(batch.nextRecordFromSplit());
        }
      } finally {
        function.close();
      }
    } else {
      CdcInputFormat inputFormat = CdcInputFormat.builder().config(conf).tableState(tableState)
          .fieldTypes(DataTypes.of(rowType).getChildren()).predicates(Collections.emptyList())
          .limit(-1).emitDelete(false).build();
      try {
        inputFormat.open(new CdcInputSplit(0, tempDir.toString(), 1024 * 1024,
            "file-1", "", new HoodieCDCFileSplit[] {change}));
        while (!inputFormat.reachedEnd()) {
          actual.add(serializer.copy(inputFormat.nextRecord(null)));
        }
      } finally {
        inputFormat.close();
      }
    }
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      assertEquals(expected.get(i).getRowKind(), actual.get(i).getRowKind(), "RowKind at " + i);
      assertEquals(serializer.toBinaryRow(expected.get(i)).copy(), serializer.toBinaryRow(actual.get(i)).copy(), "Image at " + i);
    }
  }

  private FileSlice writeImageSlice(Configuration conf, HoodieTableMetaClient metaClient, HoodieSchema schema,
                                    HoodieFileFormat format, String instant, GenericRowData... images) throws Exception {
    StoragePath path = new StoragePath(tempDir.resolve("file-1_0-0-0_" + instant + format.getFileExtension()).toUri());
    try (HoodieRowDataFileWriter writer = createWriter(conf, metaClient, schema, format, instant, path)) {
      for (GenericRowData image : images) {
        StringData id = image.getString(0);
        writer.writeRow(id.toString(), GenericRowData.of(StringData.fromString(instant), StringData.fromString("seq"),
            id, StringData.fromString(""), StringData.fromString(path.getName()),
            id, image.getField(1), image.getField(2), image.getField(3)));
      }
    }
    FileSlice slice = new FileSlice("", instant, "file-1");
    slice.setBaseFile(new HoodieBaseFile(path.toString()));
    return slice;
  }

  private String writeCdcFile(Configuration conf, HoodieTableMetaClient metaClient, HoodieSchema schema,
                              HoodieFileFormat format, String instant, List<RowData> rows) throws Exception {
    StoragePath path = new StoragePath(tempDir.toString(), FSUtils.makeNativeLogFileName(
        "file-1", "0-0-0", instant, 1, HoodieCDCUtils.CDC_LOGFILE_SUFFIX, format));
    try (HoodieRowDataFileWriter writer = createWriter(conf, metaClient, schema, format, instant, path)) {
      for (RowData row : rows) {
        writer.writeRow("key", row);
      }
    }
    return path.getName();
  }

  private static HoodieRowDataFileWriter createWriter(Configuration conf, HoodieTableMetaClient metaClient,
                                                       HoodieSchema schema, HoodieFileFormat format,
                                                       String instant, StoragePath path) throws Exception {
    HoodieRowDataFileWriterFactory factory = new HoodieRowDataFileWriterFactory(metaClient.getStorage());
    return (HoodieRowDataFileWriter) (format == HoodieFileFormat.PARQUET
        ? factory.newParquetFileWriter(instant, path, FlinkWriteClients.getHoodieClientConfig(conf), schema, mock(TaskContextSupplier.class))
        : factory.newLanceFileWriter(instant, path, FlinkWriteClients.getHoodieClientConfig(conf), schema, mock(TaskContextSupplier.class)));
  }

  private static GenericRowData image(String id, int value, boolean nullVector) {
    return GenericRowData.of(StringData.fromString(id),
        nullVector ? null : new GenericArrayData(new float[] {value + 0.25F, value + 0.5F}),
        new GenericArrayData(new double[] {value + 0.5D, value + 1.5D, value + 2.5D}),
        new GenericArrayData(new Integer[] {value, null, value + 10}));
  }

  private static RowData projectedImage(GenericRowData image, RowKind kind) {
    GenericRowData projected = GenericRowData.of(image.getField(3), image.getField(2), image.getField(0), image.getField(1));
    projected.setRowKind(kind);
    return projected;
  }

  private static void assertRow(RowData row, int index, RowKind kind) {
    assertEquals(kind, row.getRowKind());
    assertEquals(4, row.getArity());
    assertEquals("id" + index, row.getString(2).toString());
    assertEquals(3, row.getArray(0).size());
    assertEquals(10 + index, row.getArray(0).getInt(0));
    assertTrue(row.getArray(0).isNullAt(1));
    assertEquals(20 + index, row.getArray(0).getInt(2));
    assertEquals(3, row.getArray(1).size());
    assertEquals(3.5D, row.getArray(1).getDouble(0));
    assertEquals(4.5D, row.getArray(1).getDouble(1));
    assertEquals(5.5D, row.getArray(1).getDouble(2));
    if (index == 0) {
      assertEquals(2, row.getArray(3).size());
      assertEquals(1.25F, row.getArray(3).getFloat(0));
      assertEquals(2.5F, row.getArray(3).getFloat(1));
    } else {
      assertTrue(row.isNullAt(3));
    }
  }
}
