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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.testutils.FileCreateUtilsLegacy;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.storage.HoodieFileWriter;
import org.apache.hudi.io.storage.HoodieFileWriterFactory;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.Lazy;

import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;
import static org.apache.hudi.avro.TestHoodieAvroUtils.SCHEMA_WITH_AVRO_TYPES_STR;
import static org.apache.hudi.avro.TestHoodieAvroUtils.SCHEMA_WITH_NESTED_FIELD_STR;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.metadata.HoodieIndexVersion.V1;
import static org.apache.hudi.metadata.HoodieIndexVersion.V2;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.computeRevivedAndDeletedKeys;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getFileIDForFileGroup;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestHoodieTableMetadataUtil extends HoodieCommonTestHarness {

  private static HoodieTestTable hoodieTestTable;
  private static final List<String> DATE_PARTITIONS = Arrays.asList("2019/01/01", "2020/01/02", "2021/03/01");

  @BeforeEach
  public void setUp() throws IOException {
    initMetaClient();
    initTestDataGenerator(DATE_PARTITIONS.toArray(new String[0]));
    hoodieTestTable = HoodieTestTable.of(metaClient);
  }

  @AfterEach
  public void tearDown() throws IOException {
    metaClient.getStorage().deleteDirectory(metaClient.getBasePath());
    cleanupTestDataGenerator();
    cleanMetaClient();
  }

  @Test
  public void testReadRecordKeysFromBaseFilesWithEmptyPartitionBaseFilePairs() {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    List<Pair<String, FileSlice>> partitionFileSlicePairs = Collections.emptyList();
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.readRecordKeysFromFileSlices(
        engineContext,
        partitionFileSlicePairs,
        1,
        "activeModule",
        metaClient,
        false
    );
    assertTrue(result.isEmpty());
  }

  @Test
  public void testConvertFilesToPartitionStatsRecords() throws Exception {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    String instant1 = "20230918120000000";
    hoodieTestTable = hoodieTestTable.addCommit(instant1);
    String instant2 = "20230918121110000";
    hoodieTestTable = hoodieTestTable.addCommit(instant2);
    List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
    // Generate 10 inserts for each partition and populate partitionBaseFilePairs and recordKeys.
    DATE_PARTITIONS.forEach(p -> {
      try {
        URI partitionMetaFile = FileCreateUtilsLegacy.createPartitionMetaFile(basePath, p);
        StoragePath partitionMetadataPath = new StoragePath(partitionMetaFile);
        String fileId1 = UUID.randomUUID().toString();
        FileSlice fileSlice1 = new FileSlice(p, instant1, fileId1);
        StoragePath storagePath1 = new StoragePath(hoodieTestTable.getBaseFilePath(p, fileId1).toUri());
        writeParquetFile(
            instant1,
            storagePath1,
            dataGen.generateInsertsForPartition(instant1, 10, p),
            metaClient,
            engineContext);
        HoodieBaseFile baseFile1 = new HoodieBaseFile(hoodieTestTable.getBaseFilePath(p, fileId1).toString());
        fileSlice1.setBaseFile(baseFile1);
        String fileId2 = UUID.randomUUID().toString();
        FileSlice fileSlice2 = new FileSlice(p, instant2, fileId2);
        StoragePath storagePath2 = new StoragePath(hoodieTestTable.getBaseFilePath(p, fileId2).toUri());
        writeParquetFile(
            instant2,
            storagePath2,
            dataGen.generateInsertsForPartition(instant2, 10, p),
            metaClient,
            engineContext);
        HoodieBaseFile baseFile2 = new HoodieBaseFile(hoodieTestTable.getBaseFilePath(p, fileId2).toString());
        fileSlice2.setBaseFile(baseFile2);
        partitionFileSlicePairs.add(Pair.of(p, fileSlice1));
        partitionFileSlicePairs.add(Pair.of(p, fileSlice2));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.convertFilesToPartitionStatsRecords(
        engineContext,
        partitionFileSlicePairs,
        HoodieMetadataConfig.newBuilder().enable(true)
            .withMetadataIndexColumnStats(true)
            .withMetadataIndexPartitionStats(true)
            .withColumnStatsIndexForColumns("rider,driver")
            .withPartitionStatsIndexParallelism(1)
            .build(),
        metaClient,
        Lazy.eagerly(Option.of(HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS)),
        Option.empty());
    // Validate the result.
    validatePartitionStats(result, instant1, instant2);
  }

  @Test
  public void testReadRecordKeysFromBaseFilesWithValidRecords() throws Exception {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    String instant = "20230918120000000";
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, TRIP_EXAMPLE_SCHEMA);
    hoodieTestTable = hoodieTestTable.addCommit(instant, Option.of(commitMetadata));
    Set<String> recordKeys = new HashSet<>();
    final List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
    // Generate 10 inserts for each partition and populate partitionBaseFilePairs and recordKeys.
    DATE_PARTITIONS.forEach(p -> {
      try {
        List<HoodieRecord> hoodieRecords = dataGen.generateInsertsForPartition(instant, 10, p);
        String fileId = UUID.randomUUID().toString();
        FileSlice fileSlice = new FileSlice(p, instant, fileId);
        writeParquetFile(
            instant,
            new StoragePath(hoodieTestTable.getBaseFilePath(p, fileId).toUri()),
            hoodieRecords,
            metaClient,
            engineContext);
        HoodieBaseFile baseFile = new HoodieBaseFile(hoodieTestTable.getBaseFilePath(p, fileId).toString(), fileId, instant, null);
        fileSlice.setBaseFile(baseFile);
        partitionFileSlicePairs.add(Pair.of(p, fileSlice));
        recordKeys.addAll(hoodieRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Call the method readRecordKeysFromBaseFiles with the created partitionBaseFilePairs.
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.readRecordKeysFromFileSlices(
        engineContext,
        partitionFileSlicePairs,
        1,
        "activeModule",
        metaClient,
        false
    );
    // Validate the result.
    List<HoodieRecord> records = result.collectAsList();
    assertEquals(30, records.size());
    assertEquals(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), records.get(0).getPartitionPath());
    for (HoodieRecord record : records) {
      assertTrue(recordKeys.contains(record.getRecordKey()));
    }
  }

  @Test
  public void testGetLogFileColumnRangeMetadata() throws Exception {
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    String instant1 = "20230918120000000";

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS.toString());
    hoodieTestTable = hoodieTestTable.addCommit(instant1, Option.of(commitMetadata));
    String instant2 = "20230918121110000";
    hoodieTestTable = hoodieTestTable.addCommit(instant2);
    List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
    List<String> columnsToIndex = Arrays.asList("rider", "driver");
    // Generate 10 inserts for each partition and populate partitionBaseFilePairs and recordKeys.
    DATE_PARTITIONS.forEach(p -> {
      try {
        URI partitionMetaFile = FileCreateUtilsLegacy.createPartitionMetaFile(basePath, p);
        StoragePath partitionMetadataPath = new StoragePath(partitionMetaFile);
        String fileId1 = UUID.randomUUID().toString();
        // add only one parquet file in first file slice
        FileSlice fileSlice1 = new FileSlice(p, instant1, fileId1);
        StoragePath storagePath1 = new StoragePath(hoodieTestTable.getBaseFilePath(p, fileId1).toUri());
        writeParquetFile(instant1, storagePath1, dataGen.generateInsertsForPartition(instant1, 10, p), metaClient, engineContext);
        HoodieBaseFile baseFile1 = new HoodieBaseFile(hoodieTestTable.getBaseFilePath(p, fileId1).toString());
        fileSlice1.setBaseFile(baseFile1);
        // add log file in second file slice with higher rider and driver values (which are concatenated with instant)
        FileSlice fileSlice2 = new FileSlice(p, instant2, fileId1);
        fileSlice2.setBaseFile(baseFile1);
        StoragePath storagePath2 = new StoragePath(partitionMetadataPath.getParent(), hoodieTestTable.getLogFileNameById(fileId1, 1));
        writeLogFiles(new StoragePath(metaClient.getBasePath(), p), HoodieTestDataGenerator.AVRO_SCHEMA, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS,
            dataGen.generateInsertsForPartition(instant2, 10, p), 1, metaClient.getStorage(), new Properties(), fileId1, instant2);
        fileSlice2.addLogFile(new HoodieLogFile(storagePath2.toUri().toString()));
        partitionFileSlicePairs.add(Pair.of(p, fileSlice1));
        partitionFileSlicePairs.add(Pair.of(p, fileSlice2));
        // NOTE: we need to set table config as we are not using write client explicitly and these configs are needed for log record reader
        metaClient.getTableConfig().setValue(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
        metaClient.getTableConfig().setValue(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
        metaClient.getTableConfig().setValue(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
        List<HoodieColumnRangeMetadata<Comparable>> columnRangeMetadataLogFile = HoodieTableMetadataUtil.getLogFileColumnRangeMetadata(
            storagePath2.toString(),
            p,
            metaClient,
            columnsToIndex,
            Option.of(HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS),
            HoodieMetadataConfig.MAX_READER_BUFFER_SIZE_PROP.defaultValue());
        // there must be two ranges for rider and driver
        assertEquals(2, columnRangeMetadataLogFile.size());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    // collect partition stats, this will collect stats for log files as well
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.convertFilesToPartitionStatsRecords(
        engineContext,
        partitionFileSlicePairs,
        HoodieMetadataConfig.newBuilder().enable(true)
            .withMetadataIndexColumnStats(true)
            .withMetadataIndexPartitionStats(true)
            .withColumnStatsIndexForColumns("rider,driver")
            .withPartitionStatsIndexParallelism(1)
            .build(),
        metaClient,
        Lazy.eagerly(Option.of(HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS)),
        Option.empty());
    // Validate the result.
    validatePartitionStats(result, instant1, instant2, 6);
  }

  private static void validatePartitionStats(HoodieData<HoodieRecord> result, String instant1, String instant2) {
    validatePartitionStats(result, instant1, instant2, 15);
  }

  private static void validatePartitionStats(HoodieData<HoodieRecord> result, String instant1, String instant2, int expectedTotalRecords) {
    List<HoodieRecord> records = result.collectAsList();
    // 3 partitions * (2 + 3) columns = 15 partition stats records. 3 meta fields are indexed by default.
    assertEquals(expectedTotalRecords, records.size());
    assertEquals(MetadataPartitionType.PARTITION_STATS.getPartitionPath(), records.get(0).getPartitionPath());
    ((HoodieMetadataPayload) result.collectAsList().get(0).getData()).getColumnStatMetadata().get().getColumnName();
    records.forEach(r -> {
      HoodieMetadataPayload payload = (HoodieMetadataPayload) r.getData();
      assertTrue(payload.getColumnStatMetadata().isPresent());
      // instant1 < instant2 so instant1 should be in the min value and instant2 should be in the max value.
      if (payload.getColumnStatMetadata().get().getColumnName().equals("rider")) {
        assertEquals(String.format("{\"value\": \"rider-%s\"}", instant1), String.valueOf(payload.getColumnStatMetadata().get().getMinValue()));
        assertEquals(String.format("{\"value\": \"rider-%s\"}", instant2), String.valueOf(payload.getColumnStatMetadata().get().getMaxValue()));
      } else if (payload.getColumnStatMetadata().get().getColumnName().equals("driver")) {
        assertEquals(String.format("{\"value\": \"driver-%s\"}", instant1), String.valueOf(payload.getColumnStatMetadata().get().getMinValue()));
        assertEquals(String.format("{\"value\": \"driver-%s\"}", instant2), String.valueOf(payload.getColumnStatMetadata().get().getMaxValue()));
      }
    });
  }

  private static void writeParquetFile(String instant,
                                       StoragePath path,
                                       List<HoodieRecord> records,
                                       HoodieTableMetaClient metaClient,
                                       HoodieLocalEngineContext engineContext) throws IOException {
    HoodieFileWriter writer = HoodieFileWriterFactory.getFileWriter(
        instant,
        path,
        metaClient.getStorage(),
        metaClient.getTableConfig(),
        HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS,
        engineContext.getTaskContextSupplier(),
        HoodieRecord.HoodieRecordType.AVRO);
    for (HoodieRecord record : records) {
      writer.writeWithMetadata(record.getKey(),
          record.rewriteRecordWithNewSchema(AVRO_SCHEMA, CollectionUtils.emptyProps(), AVRO_SCHEMA_WITH_METADATA_FIELDS), HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS);
    }
    writer.close();
  }

  @Test
  public void testGetFileGroupIndexFromFileId() {
    String result = getFileIDForFileGroup(MetadataPartitionType.FILES, 1, "test_partition", Option.empty());
    assertEquals("files-0001-0", result);

    result = getFileIDForFileGroup(MetadataPartitionType.COLUMN_STATS, 2, "stats_partition", Option.empty());
    assertEquals("col-stats-0002-0", result);

    result = getFileIDForFileGroup(MetadataPartitionType.BLOOM_FILTERS, 3, "bloom_partition", Option.empty());
    assertEquals("bloom-filters-0003-0", result);

    result = getFileIDForFileGroup(MetadataPartitionType.RECORD_INDEX, 4, "record_partition", Option.empty());
    assertEquals("record-index-0004-0", result);

    result = getFileIDForFileGroup(MetadataPartitionType.RECORD_INDEX, 12, "record_partition", Option.of("test"));
    assertEquals("record-index-test-0012-0", result);

    result = getFileIDForFileGroup(MetadataPartitionType.SECONDARY_INDEX, 6, "secondary_index_idx_ts", Option.empty());
    assertEquals("secondary-index-idx-ts-0006-0", result);

    result = getFileIDForFileGroup(MetadataPartitionType.EXPRESSION_INDEX, 5, "expr_index_ts", Option.empty());
    assertEquals("expr-index-ts-0005-0", result);
  }

  @Test
  public void testGetColumnsToIndex() {
    HoodieTableConfig tableConfig = metaClient.getTableConfig();

    //test default
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .build();
    List<String> colNames = new ArrayList<>();
    addNColumns(colNames, HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS.defaultValue() + 10);
    List<String> expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    addNColumns(expected, HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS.defaultValue());
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(getTableSchema(expected))), false, V1).keySet()));

    //test with table schema < default
    int tableSchemaSize = HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS.defaultValue() - 10;
    assertTrue(tableSchemaSize > 0);
    colNames = new ArrayList<>();
    addNColumns(colNames, tableSchemaSize);
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    addNColumns(expected, tableSchemaSize);
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(getTableSchema(expected))), false, V1).keySet()));

    //test with max val < tableSchema
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withMaxColumnsToIndexForColStats(3)
        .build();
    colNames = new ArrayList<>();
    addNColumns(colNames, HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS.defaultValue() + 10);
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    addNColumns(expected, 3);
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(getTableSchema(expected))), false, V1).keySet()));

    //test with max val > tableSchema
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withMaxColumnsToIndexForColStats(HoodieMetadataConfig.COLUMN_STATS_INDEX_MAX_COLUMNS.defaultValue() + 10)
        .build();
    colNames = new ArrayList<>();
    addNColumns(colNames, tableSchemaSize);
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    addNColumns(expected, tableSchemaSize);
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(getTableSchema(expected))), false, V1).keySet()));

    //test with list of cols and a nested field as well.
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withColumnStatsIndexForColumns("col_1,col_7,col_11,col_12.col12_1")
        .build();
    colNames = new ArrayList<>();
    addNColumns(colNames, 15);
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    expected.add("col_1");
    expected.add("col_7");
    expected.add("col_11");
    expected.add("col_12.col12_1");

    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .requiredString("col_1")
        .requiredString("col_7")
        .requiredString("col_11")
        .name("col_12").type().record("NestedRecord")
        .fields()
        .requiredString("col12_1")
        .endRecord()
        .noDefault()
        .endRecord();

    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(schema)), false, V1).keySet()));

    //test with list of cols longer than config
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withMaxColumnsToIndexForColStats(1)
        .withColumnStatsIndexForColumns("col_1,col_7,col_11")
        .build();
    colNames = new ArrayList<>();
    addNColumns(colNames, 15);
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    expected.add("col_1");
    expected.add("col_7");
    expected.add("col_11");
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(getTableSchema(expected))), false, V1).keySet()));

    //test with list of cols including meta cols than config
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withColumnStatsIndexForColumns("col_1,col_7,_hoodie_commit_time,col_11,_hoodie_commit_seqno")
        .build();
    colNames = new ArrayList<>();
    addNColumns(colNames, 15);
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    expected.add("col_1");
    expected.add("col_7");
    expected.add("col_11");
    expected.add("_hoodie_commit_seqno");
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(getTableSchema(expected))), false, V1).keySet()));

    //test with avro schema
    schema = new Schema.Parser().parse(SCHEMA_WITH_AVRO_TYPES_STR);
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withColumnStatsIndexForColumns("booleanField,decimalField,localTimestampMillisField")
        .build();
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    expected.add("booleanField");
    expected.add("decimalField");
    expected.add("localTimestampMillisField");
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(schema)), true, V1).keySet()));

    //test with avro schema and nested fields and unsupported types
    schema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD_STR);
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withColumnStatsIndexForColumns("firstname,student.lastnameNested,student")
        .build();
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    expected.add("firstname");
    expected.add("student.lastnameNested");
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(schema)), false, V1).keySet()));

    //test with avro schema with max cols set
    schema = new Schema.Parser().parse(SCHEMA_WITH_AVRO_TYPES_STR);
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withMaxColumnsToIndexForColStats(2)
        .build();
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    expected.add("booleanField");
    expected.add("intField");
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(schema)), false, V1).keySet()));
    //test with avro schema with meta cols
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(HoodieAvroUtils.addMetadataFields(schema))), false, V1).keySet()));

    //test with avro schema with type filter
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withMaxColumnsToIndexForColStats(100)
        .build();
    expected = new ArrayList<>(Arrays.asList(HoodieTableMetadataUtil.META_COLS_TO_ALWAYS_INDEX));
    expected.add("timestamp");
    expected.add("_row_key");
    expected.add("partition_path");
    expected.add("rider");
    expected.add("driver");
    expected.add("begin_lat");
    expected.add("begin_lon");
    expected.add("end_lat");
    expected.add("end_lon");
    expected.add("distance_in_meters");
    expected.add("seconds_since_epoch");
    expected.add("weight");
    expected.add("current_date");
    expected.add("current_ts");
    expected.add("_hoodie_is_deleted");
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(HoodieTestDataGenerator.AVRO_SCHEMA)), false, V1).keySet()));
    //test with avro schema with meta cols
    assertListEquality(expected,
        new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
            Lazy.eagerly(Option.of(HoodieAvroUtils.addMetadataFields(HoodieTestDataGenerator.AVRO_SCHEMA))), false, V1).keySet()));

    //test with meta cols disabled
    tableConfig.setValue(HoodieTableConfig.POPULATE_META_FIELDS.key(), "false");
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .build();
    colNames = new ArrayList<>();
    addNColumns(colNames, tableSchemaSize);
    expected = new ArrayList<>();
    addNColumns(expected, tableSchemaSize);
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(getTableSchema(expected))), false, V1).keySet()));

    //test with meta cols disabled with col list
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withColumnStatsIndexForColumns("col_1,col_7,col_11")
        .build();
    colNames = new ArrayList<>();
    addNColumns(colNames, 15);
    expected = new ArrayList<>();
    expected.add("col_1");
    expected.add("col_7");
    expected.add("col_11");
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(getTableSchema(expected))), false, V1).keySet()));

    //test with meta cols disabled with avro schema
    metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true).withMetadataIndexColumnStats(true)
        .withColumnStatsIndexForColumns("booleanField,decimalField,localTimestampMillisField")
        .build();
    expected = new ArrayList<>();
    expected.add("booleanField");
    expected.add("decimalField");
    expected.add("localTimestampMillisField");
    assertListEquality(expected, new ArrayList<>(HoodieTableMetadataUtil.getColumnsToIndex(tableConfig, metadataConfig,
        Lazy.eagerly(Option.of(schema)), true, V1).keySet()));
  }

  private void assertListEquality(List<String> expected, List<String> actual) {
    Collections.sort(expected);
    Collections.sort(actual);
    assertEquals(expected, actual);
  }

  private void addNColumns(List<String> list, int n) {
    for (int i = 0; i < n; i++) {
      list.add("col_" + i);
    }
  }

  private Schema getTableSchema(List<String> fieldNames) {
    List<Schema.Field> fields = fieldNames.stream()
        .map(fieldName -> new Schema.Field(fieldName, createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE)).collect(Collectors.toList());
    return Schema.createRecord("Test_Hoodie_Record", "", "", false, fields);
  }

  @Test
  public void testValidateDataTypeForPartitionStats() {
    // Create a dummy schema with both complex and primitive types
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .requiredString("stringField")
        .optionalInt("intField")
        .optionalBoolean("booleanField")
        .optionalFloat("floatField")
        .optionalDouble("doubleField")
        .optionalLong("longField")
        .optionalBytes("bytesField")
        .name("unionIntField").type().unionOf().nullType().and().intType().endUnion().noDefault()
        .name("arrayField").type().array().items().stringType().noDefault()
        .name("mapField").type().map().values().intType().noDefault()
        .name("structField").type().record("NestedRecord")
        .fields()
        .requiredString("nestedString")
        .endRecord()
        .noDefault()
        .endRecord();

    // Test for primitive fields
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("stringField").schema(), Option.empty(), V1));
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("intField").schema(), Option.empty(), V1));
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("booleanField").schema(), Option.empty(), V1));
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("floatField").schema(), Option.empty(), V1));
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("doubleField").schema(), Option.empty(), V1));
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("longField").schema(), Option.empty(), V1));
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("unionIntField").schema(), Option.empty(), V1));

    // Test for unsupported fields
    assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("arrayField").schema(), Option.empty(), V1));
    assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("mapField").schema(), Option.empty(), V1));
    assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("structField").schema(), Option.empty(), V1));
    assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("bytesField").schema(), Option.of(HoodieRecord.HoodieRecordType.SPARK), V1));

    // Test for logical types
    Schema dateFieldSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    schema = SchemaBuilder.record("TestRecord")
        .fields()
        .name("dateField").type(dateFieldSchema).noDefault()
        .endRecord();
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema.getField("dateField").schema(), Option.empty(), V1));

    // Test for logical decimal type with allowed precision and scale
    schema = Schema.create(Schema.Type.BYTES);
    LogicalTypes.Decimal decimalType = LogicalTypes.decimal(30, 15);
    decimalType.addToSchema(schema);
    // Expect the column to be supported.
    assertTrue(HoodieTableMetadataUtil.isColumnTypeSupported(schema, Option.of(HoodieRecord.HoodieRecordType.AVRO), V1));

    // Test for logical decimal type with precision and scale exceeding the limit
    schema = Schema.create(Schema.Type.BYTES);
    decimalType = LogicalTypes.decimal(35, 20);
    decimalType.addToSchema(schema);
    // Expect the column to be unsupported.
    assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(schema, Option.of(HoodieRecord.HoodieRecordType.AVRO), V1));

    // Test for logical decimal type with precision exceeding limit after upscaling
    schema = Schema.create(Schema.Type.BYTES);
    decimalType = LogicalTypes.decimal(28, 10);
    decimalType.addToSchema(schema);
    // Expect the column to be unsupported.
    assertFalse(HoodieTableMetadataUtil.isColumnTypeSupported(schema, Option.of(HoodieRecord.HoodieRecordType.AVRO), V1));
  }

  @Test
  public void testComputeRevivedAndDeletedKeys() {
    // Test Input Sets
    Set<String> validKeysForPreviousLogs = new HashSet<>(Arrays.asList("K1", "K2", "K3"));
    Set<String> deletedKeysForPreviousLogs = new HashSet<>(Arrays.asList("K4", "K5"));
    Set<String> validKeysForAllLogs = new HashSet<>(Arrays.asList("K2", "K4", "K6")); // revived: K4, deleted: K1
    Set<String> deletedKeysForAllLogs = new HashSet<>(Arrays.asList("K1", "K5", "K7"));

    // Expected Results
    Set<String> expectedRevivedKeys = new HashSet<>(Collections.singletonList("K4")); // Revived: Deleted in previous but now valid
    Set<String> expectedDeletedKeys = new HashSet<>(Collections.singletonList("K1")); // Deleted: Valid in previous but now deleted

    // Compute Revived and Deleted Keys
    Pair<Set<String>, Set<String>> result = computeRevivedAndDeletedKeys(validKeysForPreviousLogs, deletedKeysForPreviousLogs, validKeysForAllLogs, deletedKeysForAllLogs);
    assertEquals(expectedRevivedKeys, result.getKey());
    assertEquals(expectedDeletedKeys, result.getValue());

    // Case 1: All keys remain valid, just updates, no deletes or revives
    Set<String> allValidKeys = new HashSet<>(Arrays.asList("K1", "K2", "K3"));
    Set<String> allEmpty = Collections.emptySet();
    result = computeRevivedAndDeletedKeys(allValidKeys, allEmpty, allValidKeys, allEmpty);
    assertEquals(Collections.emptySet(), result.getKey());
    assertEquals(Collections.emptySet(), result.getValue());

    // Case 2: All keys are deleted
    result = computeRevivedAndDeletedKeys(allValidKeys, allEmpty, allEmpty, allValidKeys);
    assertEquals(Collections.emptySet(), result.getKey());
    assertEquals(allValidKeys, result.getValue());

    // Case 3: Delete K3
    result = computeRevivedAndDeletedKeys(allValidKeys, allEmpty, new HashSet<>(Arrays.asList("K1", "K2")), new HashSet<>(Collections.singletonList("K3")));
    assertEquals(Collections.emptySet(), result.getKey());
    assertEquals(new HashSet<>(Collections.singletonList("K3")), result.getValue());

    // Case 4: Empty input sets
    result = computeRevivedAndDeletedKeys(Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    assertEquals(Collections.emptySet(), result.getKey());
    assertEquals(Collections.emptySet(), result.getValue());
  }

  @Test
  public void testGetExpressionIndexPartitionsToInit() {
    MetadataPartitionType partitionType = MetadataPartitionType.EXPRESSION_INDEX;

    // Mock meta client
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    when(metaClient.getIndexMetadata()).thenReturn(Option.empty());

    // Mock metadata partitions
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getMetadataPartitions()).thenReturn(new HashSet<>(Collections.singleton("expr_index_idx_ts")));
    when(tableConfig.getTableVersion()).thenReturn(HoodieTableVersion.current());

    // Build metadata config
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true)
        .withExpressionIndexColumn("ts")
        .withExpressionIndexType("column_stats")
        .withExpressionIndexOptions(Collections.singletonMap("expr", "from_unixtime(ts, format='yyyy-MM-dd')"))
        .build();

    // Get partitions to init
    Set<String> result = HoodieTableMetadataUtil.getExpressionIndexPartitionsToInit(partitionType, metadataConfig, metaClient);

    // Verify the result
    assertNotNull(result);
    assertTrue(result.isEmpty());
    verify(metaClient, atLeastOnce()).buildIndexDefinition(any());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("mapRecordKeyToFileGroupIndexTestCases")
  public void testMapRecordKeyToFileGroupIndex(
      String testName,
      String recordKey,
      int numFileGroups,
      String partitionName,
      HoodieIndexVersion version,
      int expectedIndex) {
    boolean needsSecondaryKeyExtraction = MetadataPartitionType.SECONDARY_INDEX.matchesPartitionPath(partitionName)
        && version.greaterThanOrEquals(HoodieIndexVersion.V2);
    SerializableBiFunction<String, Integer, Integer> mappingFunction =
        HoodieTableMetadataUtil.getSecondaryKeyToFileGroupMappingFunction(needsSecondaryKeyExtraction);
    int index = mappingFunction.apply(recordKey, numFileGroups);
    assertEquals(expectedIndex, index, "File group index should match expected value");
  }

  private static Stream<Arguments> mapRecordKeyToFileGroupIndexTestCases() {
    return Stream.of(
        // Test case 1: Regular record key (no secondary index)
        Arguments.of(
            "Regular record key",
            "test_key",
            10,
            "files",
            HoodieIndexVersion.V1,
            8  // Calculated using the explicit hashing algorithm
        ),
        // Test case 2: Secondary index record key with version >= 2
        Arguments.of(
            "Secondary index record key with version >= 2",
            "secondary_key$primary_key",
            10,
            "secondary_index_idx_ts",
            V2,
            8  // Uses secondary key portion for hashing
        ),
        // Test case 3: Secondary index record key but version < 2
        Arguments.of(
            "Secondary index record key but version < 2",
            "secondary_key$primary_key",
            10,
            "secondary_index_idx_ts",
            HoodieIndexVersion.V1,
            0  // Uses full key for hashing
        ),
        // Test case 4: Secondary index record key but not in secondary index partition
        Arguments.of(
            "Secondary index record key but not in secondary index partition",
            "secondary_key$primary_key",
            10,
            "files",
            HoodieIndexVersion.V1,
            0  // Uses full key for hashing since not in secondary index partition
        ),
        // Test case 7: Single file group
        Arguments.of(
            "Single file group",
            "test_key$record_key",
            1,
            "secondary_index_idx_ts",
            V2,
            0  // Any key with numFileGroups=1 should return 0
        ),
        // Test case 8: Record key with special characters
        Arguments.of(
            "Record key with special characters",
            "test@key#123",
            10,
            "files",
            V1,
            0  // Calculated using the explicit hashing algorithm
        )
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("mapKeyNoSeparatorToFileGroupIndexTestCases")
  public void testMapKeyNoSeparatorToFileGroupIndex(
      String testName,
      String recordKey,
      int numFileGroups,
      String partitionName,
      HoodieIndexVersion version,
      int expectedIndex) {
    // If the key does not have separator, hashing function for SI write will error out.
    boolean needsSecondaryKeyExtraction = MetadataPartitionType.SECONDARY_INDEX.matchesPartitionPath(partitionName)
        && version.greaterThanOrEquals(HoodieIndexVersion.V2);
    SerializableBiFunction<String, Integer, Integer> mappingFunction =
        HoodieTableMetadataUtil.getSecondaryKeyToFileGroupMappingFunction(needsSecondaryKeyExtraction);
    assertThrows(IllegalStateException.class, () -> mappingFunction.apply(recordKey, numFileGroups));

    int index = HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(recordKey, numFileGroups);
    assertEquals(expectedIndex, index, "File group index should match expected value");
  }

  private static Stream<Arguments> mapKeyNoSeparatorToFileGroupIndexTestCases() {
    return Stream.of(
        // Test case 5: Secondary index record key but no separator
        Arguments.of(
            "Secondary index record key but no separator",
            "primary_key_secondary_key",
            10,
            "secondary_index_idx_ts",
            V2,
            7
        ),
        // Test case 6: Empty record key
        Arguments.of(
            "Empty record key",
            "",
            10,
            "secondary_index_idx_ts",
            V2,
            0
        )
    );
  }

  /*
  @Disabled
  @Test
  public void testConvertMetadataToPartitionStatRecordsWithoutColumnStats() throws Exception {
    // Test the case where shouldScanColStatsForTightBound = false (no column stats partition)
    // In this case, only new write stats should be processed
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    String instant1 = "20240101120000000";
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS.toString());
    hoodieTestTable = hoodieTestTable.addCommit(instant1, Option.of(commitMetadata));

    // Create test data files
    String partition = DATE_PARTITIONS.get(0);
    String fileId = UUID.randomUUID().toString();
    StoragePath filePath = new StoragePath(hoodieTestTable.getBaseFilePath(partition, fileId).toUri());
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(instant1, 10, partition);
    writeParquetFile(instant1, filePath, records, metaClient, engineContext);

    // Add write stats to commit metadata
    commitMetadata.getPartitionToWriteStats().put(partition, Collections.singletonList(
        createWriteStat(partition, filePath.getName(), instant1)
    ));

    // Mock HoodieTableMetadata
    HoodieTableMetadata tableMetadata = mock(HoodieTableMetadata.class);
    when(tableMetadata.getAllFilesInPartitions(any())).thenReturn(Collections.emptyMap());

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexColumnStats(false)  // Disable column stats
        .withMetadataIndexPartitionStats(true)
        .withColumnStatsIndexForColumns("rider,driver")
        .withPartitionStatsIndexParallelism(1)
        .build();

    // Call the method under test
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.convertMetadataToPartitionStatRecords(
        commitMetadata, "20240102120000000", engineContext, metaClient, tableMetadata, metadataConfig, Option.empty(), false);

    // Validate results - should have partition stats from write stats only
    List<HoodieRecord> partitionStatsRecords = result.collectAsList();
    assertFalse(partitionStatsRecords.isEmpty());
    assertEquals(MetadataPartitionType.PARTITION_STATS.getPartitionPath(), partitionStatsRecords.get(0).getPartitionPath());
  }

  @Disabled
  @Test
  public void testConvertMetadataToPartitionStatRecordsWithColumnStatsEnabled() throws Exception {
    // Test the case where shouldScanColStatsForTightBound = true (column stats partition exists)
    // In this case, both existing files and new write stats should be combined
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());

    // Create two commits - one existing and one new
    String instant1 = "20240101120000000";
    HoodieCommitMetadata commitMetadata1 = new HoodieCommitMetadata();
    commitMetadata1.setOperationType(WriteOperationType.INSERT);
    commitMetadata1.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS.toString());
    hoodieTestTable = hoodieTestTable.addCommit(instant1, Option.of(commitMetadata1));

    String instant2 = "20240101130000000";
    HoodieCommitMetadata commitMetadata2 = new HoodieCommitMetadata();
    commitMetadata2.setOperationType(WriteOperationType.UPSERT);
    commitMetadata2.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS.toString());
    hoodieTestTable = hoodieTestTable.addCommit(instant2);

    String partition = DATE_PARTITIONS.get(0);

    // Create existing file from instant1
    String fileId1 = UUID.randomUUID().toString();
    StoragePath filePath1 = new StoragePath(hoodieTestTable.getBaseFilePath(partition, fileId1).toUri());
    List<HoodieRecord> records1 = dataGen.generateInsertsForPartition(instant1, 10, partition);
    writeParquetFile(instant1, filePath1, records1, metaClient, engineContext);

    // Create new file from instant2
    String fileId2 = UUID.randomUUID().toString();
    StoragePath filePath2 = new StoragePath(hoodieTestTable.getBaseFilePath(partition, fileId2).toUri());
    List<HoodieRecord> records2 = dataGen.generateInsertsForPartition(instant2, 10, partition);
    writeParquetFile(instant2, filePath2, records2, metaClient, engineContext);

    // Add write stats for instant2 only (the new commit)
    commitMetadata2.getPartitionToWriteStats().put(partition, Collections.singletonList(
        createWriteStat(partition, filePath2.getName(), instant2)
    ));

    // Mock HoodieTableMetadata with column stats available
    HoodieTableMetadata tableMetadata = mock(HoodieTableMetadata.class);
    when(tableMetadata.getAllFilesInPartitions(any())).thenReturn(Collections.emptyMap());
    when(tableMetadata.getRecordsByKeyPrefixes(any(), any(), any())).thenReturn(engineContext.parallelize(Collections.emptyList()));

    // Enable column stats in metaClient to make shouldScanColStatsForTightBound return true
    metaClient.getTableConfig().setValue(HoodieTableConfig.TABLE_METADATA_PARTITIONS.key(),
        MetadataPartitionType.FILES.getPartitionPath() + "," + MetadataPartitionType.COLUMN_STATS.getPartitionPath());

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexColumnStats(true)
        .withMetadataIndexPartitionStats(true)
        .withColumnStatsIndexForColumns("rider,driver")
        .withPartitionStatsIndexParallelism(1)
        .build();

    // Call the method under test
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.convertMetadataToPartitionStatRecords(
        commitMetadata2, "20240102120000000", engineContext, metaClient, tableMetadata, metadataConfig, Option.empty(), false);

    // Validate results
    List<HoodieRecord> partitionStatsRecords = result.collectAsList();
    assertFalse(partitionStatsRecords.isEmpty());
    assertEquals(MetadataPartitionType.PARTITION_STATS.getPartitionPath(), partitionStatsRecords.get(0).getPartitionPath());

    // Verify that getRecordsByKeyPrefixes was called to fetch existing column stats
    verify(tableMetadata, atLeastOnce()).getRecordsByKeyPrefixes(any(), any(), any());
  }

  @Test
  public void testConvertMetadataToPartitionStatRecordsWithDeletePartition() throws Exception {
    // Test DELETE_PARTITION operation which should create stub records
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    String instant1 = "20240101120000000";
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.DELETE_PARTITION);
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS.toString());

    String partition = DATE_PARTITIONS.get(0);
    commitMetadata.setPartitionToReplaceFileIds(Collections.singletonMap(partition, Collections.singletonList("file1")));

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexColumnStats(true)
        .withMetadataIndexPartitionStats(true)
        .withColumnStatsIndexForColumns("rider,driver")
        .build();

    // Call the method under test with isDeletePartition = true
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.convertMetadataToPartitionStatRecords(
        commitMetadata, "20240102120000000", engineContext, metaClient, null, metadataConfig, Option.empty(), true);

    // Validate results - should have stub records for all columns
    List<HoodieRecord> partitionStatsRecords = result.collectAsList();
    assertFalse(partitionStatsRecords.isEmpty());
    assertEquals(MetadataPartitionType.PARTITION_STATS.getPartitionPath(), partitionStatsRecords.get(0).getPartitionPath());

    // Verify all records are delete records (stubs)
    partitionStatsRecords.forEach(record -> {
      HoodieMetadataPayload payload = (HoodieMetadataPayload) record.getData();
      assertTrue(payload.getColumnStatMetadata().isPresent());
      // Stub records have empty file name
      assertEquals("", payload.getColumnStatMetadata().get().getFileName());
    });
  }

  @Test
  public void testConvertMetadataToPartitionStatRecordsWithEmptyWriteStats() throws Exception {
    // Test with empty write stats - should return empty result
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS.toString());

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexPartitionStats(true)
        .withColumnStatsIndexForColumns("rider,driver")
        .build();

    // Call the method under test with no write stats
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.convertMetadataToPartitionStatRecords(
        commitMetadata, "20240102120000000", engineContext, metaClient, null, metadataConfig, Option.empty(), false);

    // Validate results - should be empty
    assertTrue(result.isEmpty());
  }

  @Test
  public void testConvertMetadataToPartitionStatRecordsWithNoSchema() throws Exception {
    // Test with no schema in commit metadata - should return empty result
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    // No schema added to metadata

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexPartitionStats(true)
        .withColumnStatsIndexForColumns("rider,driver")
        .build();

    // Call the method under test
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.convertMetadataToPartitionStatRecords(
        commitMetadata, "20240102120000000", engineContext, metaClient, null, metadataConfig, Option.empty(), false);

    // Validate results - should be empty due to no schema
    assertTrue(result.isEmpty());
  }

  @Test
  public void testConvertMetadataToPartitionStatRecordsMultiplePartitions() throws Exception {
    // Test with multiple partitions to ensure partition stats are generated correctly for each
    HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
    String instant1 = "20240101120000000";
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, HoodieTestDataGenerator.AVRO_SCHEMA_WITH_METADATA_FIELDS.toString());
    hoodieTestTable = hoodieTestTable.addCommit(instant1, Option.of(commitMetadata));

    // Create files in multiple partitions
    for (String partition : DATE_PARTITIONS) {
      String fileId = UUID.randomUUID().toString();
      StoragePath filePath = new StoragePath(hoodieTestTable.getBaseFilePath(partition, fileId).toUri());
      List<HoodieRecord> records = dataGen.generateInsertsForPartition(instant1, 10, partition);
      writeParquetFile(instant1, filePath, records, metaClient, engineContext);

      commitMetadata.getPartitionToWriteStats().put(partition, Collections.singletonList(
          createWriteStat(partition, filePath.getName(), instant1)
      ));
    }

    // Mock HoodieTableMetadata
    HoodieTableMetadata tableMetadata = mock(HoodieTableMetadata.class);
    when(tableMetadata.getAllFilesInPartitions(any())).thenReturn(Collections.emptyMap());

    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
        .enable(true)
        .withMetadataIndexPartitionStats(true)
        .withColumnStatsIndexForColumns("rider,driver")
        .withPartitionStatsIndexParallelism(3)  // Test with parallelism > 1
        .build();

    // Call the method under test
    HoodieData<HoodieRecord> result = HoodieTableMetadataUtil.convertMetadataToPartitionStatRecords(
        commitMetadata, "20240102120000000", engineContext, metaClient, tableMetadata, metadataConfig, Option.empty(), false);

    // Validate results - should have partition stats for all partitions
    List<HoodieRecord> partitionStatsRecords = result.collectAsList();
    assertFalse(partitionStatsRecords.isEmpty());

    // Each partition should have records for indexed columns (rider, driver, and 3 meta fields)
    // 3 partitions * 5 columns = 15 records minimum
    assertTrue(partitionStatsRecords.size() >= DATE_PARTITIONS.size() * 2);
  }

  private org.apache.hudi.common.model.HoodieWriteStat createWriteStat(String partition, String fileName, String instant) {
    org.apache.hudi.common.model.HoodieWriteStat writeStat = new org.apache.hudi.common.model.HoodieWriteStat();
    writeStat.setPartitionPath(partition);
    writeStat.setPath(fileName);
    writeStat.setFileId(fileName.substring(0, fileName.indexOf('_')));
    writeStat.setPrevCommit(instant);
    writeStat.setNumWrites(10);
    writeStat.setTotalWriteBytes(1024);
    writeStat.setFileSizeInBytes(1024);
    return writeStat;
  }
   */
}
