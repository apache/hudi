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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncClient;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetReader;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.utilities.DummySchemaProvider;
import org.apache.hudi.utilities.config.SourceTestConfig;
import org.apache.hudi.utilities.ingestion.HoodieIngestionException;
import org.apache.hudi.utilities.schema.KafkaOffsetPostProcessor;
import org.apache.hudi.utilities.sources.CsvDFSSource;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.hudi.utilities.sources.ORCDFSSource;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.sources.SqlSource;
import org.apache.hudi.utilities.sources.TestParquetDFSSourceEmptyBatch;
import org.apache.hudi.utilities.streamer.HoodieStreamer;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.testutils.sources.DistributedTestDataSource;
import org.apache.hudi.utilities.transform.SqlQueryBasedTransformer;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD;
import static org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.TestSpecificPartitionTransformer;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.TripsWithDistanceTransformer;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.assertCheckpointVersion;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.prepareJsonKafkaDFSFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Source-format coverage for {@link HoodieDeltaStreamer}: the parquet, ORC, CSV, JSON-Kafka and SQL sources,
 * the transformers layered on them, the row-writer bulk-insert path and the insert-overwrite and delete operations.
 * Shared helpers and the transformer, key generator and payload classes these tests name live in
 * {@link TestHoodieDeltaStreamer}.
 */
@Slf4j
public class TestHoodieDeltaStreamerSources extends HoodieDeltaStreamerTestBase {

  // Kept per class rather than hoisted to the base, because the other base subclasses deliberately do not increment testNum.
  @AfterEach
  public void perTestAfterEach() {
    testNum++;
  }

  @Test
  public void testBulkInsertRowWriterNoSchemaProviderNoTransformer() throws Exception {
    testBulkInsertRowWriterMultiBatches(false, null);
  }

  @Test
  public void testBulkInsertRowWriterWithoutSchemaProviderAndTransformer() throws Exception {
    testBulkInsertRowWriterMultiBatches(false, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Test
  public void testBulkInsertRowWriterWithSchemaProviderAndNoTransformer() throws Exception {
    testBulkInsertRowWriterMultiBatches(true, null);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT", "NINE"})
  public void testBulkInsertRowWriterWithSchemaProviderAndTransformer(HoodieTableVersion tableVersion) throws Exception {
    testBulkInsertRowWriterMultiBatches(true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()), false, tableVersion);
  }

  @Test
  public void testBulkInsertRowWriterForEmptyBatch() throws Exception {
    testBulkInsertRowWriterMultiBatches(false, null, true, HoodieTableVersion.current());
  }

  private void testBulkInsertRowWriterMultiBatches(boolean useSchemaProvider, List<String> transformerClassNames) throws Exception {
    testBulkInsertRowWriterMultiBatches(useSchemaProvider, transformerClassNames, false, HoodieTableVersion.current());
  }

  private void testBulkInsertRowWriterMultiBatches(Boolean useSchemaProvider, List<String> transformerClassNames, boolean testEmptyBatch, HoodieTableVersion hoodieTableVersion) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 100;
    boolean hasTransformer = transformerClassNames != null && !transformerClassNames.isEmpty();
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "", false, true);

    String tableBasePath = basePath + "/test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT, testEmptyBatch ? TestParquetDFSSourceEmptyBatch.class.getName()
            : ParquetDFSSource.class.getName(),
        transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
        useSchemaProvider, 100000, false, null, null, "timestamp", null, false, hoodieTableVersion);
    cfg.configs.add(DataSourceWriteOptions.ENABLE_ROW_WRITER().key() + "=true");
    cfg.configs.add(HoodieWriteConfig.WRITE_TABLE_VERSION.key() + "=" + hoodieTableVersion.versionCode());
    syncOnce(cfg);
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);

    HoodieStreamer deltaStreamer = null;
    try {
      if (testEmptyBatch) {
        prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
            PARQUET_SOURCE_ROOT, false, "partition_path", "0");
        prepareParquetDFSFiles(100, PARQUET_SOURCE_ROOT, "2.parquet", false, null, null);
        syncOnce(cfg);
        // since we mimic'ed empty batch, total records should be same as first sync().
        assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
        HoodieTableMetaClient metaClient = createMetaClient(jsc, tableBasePath);

        // validate table schema fetches valid schema from last but one commit.
        TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
        assertNotEquals(tableSchemaResolver.getTableSchema(), Schema.create(Schema.Type.NULL).toString());
        // schema from latest commit and last but one commit should match
        compareLatestTwoSchemas(metaClient);
        prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
            PARQUET_SOURCE_ROOT, false, "partition_path", "");
      }

      int recordsSoFar = 100;
      deltaStreamer = new HoodieDeltaStreamer(cfg, jsc);
      // add 3 more batches and ensure all commits succeed.
      for (int i = 2; i < 5; i++) {
        prepareParquetDFSFiles(100, PARQUET_SOURCE_ROOT, Integer.toString(i) + ".parquet", false, null, null);
        deltaStreamer.sync();
        assertRecordCount(recordsSoFar + (i - 1) * 100, tableBasePath, sqlContext);
        if (i == 2 || i == 4) { // this validation reloads the timeline. So, we are validating only for first and last batch.
          // validate commit metadata for all completed commits to have valid schema in extra metadata.
          HoodieTableMetaClient metaClient = createMetaClient(jsc, tableBasePath);
          metaClient.reloadActiveTimeline().getCommitsTimeline()
              .filterCompletedInstants().getInstants()
              .forEach(entry -> assertValidSchemaAndOperationTypeInCommitMetadata(
                  entry, metaClient, WriteOperationType.BULK_INSERT));
        }
      }
      assertCheckpointVersion(createMetaClient(jsc, tableBasePath));
    } finally {
      deltaStreamer.shutdownGracefully();
    }
    testNum++;
  }

  /**
   * Test Bulk Insert and upserts with hive syncing. Tests Hudi incremental processing using a 2 step pipeline The first
   * step involves using a SQL template to transform a source TEST-DATA-SOURCE ============================> HUDI TABLE
   * 1 ===============> HUDI TABLE 2 (incr-pull with transform) (incr-pull) Hudi Table 1 is synced with Hive.
   */
  @Test
  public void testBulkInsertsAndUpsertsWithSQLBasedTransformerFor2StepPipeline() throws Exception {
    HoodieRecordType recordType = HoodieRecordType.AVRO;
    String tableBasePath = basePath + "/" + recordType.toString() + "/test_table2";
    String downstreamTableBasePath = basePath + "/" + recordType.toString() + "/test_downstream_table2";

    // Initial bulk insert to ingest to first hudi table
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, true);
    addRecordMerger(recordType, cfg.configs);
    // NOTE: We should not have need to set below config, 'datestr' should have assumed date partitioning
    cfg.configs.add("hoodie.datasource.hive_sync.partition_fields=year,month,day");
    new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()).sync();
    assertRecordCount(1000, tableBasePath, sqlContext);
    assertDistanceCount(1000, tableBasePath, sqlContext);
    assertDistanceCountWithExactValue(1000, tableBasePath, sqlContext);
    HoodieInstant lastInstantForUpstreamTable = TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

    // Now incrementally pull from the above hudi table and ingest to second table
    HoodieDeltaStreamer.Config downstreamCfg =
        TestHelpers.makeConfigForHudiIncrSrc(tableBasePath, downstreamTableBasePath, WriteOperationType.BULK_INSERT,
            true, null);
    addRecordMerger(recordType, downstreamCfg.configs);
    syncOnce(new HoodieDeltaStreamer(downstreamCfg, jsc, fs, hiveServer.getHiveConf()));
    assertRecordCount(1000, downstreamTableBasePath, sqlContext);
    assertDistanceCount(1000, downstreamTableBasePath, sqlContext);
    assertDistanceCountWithExactValue(1000, downstreamTableBasePath, sqlContext);
    TestHelpers.assertCommitMetadataForIncrSource(lastInstantForUpstreamTable.getCompletionTime(), downstreamTableBasePath, 1);

    // No new data => no commits for upstream table
    cfg.sourceLimit = 0;
    syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()));
    assertRecordCount(1000, tableBasePath, sqlContext);
    assertDistanceCount(1000, tableBasePath, sqlContext);
    assertDistanceCountWithExactValue(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

    // with no change in upstream table, no change in downstream too when pulled.
    HoodieDeltaStreamer.Config downstreamCfg1 =
        TestHelpers.makeConfigForHudiIncrSrc(tableBasePath, downstreamTableBasePath,
            WriteOperationType.BULK_INSERT, true, DummySchemaProvider.class.getName());
    syncOnce(downstreamCfg1);
    assertRecordCount(1000, downstreamTableBasePath, sqlContext);
    assertDistanceCount(1000, downstreamTableBasePath, sqlContext);
    assertDistanceCountWithExactValue(1000, downstreamTableBasePath, sqlContext);
    TestHelpers.assertCommitMetadataForIncrSource(lastInstantForUpstreamTable.getCompletionTime(), downstreamTableBasePath, 1);

    // upsert() #1 on upstream hudi table
    cfg.sourceLimit = 2000;
    cfg.operation = WriteOperationType.UPSERT;
    syncOnce(new HoodieDeltaStreamer(cfg, jsc, fs, hiveServer.getHiveConf()));
    assertRecordCount(1950, tableBasePath, sqlContext);
    assertDistanceCount(1950, tableBasePath, sqlContext);
    assertDistanceCountWithExactValue(1950, tableBasePath, sqlContext);
    lastInstantForUpstreamTable = TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
    List<Row> counts = countsPerCommit(tableBasePath, sqlContext);
    assertEquals(1950, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    // Incrementally pull changes in upstream hudi table and apply to downstream table
    downstreamCfg =
        TestHelpers.makeConfigForHudiIncrSrc(tableBasePath, downstreamTableBasePath, WriteOperationType.UPSERT,
            false, null);
    addRecordMerger(recordType, downstreamCfg.configs);
    downstreamCfg.sourceLimit = 2000;
    syncOnce(downstreamCfg);
    assertRecordCount(2000, downstreamTableBasePath, sqlContext);
    assertDistanceCount(2000, downstreamTableBasePath, sqlContext);
    assertDistanceCountWithExactValue(2000, downstreamTableBasePath, sqlContext);
    HoodieInstant finalInstant =
        TestHelpers.assertCommitMetadataForIncrSource(lastInstantForUpstreamTable.getCompletionTime(), downstreamTableBasePath, 2);
    counts = countsPerCommit(downstreamTableBasePath, sqlContext);
    assertEquals(2000, counts.stream().mapToLong(entry -> entry.getLong(1)).sum());

    // Test Hive integration
    HiveSyncConfig hiveSyncConfig = getHiveSyncConfig(tableBasePath, "hive_trips");
    hiveSyncConfig.setValue(HoodieSyncConfig.META_SYNC_PARTITION_FIELDS, "year,month,day");
    hiveSyncConfig.setHadoopConf(hiveTestService.getHiveConf());
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setConf(context.getStorageConf())
        .setBasePath(tableBasePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
    try (HoodieHiveSyncClient hiveClient = new HoodieHiveSyncClient(hiveSyncConfig, metaClient)) {
      final String tableName = hiveSyncConfig.getString(HoodieSyncConfig.META_SYNC_TABLE_NAME);
      assertTrue(hiveClient.tableExists(tableName), "Table " + tableName + " should exist");
      assertEquals(3, hiveClient.getAllPartitions(tableName).size(),
          "Table partitions should match the number of partitions we wrote");
      assertEquals(lastInstantForUpstreamTable.requestedTime(),
          hiveClient.getLastCommitTimeSynced(tableName).get(),
          "The last commit that was synced should be updated in the TBLPROPERTIES");
      UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
      UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, downstreamTableBasePath);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDistributedTestDataSource(boolean persistSourceRdd) {
    TypedProperties props = new TypedProperties();
    props.setProperty(SourceTestConfig.MAX_UNIQUE_RECORDS_PROP.key(), "1000");
    props.setProperty(SourceTestConfig.NUM_SOURCE_PARTITIONS_PROP.key(), "1");
    props.setProperty(SourceTestConfig.USE_ROCKSDB_FOR_TEST_DATAGEN_KEYS.key(), "true");
    props.setProperty(ERROR_TABLE_PERSIST_SOURCE_RDD.key(), String.valueOf(persistSourceRdd));
    DistributedTestDataSource distributedTestDataSource = new DistributedTestDataSource(props, jsc, sparkSession, null);
    InputBatch<JavaRDD<GenericRecord>> batch = distributedTestDataSource.fetchNext(Option.empty(), 10000000);
    if (persistSourceRdd) {
      Exception actualException = assertThrows(UnsupportedOperationException.class, () -> batch.getBatch().get().cache());
      assertTrue(actualException.getMessage().contains("Cannot change storage level of an RDD after it was already assigned a level"));
    } else {
      batch.getBatch().get().cache();
    }
    long c = batch.getBatch().get().count();
    assertEquals(1000, c);
  }

  private void testParquetDFSSource(boolean useSchemaProvider, List<String> transformerClassNames) throws Exception {
    testParquetDFSSource(useSchemaProvider, transformerClassNames, false);
  }

  private void testParquetDFSSource(boolean useSchemaProvider, List<String> transformerClassNames, boolean testEmptyBatch) throws Exception {
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 10;
    boolean hasTransformer = transformerClassNames != null && !transformerClassNames.isEmpty();
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "");

    String tableBasePath = basePath + "/test_parquet_table" + testNum;
    HoodieDeltaStreamer.Config cfg =
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, testEmptyBatch ? TestParquetDFSSourceEmptyBatch.class.getName()
                : ParquetDFSSource.class.getName(),
            transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
            useSchemaProvider, 100000, false, null, null, "timestamp", null);
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(cfg, jsc);
    deltaStreamer.sync();
    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    deltaStreamer.shutdownGracefully();

    if (testEmptyBatch) {
      prepareParquetDFSFiles(100, PARQUET_SOURCE_ROOT, "2.parquet", false, null, null);
      prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
          PARQUET_SOURCE_ROOT, false, "partition_path", "0");
      HoodieDeltaStreamer deltaStreamer1 = new HoodieDeltaStreamer(cfg, jsc);
      deltaStreamer1.sync();
      // since we mimic'ed empty batch, total records should be same as first sync().
      assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
      HoodieTableMetaClient metaClient = createMetaClient(jsc, tableBasePath);

      // validate table schema fetches valid schema from last but one commit.
      TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
      assertNotEquals(tableSchemaResolver.getTableSchema(), Schema.create(Schema.Type.NULL).toString());
      // schema from latest commit and last but one commit should match
      compareLatestTwoSchemas(metaClient);
      prepareParquetDFSSource(useSchemaProvider, hasTransformer, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
          PARQUET_SOURCE_ROOT, false, "partition_path", "");
      deltaStreamer1.shutdownGracefully();
    }

    // proceed w/ non empty batch.
    prepareParquetDFSFiles(100, PARQUET_SOURCE_ROOT, "3.parquet", false, null, null);
    deltaStreamer.sync();
    assertRecordCount(parquetRecordsCount + 100, tableBasePath, sqlContext);
    // validate commit metadata for all completed commits to have valid schema in extra metadata.
    HoodieTableMetaClient metaClient = createMetaClient(jsc, tableBasePath);
    metaClient.reloadActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants()
        .forEach(entry -> assertValidSchemaAndOperationTypeInCommitMetadata(
            entry, metaClient, WriteOperationType.INSERT));
    testNum++;
    deltaStreamer.shutdownGracefully();
  }

  private void assertValidSchemaAndOperationTypeInCommitMetadata(HoodieInstant instant,
                                                                 HoodieTableMetaClient metaClient,
                                                                 WriteOperationType operationType) {
    try {
      HoodieCommitMetadata commitMetadata =
          metaClient.getActiveTimeline().readCommitMetadata(instant);
      assertFalse(StringUtils.isNullOrEmpty(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY)));
      assertEquals(operationType, commitMetadata.getOperationType());
    } catch (IOException ioException) {
      throw new HoodieException("Failed to parse commit metadata for " + instant.toString());
    }
  }

  private void compareLatestTwoSchemas(HoodieTableMetaClient metaClient) throws IOException {
    // schema from latest commit and last but one commit should match
    List<HoodieInstant> completedInstants = metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().getInstants();
    HoodieCommitMetadata commitMetadata1 = TimelineUtils.getCommitMetadata(completedInstants.get(0), metaClient.getActiveTimeline());
    HoodieCommitMetadata commitMetadata2 = TimelineUtils.getCommitMetadata(completedInstants.get(1), metaClient.getActiveTimeline());
    assertEquals(commitMetadata1.getMetadata(HoodieCommitMetadata.SCHEMA_KEY), commitMetadata2.getMetadata(HoodieCommitMetadata.SCHEMA_KEY));
  }

  private void testORCDFSSource(boolean useSchemaProvider, List<String> transformerClassNames) throws Exception {
    // prepare ORCDFSSource
    prepareORCDFSFiles(ORC_NUM_RECORDS, ORC_SOURCE_ROOT);
    TypedProperties orcProps = new TypedProperties();

    // Properties used for testing delta-streamer with orc source
    orcProps.setProperty("include", "base.properties");
    orcProps.setProperty("hoodie.embed.timeline.server", "false");
    orcProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    orcProps.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    if (useSchemaProvider) {
      orcProps.setProperty("hoodie.streamer.schemaprovider.source.schema.file", basePath + "/" + "source.avsc");
      if (transformerClassNames != null) {
        orcProps.setProperty("hoodie.streamer.schemaprovider.target.schema.file", basePath + "/" + "target.avsc");
      }
    }
    orcProps.setProperty("hoodie.streamer.source.dfs.root", ORC_SOURCE_ROOT);
    UtilitiesTestBase.Helpers.savePropsToDFS(orcProps, storage, basePath + "/" + PROPS_FILENAME_TEST_ORC);

    String tableBasePath = basePath + "/test_orc_source_table" + testNum;
    syncOnce(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT,
            ORCDFSSource.class.getName(),
            transformerClassNames, PROPS_FILENAME_TEST_ORC, false,
            useSchemaProvider, 100000, false, null, null, "timestamp", null));
    assertRecordCount(ORC_NUM_RECORDS, tableBasePath, sqlContext);
    testNum++;
  }

  /**
   * Tests Deltastreamer with parquet dfs source and transitions to JsonKafkaSource.
   *
   * @param autoResetToLatest true if auto reset value to be set to LATEST. false to leave it as default(i.e. EARLIEST)
   * @throws Exception
   */
  private void testDeltaStreamerTransitionFromParquetToKafkaSource(boolean autoResetToLatest) throws Exception {
    // prep parquet source
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfsToKafka" + testNum;
    int parquetRecords = 10;
    prepareParquetDFSFiles(parquetRecords, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, true, HoodieTestDataGenerator.TRIP_SCHEMA, HoodieTestDataGenerator.AVRO_TRIP_SCHEMA);

    prepareParquetDFSSource(true, true, "source_uber.avsc", "target_uber.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "driver");
    // delta streamer w/ parquet source
    String tableBasePath = basePath + "/test_dfs_to_kafka" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
            Collections.emptyList(), PROPS_FILENAME_TEST_PARQUET, false,
            true, 100000, false, null, null, "timestamp", null), jsc);
    deltaStreamer.sync();
    assertRecordCount(parquetRecords, tableBasePath, sqlContext);
    deltaStreamer.shutdownGracefully();

    // prep json kafka source
    topicName = "topic" + testNum;
    prepareJsonKafkaDFSFiles(JSON_KAFKA_NUM_RECORDS, true, topicName);
    prepareJsonKafkaDFSSource(PROPS_FILENAME_TEST_JSON_KAFKA, autoResetToLatest ? "latest" : "earliest", topicName);
    // delta streamer w/ json kafka source
    deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
            Collections.emptyList(), PROPS_FILENAME_TEST_JSON_KAFKA, false,
            true, 100000, false, null, null, "timestamp", null), jsc);
    deltaStreamer.sync();
    // if auto reset value is set to LATEST, this all kafka records so far may not be synced.
    int totalExpectedRecords = parquetRecords + ((autoResetToLatest) ? 0 : JSON_KAFKA_NUM_RECORDS);
    assertRecordCount(totalExpectedRecords, tableBasePath, sqlContext);

    // verify 2nd batch to test LATEST auto reset value.
    prepareJsonKafkaDFSFiles(20, false, topicName);
    totalExpectedRecords += 20;
    deltaStreamer.sync();
    assertRecordCount(totalExpectedRecords, tableBasePath, sqlContext);
    testNum++;
    deltaStreamer.shutdownGracefully();
  }

  @Test
  public void testJsonKafkaDFSSource() throws Exception {
    topicName = "topic" + testNum;
    prepareJsonKafkaDFSFiles(JSON_KAFKA_NUM_RECORDS, true, topicName);
    prepareJsonKafkaDFSSource(PROPS_FILENAME_TEST_JSON_KAFKA, "earliest", topicName);
    String tableBasePath = basePath + "/test_json_kafka_table" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
            Collections.emptyList(), PROPS_FILENAME_TEST_JSON_KAFKA, false,
            true, 100000, false, null, null, "timestamp", null), jsc);
    deltaStreamer.sync();
    assertRecordCount(JSON_KAFKA_NUM_RECORDS, tableBasePath, sqlContext);

    int totalRecords = JSON_KAFKA_NUM_RECORDS;
    int records = 10;
    totalRecords += records;
    prepareJsonKafkaDFSFiles(records, false, topicName);
    deltaStreamer.sync();
    assertRecordCount(totalRecords, tableBasePath, sqlContext);
    deltaStreamer.shutdownGracefully();
  }

  @Test
  public void testJsonKafkaDFSSourceWithOffsets() throws Exception {
    topicName = "topic" + testNum;
    int numRecords = 30;
    int numPartitions = 2;
    int recsPerPartition = numRecords / numPartitions;
    long beforeTime = Instant.now().toEpochMilli();
    prepareJsonKafkaDFSFiles(numRecords, true, topicName, numPartitions);
    prepareJsonKafkaDFSSource(PROPS_FILENAME_TEST_JSON_KAFKA, "earliest", topicName, null, true);
    String tableBasePath = basePath + "/test_json_kafka_offsets_table" + testNum;
    syncOnce(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
            Collections.emptyList(), PROPS_FILENAME_TEST_JSON_KAFKA, false,
            true, 100000, false, null, null, "timestamp", null));
    sqlContext.clearCache();
    Dataset<Row> ds = sqlContext.read().format("org.apache.hudi").load(tableBasePath);
    assertEquals(numRecords, ds.count());
    //ensure that kafka partition column exists and is populated correctly
    for (int i = 0; i < numPartitions; i++) {
      assertEquals(recsPerPartition, ds.filter(KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN + "=" + i).count());
    }

    //ensure that kafka timestamp column exists and is populated correctly
    long afterTime = Instant.now().toEpochMilli();
    assertEquals(numRecords, ds.filter(KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN + ">" + beforeTime)
        .filter(KafkaOffsetPostProcessor.KAFKA_SOURCE_TIMESTAMP_COLUMN + "<" + afterTime).count());


    //ensure that kafka offset column exists and is populated correctly
    sqlContext.read().format("org.apache.hudi").load(tableBasePath).col(KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN);
    for (int i = 0; i < recsPerPartition; i++) {
      for (int j = 0; j < numPartitions; j++) {
        //each offset partition pair should be unique
        assertEquals(1, ds.filter(KafkaOffsetPostProcessor.KAFKA_SOURCE_OFFSET_COLUMN + "=" + i)
            .filter(KafkaOffsetPostProcessor.KAFKA_SOURCE_PARTITION_COLUMN + "=" + j).count());
      }
    }
  }

  @Test
  public void testKafkaTimestampType() throws Exception {
    // Timestamp-based Kafka checkpoints have two distinct fallback behaviors we need to cover:
    //   (1) Checkpoint captured BEFORE records are produced: every record has ts >= checkpoint,
    //       so `offsetsForTimes` returns concrete offsets and ingestion consumes all of them.
    //   (2) Checkpoint captured AFTER records are produced: no record has ts >= checkpoint, so
    //       `offsetsForTimes` returns null for every partition and we fall back to the end offset
    //       of each partition. Nothing should be ingested, and a subsequent batch produced *after*
    //       the checkpoint should be picked up on the next sync — this proves that the fallback
    //       stored a usable checkpoint at the partition tip (not offset 0, which would replay the
    //       original records).
    kafkaCheckpointType = "timestamp";

    // ---- Case 1: checkpoint captured BEFORE producing records ----
    long checkpointBeforeProduction = System.currentTimeMillis();
    prepareJsonKafkaDFSFiles(JSON_KAFKA_NUM_RECORDS, true, "topic" + testNum);
    prepareJsonKafkaDFSSource(PROPS_FILENAME_TEST_JSON_KAFKA, "earliest", "topic" + testNum);
    String tableBasePath1 = basePath + "/test_json_kafka_table" + testNum;
    syncOnce(TestHelpers.makeConfig(tableBasePath1, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
        Collections.emptyList(), PROPS_FILENAME_TEST_JSON_KAFKA, false,
        true, 100000, false, null,
        null, "timestamp", String.valueOf(checkpointBeforeProduction)));
    assertRecordCount(JSON_KAFKA_NUM_RECORDS, tableBasePath1, sqlContext);

    // ---- Case 2: checkpoint captured AFTER producing records ----
    // First batch predates the checkpoint => fallback path returns end offsets (partition tips).
    // Nothing should be ingested in the first sync; a second batch produced after the checkpoint
    // should be fully consumed on the follow-up sync (which reuses the checkpoint stored by the
    // first sync). This asserts we resumed at the tip, not at offset 0.
    String topicName2 = "topic_after_" + testNum;
    prepareJsonKafkaDFSFiles(JSON_KAFKA_NUM_RECORDS, true, topicName2);
    // Small pause so the timestamp is guaranteed to be after the last produced record's ts.
    Thread.sleep(10);
    long checkpointAfterProduction = System.currentTimeMillis();
    prepareJsonKafkaDFSSource(PROPS_FILENAME_TEST_JSON_KAFKA, "earliest", topicName2);
    String tableBasePath2 = basePath + "/test_json_kafka_table_after_" + testNum;
    syncOnce(TestHelpers.makeConfig(tableBasePath2, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
        Collections.emptyList(), PROPS_FILENAME_TEST_JSON_KAFKA, false,
        true, 100000, false, null, null,
        "timestamp", String.valueOf(checkpointAfterProduction)));
    assertRecordCount(0, tableBasePath2, sqlContext);

    // Produce a fresh batch strictly after the checkpoint and sync again with no --checkpoint
    // override, so the streamer picks up from the offsets we stored in the first sync.
    prepareJsonKafkaDFSFiles(JSON_KAFKA_NUM_RECORDS, false, topicName2);
    syncOnce(TestHelpers.makeConfig(tableBasePath2, WriteOperationType.UPSERT, JsonKafkaSource.class.getName(),
        Collections.emptyList(), PROPS_FILENAME_TEST_JSON_KAFKA, false,
        true, 100000, false, null, null,
        "timestamp", null));
    // Only the second batch should be ingested; the first batch (which predates the checkpoint)
    // stays skipped, confirming the fallback resumed at the partition tip.
    assertRecordCount(JSON_KAFKA_NUM_RECORDS, tableBasePath2, sqlContext);
  }

  @Test
  public void testParquetSourceToKafkaSourceEarliestAutoResetValue() throws Exception {
    testDeltaStreamerTransitionFromParquetToKafkaSource(false);
  }

  @Test
  public void testParquetSourceToKafkaSourceLatestAutoResetValue() throws Exception {
    testDeltaStreamerTransitionFromParquetToKafkaSource(true);
  }

  @Test
  public void testParquetDFSSourceWithoutSchemaProviderAndNoTransformer() throws Exception {
    testParquetDFSSource(false, null);
  }

  @Test
  public void testParquetDFSSourceForEmptyBatch() throws Exception {
    testParquetDFSSource(false, null, true);
  }

  @Test
  public void testParquetDFSSourceWithoutSchemaProviderAndTransformer() throws Exception {
    testParquetDFSSource(false, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Test
  public void testParquetDFSSourceWithSourceSchemaFileAndNoTransformer() throws Exception {
    testParquetDFSSource(true, null);
  }

  @Test
  public void testParquetDFSSourceWithSchemaFilesAndTransformer() throws Exception {
    testParquetDFSSource(true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Disabled("HUDI-8081")
  @Test
  public void testORCDFSSourceWithoutSchemaProviderAndNoTransformer() throws Exception {
    testORCDFSSource(false, null);
  }

  @Disabled("HUDI-8081")
  @Test
  public void testORCDFSSourceWithSchemaProviderAndWithTransformer() throws Exception {
    testORCDFSSource(true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  private void prepareCsvDFSSource(
      boolean hasHeader, char sep, boolean useSchemaProvider, boolean hasTransformer) throws IOException {
    String sourceRoot = basePath + "/csvFiles";
    String recordKeyField = (hasHeader || useSchemaProvider) ? "_row_key" : "_c1";
    String partitionPath = (hasHeader || useSchemaProvider) ? "partition_path" : "_c2";

    // Properties used for testing delta-streamer with CSV source
    TypedProperties csvProps = new TypedProperties();
    csvProps.setProperty("include", "base.properties");
    csvProps.setProperty("hoodie.datasource.write.recordkey.field", recordKeyField);
    csvProps.setProperty("hoodie.datasource.write.partitionpath.field", partitionPath);
    if (useSchemaProvider) {
      csvProps.setProperty("hoodie.streamer.schemaprovider.source.schema.file", basePath + "/source-flattened.avsc");
      if (hasTransformer) {
        csvProps.setProperty("hoodie.streamer.schemaprovider.target.schema.file", basePath + "/target-flattened.avsc");
      }
    }
    csvProps.setProperty("hoodie.streamer.source.dfs.root", sourceRoot);

    if (sep != ',') {
      if (sep == '\t') {
        csvProps.setProperty("hoodie.streamer.csv.sep", "\\t");
      } else {
        csvProps.setProperty("hoodie.streamer.csv.sep", Character.toString(sep));
      }
    }
    if (hasHeader) {
      csvProps.setProperty("hoodie.streamer.csv.header", Boolean.toString(hasHeader));
    }

    UtilitiesTestBase.Helpers.savePropsToDFS(csvProps, storage,
        basePath + "/" + PROPS_FILENAME_TEST_CSV);

    String path = sourceRoot + "/1.csv";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    UtilitiesTestBase.Helpers.saveCsvToDFS(
        hasHeader, sep,
        Helpers.jsonifyRecords(dataGenerator.generateInserts("000", CSV_NUM_RECORDS, true)),
        fs, path);
  }

  private void testCsvDFSSource(
      boolean hasHeader, char sep, boolean useSchemaProvider, List<String> transformerClassNames) throws Exception {
    prepareCsvDFSSource(hasHeader, sep, useSchemaProvider, transformerClassNames != null);
    String tableBasePath = basePath + "/test_csv_table" + testNum;
    String sourceOrderingField = (hasHeader || useSchemaProvider) ? "timestamp" : "_c0";
    syncOnce(TestHelpers.makeConfig(
        tableBasePath, WriteOperationType.INSERT, CsvDFSSource.class.getName(),
        transformerClassNames, PROPS_FILENAME_TEST_CSV, false,
        useSchemaProvider, 1000, false, null, null, sourceOrderingField, null));
    assertRecordCount(CSV_NUM_RECORDS, tableBasePath, sqlContext);
    testNum++;
  }

  @Test
  public void testCsvDFSSourceWithHeaderWithoutSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files have header, the columns are separated by ',', the default separator
    // No schema provider is specified, no transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files
    testCsvDFSSource(true, ',', false, null);
  }

  @Test
  public void testCsvDFSSourceWithHeaderAndSepWithoutSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files have header, the columns are separated by '\t',
    // which is passed in through the Hudi CSV properties
    // No schema provider is specified, no transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files
    testCsvDFSSource(true, '\t', false, null);
  }

  @Test
  public void testCsvDFSSourceWithHeaderAndSepWithSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files have header, the columns are separated by '\t'
    // File schema provider is used, no transformer is applied
    // In this case, the source schema comes from the source Avro schema file
    testCsvDFSSource(true, '\t', true, null);
  }

  @Test
  public void testCsvDFSSourceWithHeaderAndSepWithoutSchemaProviderAndWithTransformer() throws Exception {
    // The CSV files have header, the columns are separated by '\t'
    // No schema provider is specified, transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files.
    // Target schema is determined based on the Dataframe after transformation
    testCsvDFSSource(true, '\t', false, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Test
  public void testCsvDFSSourceWithHeaderAndSepWithSchemaProviderAndTransformer() throws Exception {
    // The CSV files have header, the columns are separated by '\t'
    // File schema provider is used, transformer is applied
    // In this case, the source and target schema come from the Avro schema files
    testCsvDFSSource(true, '\t', true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  @Test
  public void testCsvDFSSourceNoHeaderWithoutSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files do not have header, the columns are separated by '\t',
    // which is passed in through the Hudi CSV properties
    // No schema provider is specified, no transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files
    // No CSV header and no schema provider at the same time are not recommended
    // as the column names are not informative
    testCsvDFSSource(false, '\t', false, null);
  }

  @Test
  public void testCsvDFSSourceNoHeaderWithSchemaProviderAndNoTransformer() throws Exception {
    // The CSV files do not have header, the columns are separated by '\t'
    // File schema provider is used, no transformer is applied
    // In this case, the source schema comes from the source Avro schema file
    testCsvDFSSource(false, '\t', true, null);
  }

  @Test
  public void testCsvDFSSourceNoHeaderWithoutSchemaProviderAndWithTransformer() throws Exception {
    // The CSV files do not have header, the columns are separated by '\t'
    // No schema provider is specified, transformer is applied
    // In this case, the source schema comes from the inferred schema of the CSV files.
    // Target schema is determined based on the Dataframe after transformation
    // No CSV header and no schema provider at the same time are not recommended,
    // as the transformer behavior may be unexpected
    Exception e = assertThrows(HoodieIngestionException.class, () -> {
      testCsvDFSSource(false, '\t', false, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
    }, "Should error out when doing the transformation.");
    log.debug("Expected error during transformation", e);
    Throwable cause = e.getCause();
    assertTrue(cause instanceof AnalysisException, "Expected cause to be AnalysisException but was: " + cause.getClass());
    // First message for Spark 3.4 and above, second message for Spark 3.3, third message for Spark 3.2 and below
    assertTrue(
        cause.getMessage().contains("[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column or function parameter "
            + "with name `begin_lat` cannot be resolved. Did you mean one of the following?")
            || cause.getMessage().contains("Column 'begin_lat' does not exist. Did you mean one of the following?")
            || cause.getMessage().contains("cannot resolve 'begin_lat' given input columns:"));
  }

  @Test
  public void testCsvDFSSourceNoHeaderWithSchemaProviderAndTransformer() throws Exception {
    // The CSV files do not have header, the columns are separated by '\t'
    // File schema provider is used, transformer is applied
    // In this case, the source and target schema come from the Avro schema files
    testCsvDFSSource(false, '\t', true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()));
  }

  private void prepareSqlSource() throws IOException {
    String sourceRoot = basePath + "sqlSourceFiles";
    TypedProperties sqlSourceProps = new TypedProperties();
    sqlSourceProps.setProperty("include", "base.properties");
    sqlSourceProps.setProperty("hoodie.embed.timeline.server", "false");
    sqlSourceProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
    sqlSourceProps.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
    sqlSourceProps.setProperty("hoodie.streamer.source.sql.sql.query", "select * from test_sql_table");

    UtilitiesTestBase.Helpers.savePropsToDFS(sqlSourceProps, storage,
        basePath + "/" + PROPS_FILENAME_TEST_SQL_SOURCE);

    // Data generation
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    generateSqlSourceTestTable(sourceRoot, "1", "1000", SQL_SOURCE_NUM_RECORDS, dataGenerator);
  }

  private void generateSqlSourceTestTable(String dfsRoot, String filename, String instantTime, int n, HoodieTestDataGenerator dataGenerator) throws IOException {
    Path path = new Path(dfsRoot, filename);
    Helpers.saveParquetToDFS(Helpers.toGenericRecords(dataGenerator.generateInserts(instantTime, n, false)), path);
    sparkSession.read().parquet(dfsRoot).createOrReplaceTempView("test_sql_table");
  }

  @Test
  public void testSqlSourceSource() throws Exception {
    prepareSqlSource();
    String tableBasePath = basePath + "/test_sql_source_table" + testNum++;
    HoodieDeltaStreamer deltaStreamer =
        new HoodieDeltaStreamer(TestHelpers.makeConfig(
            tableBasePath, WriteOperationType.UPSERT, SqlSource.class.getName(),
            Collections.emptyList(), PROPS_FILENAME_TEST_SQL_SOURCE, false,
            false, 2000, false, null, null, "timestamp", null, true), jsc);
    deltaStreamer.sync();
    assertRecordCount(SQL_SOURCE_NUM_RECORDS, tableBasePath, sqlContext);
    // Data generation
    String sourceRoot = basePath + "sqlSourceFiles";
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    generateSqlSourceTestTable(sourceRoot, "2", "1000", SQL_SOURCE_NUM_RECORDS, dataGenerator);

    deltaStreamer.sync();
    assertRecordCount(SQL_SOURCE_NUM_RECORDS * 2, tableBasePath, sqlContext);
    deltaStreamer.shutdownGracefully();
  }

  @Test
  public void testHoodieIncrFallback() throws Exception {
    String tableBasePath = basePath + "/incr_test_table";
    String downstreamTableBasePath = basePath + "/incr_test_downstream_table";

    insertInTable(tableBasePath, 1, WriteOperationType.BULK_INSERT);
    HoodieDeltaStreamer.Config downstreamCfg =
        TestHelpers.makeConfigForHudiIncrSrc(tableBasePath, downstreamTableBasePath,
            WriteOperationType.BULK_INSERT, true, null);
    downstreamCfg.configs.add("hoodie.streamer.source.hoodieincr.num_instants=1");
    syncOnce(downstreamCfg);

    insertInTable(tableBasePath, 9, WriteOperationType.UPSERT);
    assertRecordCount(1000, downstreamTableBasePath, sqlContext);

    if (downstreamCfg.configs == null) {
      downstreamCfg.configs = new ArrayList<>();
    }

    // Remove source.hoodieincr.num_instants config
    downstreamCfg.configs.remove(downstreamCfg.configs.size() - 1);
    downstreamCfg.configs.add(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN().key() + "=true");
    //Adding this conf to make testing easier :)
    downstreamCfg.configs.add("hoodie.streamer.source.hoodieincr.num_instants=10");
    downstreamCfg.operation = WriteOperationType.UPSERT;
    syncOnce(downstreamCfg);
    syncOnce(downstreamCfg);

    long baseTableRecords = sqlContext.read().format("org.apache.hudi").load(tableBasePath).count();
    long downStreamTableRecords = sqlContext.read().format("org.apache.hudi").load(downstreamTableBasePath).count();
    assertEquals(baseTableRecords, downStreamTableRecords);
  }

  private void insertInTable(String tableBasePath, int count, WriteOperationType operationType) throws Exception {
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, operationType,
        Collections.singletonList(SqlQueryBasedTransformer.class.getName()), PROPS_FILENAME_TEST_SOURCE, false);
    if (cfg.configs == null) {
      cfg.configs = new ArrayList<>();
    }
    cfg.configs.add("hoodie.clean.commits.retained=2");
    cfg.configs.add("hoodie.keep.min.commits=4");
    cfg.configs.add("hoodie.keep.max.commits=5");
    cfg.configs.add("hoodie.test.source.generate.inserts=true");

    for (int i = 0; i < count; i++) {
      syncOnce(cfg);
    }
  }

  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO", "SPARK"})
  public void testInsertOverwrite(HoodieRecordType recordType) throws Exception {
    testDeltaStreamerWithSpecifiedOperation(basePath + "/insert_overwrite", WriteOperationType.INSERT_OVERWRITE, recordType);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO", "SPARK"})
  public void testInsertOverwriteTable(HoodieRecordType recordType) throws Exception {
    testDeltaStreamerWithSpecifiedOperation(basePath + "/insert_overwrite_table", WriteOperationType.INSERT_OVERWRITE_TABLE, recordType);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieRecordType.class, names = {"AVRO", "SPARK"})
  public void testDelete(HoodieRecordType recordType) throws Exception {
    testDeltaStreamerWithSpecifiedOperation(basePath + "/delete", WriteOperationType.DELETE, recordType);
  }

  @Test
  public void testDeletePartitions() throws Exception {
    prepareParquetDFSFiles(PARQUET_NUM_RECORDS, PARQUET_SOURCE_ROOT);
    prepareParquetDFSSource(false, false, "source.avsc", "target.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path");
    String tableBasePath = basePath + "test_parquet_table" + testNum;

    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
            null, PROPS_FILENAME_TEST_PARQUET, false,
            false, 100000, false, null, null, "timestamp", null), jsc);
    deltaStreamer.sync();
    // There should be fileIDs in the partition being deleted
    assertFalse(getAllFileIDsInTable(tableBasePath, Option.of(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).isEmpty());

    assertRecordCount(PARQUET_NUM_RECORDS, tableBasePath, sqlContext);
    testNum++;

    prepareParquetDFSFiles(PARQUET_NUM_RECORDS, PARQUET_SOURCE_ROOT);
    prepareParquetDFSSource(false, false);
    // set write operation to DELETE_PARTITION and add transformer to filter only for records with partition HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION
    deltaStreamer = new HoodieDeltaStreamer(
        TestHelpers.makeConfig(tableBasePath, WriteOperationType.DELETE_PARTITION, ParquetDFSSource.class.getName(),
            Collections.singletonList(TestSpecificPartitionTransformer.class.getName()), PROPS_FILENAME_TEST_PARQUET, false,
            false, 100000, false, null, null, "timestamp", null), jsc);
    deltaStreamer.sync();
    // No records should match the HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION.
    assertNoPartitionMatch(tableBasePath, sqlContext, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);

    // There should not be any fileIDs in the deleted partition
    assertTrue(getAllFileIDsInTable(tableBasePath, Option.of(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH)).isEmpty());
    deltaStreamer.shutdownGracefully();
  }

  void testDeltaStreamerWithSpecifiedOperation(final String tableBasePath, WriteOperationType operationType, HoodieRecordType recordType) throws Exception {
    // Initial insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT);
    addRecordMerger(recordType, cfg.configs);
    new HoodieDeltaStreamer(cfg, jsc).sync();
    assertRecordCount(1000, tableBasePath, sqlContext);
    assertDistanceCount(1000, tableBasePath, sqlContext);
    TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

    // Collect the fileIds before running HoodieDeltaStreamer
    Set<String> beforeFileIDs = getAllFileIDsInTable(tableBasePath, Option.empty());

    // setting the operationType
    cfg.operation = operationType;
    // No new data => no commits.
    cfg.sourceLimit = 0;
    syncOnce(cfg);

    if (operationType == WriteOperationType.INSERT_OVERWRITE) {
      assertRecordCount(1000, tableBasePath, sqlContext);
      assertDistanceCount(1000, tableBasePath, sqlContext);
      TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);
    } else if (operationType == WriteOperationType.INSERT_OVERWRITE_TABLE) {
      HoodieTableMetaClient metaClient = createMetaClient(jsc, tableBasePath);
      final HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, metaClient.getCommitsAndCompactionTimeline());
      assertEquals(0, fsView.getLatestFileSlices("").count());
      TestHelpers.assertCommitMetadata("00000", tableBasePath, 1);

      // Since the table has been overwritten all fileIDs before should have been replaced
      Set<String> afterFileIDs = getAllFileIDsInTable(tableBasePath, Option.empty());
      assertTrue(afterFileIDs.isEmpty());
    }

    cfg.sourceLimit = 1000;
    syncOnce(cfg);
    if (operationType == WriteOperationType.DELETE) {
      // Test Records Deleted
      assertRecordCount(500, tableBasePath, sqlContext);
      TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
    } else {
      assertRecordCount(950, tableBasePath, sqlContext);
      assertDistanceCount(950, tableBasePath, sqlContext);
      TestHelpers.assertCommitMetadata("00001", tableBasePath, 2);
    }
    UtilitiesTestBase.Helpers.deleteFileFromDfs(fs, tableBasePath);
  }

  @Test
  public void testBulkInsertWithUserDefinedPartitioner() throws Exception {
    String tableBasePath = basePath + "/test_table_bulk_insert";
    String sortColumn = "weight";
    TypedProperties bulkInsertProps =
        new DFSPropertiesConfiguration(fs.getConf(), new StoragePath(basePath + "/" + PROPS_FILENAME_TEST_SOURCE)).getProps();
    bulkInsertProps.setProperty("hoodie.bulkinsert.shuffle.parallelism", "1");
    bulkInsertProps.setProperty("hoodie.bulkinsert.user.defined.partitioner.class", "org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner");
    bulkInsertProps.setProperty("hoodie.bulkinsert.user.defined.partitioner.sort.columns", sortColumn);
    String bulkInsertPropsFileName = "bulk_insert_override.properties";
    UtilitiesTestBase.Helpers.savePropsToDFS(bulkInsertProps, storage, basePath + "/" + bulkInsertPropsFileName);
    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()), bulkInsertPropsFileName, false);
    syncAndAssertRecordCount(cfg, 1000, tableBasePath, "00000", 1);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(tableBasePath).setConf(HoodieTestUtils.getDefaultStorageConf()).build();
    List<String> partitions = FSUtils.getAllPartitionPaths(new HoodieLocalEngineContext(metaClient.getStorageConf()), metaClient, false);
    StorageConfiguration hadoopConf = metaClient.getStorageConf();
    HoodieLocalEngineContext engContext = new HoodieLocalEngineContext(hadoopConf);
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(engContext, metaClient,
        metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants());
    List<String> baseFiles = partitions.parallelStream().flatMap(partition -> fsView.getLatestBaseFiles(partition).map(HoodieBaseFile::getPath)).collect(Collectors.toList());
    // Verify each partition has one base file because parallelism is 1.
    assertEquals(baseFiles.size(), partitions.size());
    // Verify if each parquet file is actually sorted by sortColumn.
    for (String filePath : baseFiles) {
      try (HoodieAvroParquetReader parquetReader = new HoodieAvroParquetReader(HoodieTestUtils.getStorage(filePath), new StoragePath(filePath))) {
        ClosableIterator<HoodieRecord<IndexedRecord>> iterator = parquetReader.getRecordIterator();
        List<Float> sortColumnValues = new ArrayList<>();
        while (iterator.hasNext()) {
          IndexedRecord indexedRecord = iterator.next().getData();
          List<Schema.Field> fields = indexedRecord.getSchema().getFields();
          for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).name().equals(sortColumn)) {
              sortColumnValues.add((Float) indexedRecord.get(i));
            }
          }
        }
        // Assert whether records read are same as the sorted records.
        List<Float> actualSortColumnValues = new ArrayList<>(sortColumnValues);
        Collections.sort(sortColumnValues);
        assertEquals(sortColumnValues, actualSortColumnValues);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testBulkInsertSkewedSortColumns(boolean suffixRecordKey) throws Exception {
    String tableBasePath = basePath + "/test_table_bulk_insert_skewed_sort_columns_" + suffixRecordKey;
    int outputParallelism = 100;
    int columnCardinality = 2;
    // This column has 2 values [BLACK, UBERX]
    String sortColumn = "trip_type";
    TypedProperties bulkInsertProps =
        new DFSPropertiesConfiguration(fs.getConf(), new StoragePath(basePath + "/" + PROPS_FILENAME_TEST_SOURCE)).getProps();
    bulkInsertProps.setProperty(HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS.key(), String.valueOf(suffixRecordKey));
    bulkInsertProps.setProperty("hoodie.bulkinsert.shuffle.parallelism", String.valueOf(outputParallelism));
    bulkInsertProps.setProperty("hoodie.datasource.write.partitionpath.field", "");
    bulkInsertProps.setProperty("hoodie.datasource.write.keygenerator.class", NonpartitionedKeyGenerator.class.getName());
    bulkInsertProps.setProperty("hoodie.bulkinsert.user.defined.partitioner.class", "org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner");
    bulkInsertProps.setProperty("hoodie.bulkinsert.user.defined.partitioner.sort.columns", sortColumn);
    String bulkInsertPropsFileName = "bulk_insert_override.properties";
    UtilitiesTestBase.Helpers.savePropsToDFS(bulkInsertProps, storage, basePath + "/" + bulkInsertPropsFileName);
    // Initial bulk insert
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.BULK_INSERT,
        Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()), bulkInsertPropsFileName, false);
    syncAndAssertRecordCount(cfg, 1000, tableBasePath, "00000", 1);

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(tableBasePath).setConf(HoodieTestUtils.getDefaultStorageConf()).build();
    StorageConfiguration hadoopConf = metaClient.getStorageConf();
    HoodieLocalEngineContext engContext = new HoodieLocalEngineContext(hadoopConf);
    HoodieTableFileSystemView fsView =
        FileSystemViewManager.createInMemoryFileSystemView(engContext, metaClient, HoodieMetadataConfig.newBuilder().enable(false).build());
    List<String> baseFiles = fsView.getLatestBaseFiles("").map(HoodieBaseFile::getPath).collect(Collectors.toList());
    if (suffixRecordKey) {
      assertEquals(baseFiles.size(), outputParallelism);
    } else {
      assertEquals(baseFiles.size(), columnCardinality);
    }
  }

  @ParameterizedTest
  @MethodSource("generateErrorTablePersistSourceRddArgs")
  void testErrorTableSourcePersist(WriteOperationType writeOperationType, boolean persistSourceRdd) throws Exception {
    String tableBasePath = basePath + "/test_table_error_table" + persistSourceRdd + writeOperationType;
    TypedProperties tableProps =
        new DFSPropertiesConfiguration(fs.getConf(), new StoragePath(basePath + "/" + PROPS_FILENAME_TEST_SOURCE)).getProps();
    tableProps.setProperty(ERROR_TABLE_PERSIST_SOURCE_RDD.key(), String.valueOf(persistSourceRdd));
    switch (writeOperationType) {
      case BULK_INSERT:
        tableProps.setProperty("hoodie.datasource.write.partitionpath.field", "");
        tableProps.setProperty("hoodie.datasource.write.keygenerator.class", NonpartitionedKeyGenerator.class.getName());
        tableProps.setProperty("hoodie.bulkinsert.sort.mode", BulkInsertSortMode.GLOBAL_SORT.name());
        break;
      case UPSERT:
        tableProps.setProperty("hoodie.datasource.write.recordkey.field", "_row_key");
        tableProps.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
        break;
      case INSERT:
        tableProps.setProperty("hoodie.datasource.write.partitionpath.field", "partition_path");
        break;
      default:
        throw new UnsupportedOperationException("Invalid write operationType " + writeOperationType);
    }
    String tablePropsFileName = "table_specific.properties";
    UtilitiesTestBase.Helpers.savePropsToDFS(tableProps, storage, basePath + "/" + tablePropsFileName);
    // Initialize table config.
    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, writeOperationType,
        Collections.singletonList(TestHoodieDeltaStreamer.TripsWithDistanceTransformer.class.getName()), tablePropsFileName, false);
    HoodieStreamer deltaStreamer = new HoodieStreamer(cfg, jsc);
    HoodieStreamer.StreamSyncService streamSyncService = (HoodieStreamer.StreamSyncService) deltaStreamer.getIngestionService();
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(HoodieTestUtils.getDefaultStorageConf()).setBasePath(tableBasePath).build();
    InputBatch inputBatch = streamSyncService.getStreamSync().readFromSource(metaClient).getLeft();
    // Read from source and validate persistRdd call.
    JavaRDD<GenericRecord> sourceRdd = (JavaRDD<GenericRecord>) inputBatch.getBatch().get();
    assertEquals(1000, sourceRdd.count());
    if (persistSourceRdd) {
      assertTrue(sourceRdd.toDebugString().contains("CachedPartitions"));
    } else {
      assertFalse(sourceRdd.toDebugString().contains("CachedPartitions"));
    }
    streamSyncService.close();
    // Ingest data.
    streamSyncService.ingestOnce();
    assertRecordCount(950, tableBasePath, sqlContext);
  }

  private Set<String> getAllFileIDsInTable(String tableBasePath, Option<String> partition) {
    HoodieTableMetaClient metaClient = createMetaClient(jsc, tableBasePath);
    final HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(context, metaClient, metaClient.getCommitsAndCompactionTimeline());
    Stream<HoodieBaseFile> baseFileStream = partition.isPresent() ? fsView.getLatestBaseFiles(partition.get()) : fsView.getLatestBaseFiles();
    return baseFileStream.map(HoodieBaseFile::getFileId).collect(Collectors.toSet());
  }

  private static Stream<Arguments> testORCDFSSource() {
    // arg1 boolean useSchemaProvider, arg2 List<String> transformerClassNames
    return Stream.of(
        arguments(false, null),
        arguments(true, Collections.singletonList(TripsWithDistanceTransformer.class.getName()))
    );
  }

  private static Stream<Arguments> generateErrorTablePersistSourceRddArgs() {
    return Stream.of(
        Arguments.of(WriteOperationType.BULK_INSERT, false),
        Arguments.of(WriteOperationType.BULK_INSERT, true),
        Arguments.of(WriteOperationType.INSERT, false),
        Arguments.of(WriteOperationType.INSERT, true),
        Arguments.of(WriteOperationType.UPSERT, false),
        Arguments.of(WriteOperationType.UPSERT, true)
    );
  }
}
