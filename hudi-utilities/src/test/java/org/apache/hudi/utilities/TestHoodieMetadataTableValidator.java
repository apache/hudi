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

package org.apache.hudi.utilities;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.ActiveTimelineUtils;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerators;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import jodd.io.FileUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FACTORY;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_FACTORY;
import static org.apache.hudi.common.testutils.RawTripTestPayload.recordToString;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieMetadataTableValidator extends HoodieSparkClientTestBase {

  private static Stream<Arguments> lastNFileSlicesTestArgs() {
    return Stream.of(
        Arguments.of(-1),
        Arguments.of(1),
        Arguments.of(3),
        Arguments.of(4),
        Arguments.of(5)
    );
  }

  private static Stream<Arguments> viewStorageArgs() {
    return Stream.of(
        Arguments.of(null, null),
        Arguments.of(FileSystemViewStorageType.MEMORY.name(), FileSystemViewStorageType.MEMORY.name()),
        Arguments.of(FileSystemViewStorageType.SPILLABLE_DISK.name(), FileSystemViewStorageType.SPILLABLE_DISK.name()),
        Arguments.of(FileSystemViewStorageType.MEMORY.name(), FileSystemViewStorageType.SPILLABLE_DISK.name())
    );
  }

  @Test
  public void testAggregateColumnStats() {
    HoodieColumnRangeMetadata<Comparable> fileColumn1Range1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", "col1", 1, 5, 0, 10, 100, 200);
    HoodieColumnRangeMetadata<Comparable> fileColumn1Range2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", "col1", 1, 10, 5, 10, 100, 200);
    HoodieColumnRangeMetadata<Comparable> fileColumn2Range1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", "col2", 3, 8, 1, 15, 120, 250);
    HoodieColumnRangeMetadata<Comparable> fileColumn2Range2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", "col2", 5, 9, 4, 5, 80, 150);
    List<HoodieColumnRangeMetadata<Comparable>> colStats = new ArrayList<>();
    colStats.add(fileColumn1Range1);
    colStats.add(fileColumn1Range2);
    colStats.add(fileColumn2Range1);
    colStats.add(fileColumn2Range2);

    int col1Count = 0;
    int col2Count = 0;
    // Ensure merge logic for column stats is correct and aggregate logic creates two entries for two columns
    TreeSet<HoodieColumnRangeMetadata<Comparable>> aggregatedStats = HoodieMetadataTableValidator.aggregateColumnStats("path/to/file1", colStats);
    assertEquals(2, aggregatedStats.size());
    for (HoodieColumnRangeMetadata<Comparable> stat : aggregatedStats) {
      if (stat.getColumnName().equals("col1")) {
        assertEquals(1, stat.getMinValue());
        assertEquals(10, stat.getMaxValue());
        col1Count++;
      } else if (stat.getColumnName().equals("col2")) {
        assertEquals(3, stat.getMinValue());
        assertEquals(9, stat.getMaxValue());
        col2Count++;
      }

      assertEquals(5, stat.getNullCount());
      assertEquals(20, stat.getValueCount());
      assertEquals(200, stat.getTotalSize());
      assertEquals(400, stat.getTotalUncompressedSize());
    }
    assertEquals(1, col1Count);
    assertEquals(1, col2Count);
  }

  @ParameterizedTest
  @MethodSource("viewStorageArgs")
  public void testMetadataTableValidation(String viewStorageTypeForFSListing, String viewStorageTypeForMDTListing) {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    Dataset<Row> inserts = makeInsertDf("000", 5).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Overwrite)
        .save(basePath);
    Dataset<Row> updates = makeUpdateDf("001", 5).cache();
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Append)
        .save(basePath);

    // validate MDT
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    if (viewStorageTypeForFSListing != null && viewStorageTypeForMDTListing != null) {
      config.viewStorageTypeForFSListing = viewStorageTypeForFSListing;
      config.viewStorageTypeForMetadata = viewStorageTypeForMDTListing;
    }
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  @Test
  public void testSecondaryIndexValidation() throws IOException {
    // To overwrite the table properties created during test setup
    storage.deleteDirectory(metaClient.getBasePath());

    sparkSession.sql(
        "create table tbl ("
            + "ts bigint, "
            + "record_key_col string, "
            + "not_record_key_col string, "
            + "partition_key_col string "
            + ") using hudi "
            + "options ("
            + "primaryKey = 'record_key_col', "
            + "type = 'mor', "
            + "hoodie.metadata.enable = 'true', "
            + "hoodie.metadata.record.index.enable = 'true', "
            + "hoodie.datasource.write.recordkey.field = 'record_key_col', "
            + "hoodie.enable.data.skipping = 'true', "
            + "hoodie.datasource.write.precombine.field = 'ts'"
            + ") "
            + "partitioned by(partition_key_col) "
            + "location '" + basePath + "'");

    Dataset<Row> rows = getRowDataset(1, "row1", "abc", "p1");
    rows.write().format("hudi").mode(SaveMode.Append).save(basePath);
    rows = getRowDataset(2, "row2", "cde", "p2");
    rows.write().format("hudi").mode(SaveMode.Append).save(basePath);
    rows = getRowDataset(3, "row3", "def", "p2");
    rows.write().format("hudi").mode(SaveMode.Append).save(basePath);

    // create secondary index
    sparkSession.sql("create index idx_not_record_key_col on tbl using secondary_index(not_record_key_col)");
    validateSecondaryIndex();
  }

  @Test
  public void testGetFSSecondaryKeyToRecordKeys() throws IOException {
    // To overwrite the table properties created during test setup
    storage.deleteDirectory(metaClient.getBasePath());

    sparkSession.sql(
        "create table tbl ("
            + "ts bigint, "
            + "record_key_col string, "
            + "not_record_key_col string, "
            + "partition_key_col string "
            + ") using hudi "
            + "options ("
            + "primaryKey = 'record_key_col', "
            + "type = 'mor', "
            + "hoodie.metadata.enable = 'true', "
            + "hoodie.metadata.record.index.enable = 'true', "
            + "hoodie.datasource.write.recordkey.field = 'record_key_col', "
            + "hoodie.enable.data.skipping = 'true', "
            + "hoodie.datasource.write.precombine.field = 'ts'"
            + ") "
            + "partitioned by(partition_key_col) "
            + "location '" + basePath + "'");

    Dataset<Row> rows = getRowDataset(1, "row1", "abc", "p1");
    rows.write().format("hudi").mode(SaveMode.Append).save(basePath);
    rows = getRowDataset(2, "row2", "cde", "p2");
    rows.write().format("hudi").mode(SaveMode.Append).save(basePath);
    rows = getRowDataset(3, "row3", "def", "p2");
    rows.write().format("hudi").mode(SaveMode.Append).save(basePath);

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    config.ignoreFailed = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Validate getFSSecondaryKeyToRecordKeys API
    int i = 1;
    for (String secKey : new String[]{"abc", "cde", "def"}) {
      // There is one to one mapping between record key and secondary key
      String recKey = "row" + i++;
      List<String> recKeys = validator.getFSSecondaryKeyToRecordKeys(new HoodieSparkEngineContext(jsc, sqlContext), basePath,
              metaClient.getActiveTimeline().lastInstant().get().getRequestTime(), "not_record_key_col", Collections.singletonList(secKey))
          .get(secKey);
      assertEquals(Collections.singletonList(recKey), recKeys);
    }
  }

  private Dataset<Row> getRowDataset(Object... rowValues) {
    List<Row> values = Collections.singletonList(RowFactory.create(rowValues));
    Dataset<Row> rows = sparkSession.createDataFrame(values, new StructType()
        .add(new StructField("ts", IntegerType, true, Metadata.empty()))
        .add(new StructField("record_key_col", StringType, true, Metadata.empty()))
        .add(new StructField("not_record_key_col", StringType, true, Metadata.empty()))
        .add(new StructField("partition_key_col", StringType, true, Metadata.empty()))
    );
    return rows;
  }

  @Test
  public void testPartitionStatsValidation() {
    // TODO: Add validation for compaction and clustering cases
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put("hoodie.table.name", "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");
    writeOptions.put(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");

    Dataset<Row> inserts = makeInsertDf("000", 5);
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "true")
        .mode(SaveMode.Overwrite)
        .save(basePath);
    // validate MDT partition stats
    validatePartitionStats();

    Dataset<Row> updates = makeUpdateDf("001", 5);
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .option(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "true")
        .mode(SaveMode.Append)
        .save(basePath);

    // validate MDT partition stats
    validatePartitionStats();
  }

  private void validatePartitionStats() {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = false;
    config.validateAllFileGroups = false;
    config.validatePartitionStats = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  private void validateSecondaryIndex() {
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = false;
    config.validateAllFileGroups = false;
    config.validateSecondaryIndex = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testAdditionalPartitionsinMDT(boolean testFailureCase) throws InterruptedException {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    // constructor of HoodieMetadataValidator instantiates HoodieTableMetaClient. hence creating an actual table. but rest of tests is mocked.
    Dataset<Row> inserts = makeInsertDf("000", 5).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .mode(SaveMode.Overwrite)
        .save(basePath);

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    MockHoodieMetadataTableValidator validator = new MockHoodieMetadataTableValidator(jsc, config);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

    String partition1 = "PARTITION1";
    String partition2 = "PARTITION2";
    String partition3 = "PARTITION3";

    // mock list of partitions to return from MDT to have 1 additional partition compared to FS based listing.
    List<String> mdtPartitions = Arrays.asList(partition1, partition2, partition3);
    validator.setMetadataPartitionsToReturn(mdtPartitions);
    List<String> fsPartitions = Arrays.asList(partition1, partition2);
    validator.setFsPartitionsToReturn(fsPartitions);

    // mock completed timeline.
    HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
    HoodieTimeline completedTimeline = mock(HoodieTimeline.class);
    when(metaClient.getCommitsTimeline()).thenReturn(commitsTimeline);
    when(commitsTimeline.filterCompletedInstants()).thenReturn(completedTimeline);

    TimeGenerator timeGenerator = TimeGenerators
        .getTimeGenerator(HoodieTimeGeneratorConfig.defaultConfig(basePath),
            HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()));

    StoragePath baseStoragePath = new StoragePath(basePath);

    if (testFailureCase) {
      // 3rd partition which is additional in MDT should have creation time before last instant in timeline.

      String partition3CreationTime = ActiveTimelineUtils.createNewInstantTime(true, timeGenerator);
      Thread.sleep(100);
      String lastIntantCreationTime = ActiveTimelineUtils.createNewInstantTime(true, timeGenerator);

      HoodieInstant lastInstant = INSTANT_FACTORY.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, lastIntantCreationTime);
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
      validator.setPartitionCreationTime(Option.of(partition3CreationTime));
      // validate that exception is thrown since MDT has one additional partition.
      assertThrows(HoodieValidationException.class, () -> {
        validator.validatePartitions(engineContext, baseStoragePath, metaClient);
      });
    } else {
      // 3rd partition creation time is > last completed instant
      HoodieInstant lastInstant = INSTANT_FACTORY.createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION,
          ActiveTimelineUtils.createNewInstantTime(true, timeGenerator));
      when(completedTimeline.lastInstant()).thenReturn(Option.of(lastInstant));
      Thread.sleep(100);
      validator.setPartitionCreationTime(Option.of(ActiveTimelineUtils.createNewInstantTime(true, timeGenerator)));

      // validate that all 3 partitions are returned
      assertEquals(mdtPartitions, validator.validatePartitions(engineContext, baseStoragePath, metaClient));
    }
  }

  @ParameterizedTest
  @MethodSource("lastNFileSlicesTestArgs")
  public void testAdditionalFilesinMetadata(Integer lastNFileSlices) throws IOException {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(),"2");

    Dataset<Row> inserts = makeInsertDf("000", 10).cache();
    inserts.write().format("hudi").options(writeOptions)
        .mode(SaveMode.Overwrite)
        .save(basePath);

    for (int i = 0; i < 6; i++) {
      inserts.write().format("hudi").options(writeOptions)
          .mode(SaveMode.Append)
          .save(basePath);
    }

    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    config.ignoreFailed = true;
    HoodieMetadataTableValidator validator = new HoodieMetadataTableValidator(jsc, config);
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(engineContext.getStorageConf()).build();

    validator.run();
    assertFalse(validator.hasValidationFailure());

    // let's delete one of the log files from 1st commit and so FS based listing and MDT based listing diverges.
    HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline().filterCompletedAndCompactionInstants(), false);
    HoodieFileGroup fileGroup = fsView.getAllFileGroups(StringUtils.EMPTY_STRING).collect(Collectors.toList()).get(0);
    List<FileSlice> allFileSlices = fileGroup.getAllFileSlices().collect(Collectors.toList());
    FileSlice earliestFileSlice = allFileSlices.get(allFileSlices.size() - 1);
    HoodieLogFile earliestLogFile = earliestFileSlice.getLogFiles().collect(Collectors.toList()).get(0);
    metaClient.getStorage().deleteFile(earliestLogFile.getPath());

    HoodieMetadataTableValidator.Config finalConfig = config;
    HoodieMetadataTableValidator localValidator = new HoodieMetadataTableValidator(jsc, finalConfig);
    localValidator.run();
    assertTrue(localValidator.hasValidationFailure());
    assertTrue(localValidator.getThrowables().get(0) instanceof HoodieValidationException);

    // lets set lastN file slices to 2 and so validation should succeed. (bcoz, there will be mis-match only on first file slice)
    config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file:" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;
    if (lastNFileSlices != -1) {
      config.validateLastNFileSlices = lastNFileSlices;
    }
    config.ignoreFailed = true;
    validator = new HoodieMetadataTableValidator(jsc, config);
    validator.run();
    if (lastNFileSlices != -1 && lastNFileSlices < 4) {
      assertFalse(validator.hasValidationFailure());
    } else {
      assertTrue(validator.hasValidationFailure());
      assertTrue(validator.getThrowables().get(0) instanceof HoodieValidationException);
    }
  }

  class MockHoodieMetadataTableValidator extends HoodieMetadataTableValidator {

    private List<String> metadataPartitionsToReturn;
    private List<String> fsPartitionsToReturn;
    private Option<String> partitionCreationTime;

    public MockHoodieMetadataTableValidator(JavaSparkContext jsc, Config cfg) {
      super(jsc, cfg);
    }

    void setMetadataPartitionsToReturn(List<String> metadataPartitionsToReturn) {
      this.metadataPartitionsToReturn = metadataPartitionsToReturn;
    }

    void setFsPartitionsToReturn(List<String> fsPartitionsToReturn) {
      this.fsPartitionsToReturn = fsPartitionsToReturn;
    }

    void setPartitionCreationTime(Option<String> partitionCreationTime) {
      this.partitionCreationTime = partitionCreationTime;
    }

    @Override
    List<String> getPartitionsFromFileSystem(HoodieEngineContext engineContext, StoragePath basePath, HoodieStorage storage, HoodieTimeline completedTimeline) {
      return fsPartitionsToReturn;
    }

    @Override
    List<String> getPartitionsFromMDT(HoodieEngineContext engineContext, StoragePath basePath, HoodieStorage storage) {
      return metadataPartitionsToReturn;
    }

    @Override
    Option<String> getPartitionCreationInstant(HoodieStorage storage, StoragePath basePath, String partition) {
      return this.partitionCreationTime;
    }
  }

  @Test
  public void testRliValidationFalsePositiveCase() throws IOException {
    Map<String, String> writeOptions = new HashMap<>();
    writeOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), "test_table");
    writeOptions.put(DataSourceWriteOptions.TABLE_TYPE().key(), "MERGE_ON_READ");
    writeOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "_row_key");
    writeOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "timestamp");
    writeOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "partition_path");

    Dataset<Row> inserts = makeInsertDf("000", 5).cache();
    inserts.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Overwrite)
        .save(basePath);
    Dataset<Row> updates = makeUpdateDf("001", 5).cache();
    updates.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.UPSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Append)
        .save(basePath);

    Dataset<Row> inserts2 = makeInsertDf("002", 5).cache();
    inserts2.write().format("hudi").options(writeOptions)
        .option(DataSourceWriteOptions.OPERATION().key(), WriteOperationType.BULK_INSERT.value())
        .option(HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key(), "true")
        .option(HoodieMetadataConfig.RECORD_INDEX_MIN_FILE_GROUP_COUNT_PROP.key(), "1")
        .option(HoodieMetadataConfig.RECORD_INDEX_MAX_FILE_GROUP_COUNT_PROP.key(), "1")
        .mode(SaveMode.Append)
        .save(basePath);

    // validate MDT
    HoodieMetadataTableValidator.Config config = new HoodieMetadataTableValidator.Config();
    config.basePath = "file://" + basePath;
    config.validateLatestFileSlices = true;
    config.validateAllFileGroups = true;

    // lets ensure we have a pending commit when FS based polling is done. and the commit completes when MDT is polled.
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration())).build();
    // moving out the completed commit meta file to a temp location
    HoodieInstant lastInstant = metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get();
    String latestCompletedCommitMetaFile = basePath + "/.hoodie/" + INSTANT_FILE_NAME_FACTORY.getFileName(lastInstant);
    String tempDir = getTempLocation();
    String destFilePath = tempDir + "/" + INSTANT_FILE_NAME_FACTORY.getFileName(lastInstant);
    FileUtil.move(latestCompletedCommitMetaFile, destFilePath);

    MockHoodieMetadataTableValidatorForRli validator = new MockHoodieMetadataTableValidatorForRli(jsc, config);
    validator.setOriginalFilePath(latestCompletedCommitMetaFile);
    validator.setDestFilePath(destFilePath);
    assertTrue(validator.run());
    assertFalse(validator.hasValidationFailure());
    assertTrue(validator.getThrowables().isEmpty());
  }

  /**
   * Class to assist with testing a false positive case with RLI validation.
   */
  static class MockHoodieMetadataTableValidatorForRli extends HoodieMetadataTableValidator {

    private String destFilePath;
    private String originalFilePath;

    public MockHoodieMetadataTableValidatorForRli(JavaSparkContext jsc, Config cfg) {
      super(jsc, cfg);
    }

    @Override
    JavaPairRDD<String, Pair<String, String>> getRecordLocationsFromRLI(HoodieSparkEngineContext sparkEngineContext,
                                                                        String basePath,
                                                                        String latestCompletedCommit) {
      // move the completed file back to ".hoodie" to simuate the false positive case.
      try {
        FileUtil.move(destFilePath, originalFilePath);
        return super.getRecordLocationsFromRLI(sparkEngineContext, basePath, latestCompletedCommit);
      } catch (IOException e) {
        throw new HoodieException("Move should not have failed");
      }
    }

    public void setDestFilePath(String destFilePath) {
      this.destFilePath = destFilePath;
    }

    public void setOriginalFilePath(String originalFilePath) {
      this.originalFilePath = originalFilePath;
    }
  }

  private String getTempLocation() {
    try {
      String folderName = "temp_location";
      java.nio.file.Path tempPath = tempDir.resolve(folderName);
      java.nio.file.Files.createDirectories(tempPath);
      return tempPath.toAbsolutePath().toString();
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  protected Dataset<Row> makeInsertDf(String instantTime, Integer n) {
    List<String> records = dataGen.generateInserts(instantTime, n).stream()
        .map(r -> recordToString(r).get()).collect(Collectors.toList());
    JavaRDD<String> rdd = jsc.parallelize(records);
    return sparkSession.read().json(rdd);
  }

  protected Dataset<Row> makeUpdateDf(String instantTime, Integer n) {
    try {
      List<String> records = dataGen.generateUpdates(instantTime, n).stream()
          .map(r -> recordToString(r).get()).collect(Collectors.toList());
      JavaRDD<String> rdd = jsc.parallelize(records);
      return sparkSession.read().json(rdd);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
