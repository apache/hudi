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

package org.apache.hudi.functional;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.update.strategy.SparkExtensibleBucketDuplicateUpdateStrategy;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.bulkinsert.BulkInsertSortMode;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.spark_project.guava.collect.Lists;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecord.FILENAME_METADATA_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;

// TODO: support not row writer
@Tag("functional")
public class TestSparkExtensibleBucketClustering extends HoodieSparkClientTestHarness {

  private HoodieWriteConfig config;
  private HoodieTestDataGenerator dateGen = new HoodieTestDataGenerator(0);

  public void setUp(int maxFileSize, int targetBucketNum) throws IOException {
    setUp(maxFileSize, targetBucketNum, Collections.emptyMap());
  }

  public void setUp(int maxFileSize, int targetBucketNum, Map<String, String> options) throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initHoodieStorage();
    Properties pros = getPropertiesForKeyGen(true);
    pros.putAll(options);
    pros.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    String initBucketNum = pros.getProperty(HoodieTableConfig.INITIAL_BUCKET_NUM_FOR_NEW_PARTITION.key());
    if (initBucketNum == null) {
      pros.setProperty(HoodieTableConfig.INITIAL_BUCKET_NUM_FOR_NEW_PARTITION.key(), "8");
    }
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.MERGE_ON_READ, pros);
    config = getConfigBuilder().withProps(pros)
        .withAutoCommit(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(pros)
            .withIndexType(HoodieIndex.IndexType.BUCKET).withBucketIndexEngineType(HoodieIndex.BucketIndexEngineType.EXTENSIBLE_BUCKET)
            .build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(maxFileSize).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(HoodieClusteringConfig.SPARK_EXTENSIBLE_BUCKET_CLUSTERING_PLAN_STRATEGY)
            .withClusteringExecutionStrategyClass(HoodieClusteringConfig.SPARK_EXTENSIBLE_BUCKET_EXECUTION_STRATEGY)
            .withClusteringUpdatesStrategy(SparkExtensibleBucketDuplicateUpdateStrategy.class.getName())
            .withBucketResizingTargetBucketNum(targetBucketNum)
            .build())
        .build();

    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  public static List<Object[]> configParams() {
    return Arrays.asList(new Object[][] {
        {2, true},
        {4, true},
        {16, true},
        {128, true},
        {2, false},
        {4, false},
        {16, false},
        {128, false}
    });
  }

  @ParameterizedTest
  @MethodSource("configParams")
  public void testResizing(int targetBucketNum, boolean rowWriterEnable) throws IOException {
    int maxFileSize = 128 * 1024 * 1024;
    setUp(maxFileSize, targetBucketNum);
    config.setValue("hoodie.datasource.write.row.writer.enable", String.valueOf(rowWriterEnable));
    config.setValue("hoodie.metadata.enable", "false");
    writeData(writeClient.createNewInstantTime(), 2000, true);
    writeData(writeClient.createNewInstantTime(), 1000, true);
    List<Row> expectedRows = readRecordsSortedByPK();
    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    List<Row> actualRows = readRecordsSortedByPK();
    verifyRows(expectedRows, actualRows);
    writeClient.cluster(clusteringTime, true);
    actualRows = readRecordsSortedByPK();
    verifyRows(expectedRows, actualRows);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    Arrays.stream(dataGen.getPartitionPaths()).forEach(p -> {
      Option<HoodieExtensibleBucketMetadata> metadata = ExtensibleBucketIndexUtils.loadMetadata(table, p);
      Assertions.assertTrue(metadata.isPresent());
      assertEquals(targetBucketNum, metadata.get().getBucketNum());

      // The file slice has no log files
      table.getSliceView().getLatestFileSlices(p).forEach(fs -> {
        Assertions.assertTrue(fs.getBaseFile().isPresent());
        Assertions.assertTrue(fs.getLogFiles().count() == 0);
      });
    });
  }

  private static Stream<Arguments> configParamsForSorting() {
    return Stream.of(
        Arguments.of("begin_lat", true),
        Arguments.of("_row_key", true),
        Arguments.of("begin_lat", false),
        Arguments.of("_row_key", false)
    );
  }

  /**
   * 1. Test PARTITION_SORT mode, i.e., sort by the record key
   * 2. Test custom column sort
   *
   * @throws IOException
   */
  @ParameterizedTest
  @MethodSource("configParamsForSorting")
  public void testClusteringColumnSort(String sortColumn, boolean rowWriterEnable) throws Exception {
    Map<String, String> options = new HashMap<>();
    // Record key is handled specially
    if (sortColumn.equals("_row_key")) {
      options.put(HoodieWriteConfig.BULK_INSERT_SORT_MODE.key(), BulkInsertSortMode.PARTITION_SORT.toString());
    } else {
      options.put(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key(), sortColumn);
    }
    options.put("hoodie.datasource.write.row.writer.enable", String.valueOf(rowWriterEnable));
    setUp(128 * 1024 * 1024, 16, options);

    writeData(writeClient.createNewInstantTime(), 500, true);
    writeData(writeClient.createNewInstantTime(), 500, true);
    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    writeClient.cluster(clusteringTime, true);

    // Check the specified column is in sort order
    metaClient = HoodieTableMetaClient.reload(metaClient);

    List<Row> rows = readRecords(true);
    assertEquals(1000, rows.size());

    StructType schema = rows.get(0).schema();
    Schema rawSchema = AvroConversionUtils.convertStructTypeToAvroSchema(schema, "test_struct_name", "test_namespace");
    Schema.Field field = rawSchema.getField(sortColumn);
    Schema.Field fileNameFiled = rawSchema.getField(FILENAME_METADATA_FIELD);

    Comparator comparator;
    if (field.schema().getType() == Schema.Type.DOUBLE) {
      comparator = Comparator.comparingDouble(row -> (double) (((Row) row).get(field.pos())));
    } else if (field.schema().getType() == Schema.Type.STRING) {
      comparator = Comparator.comparing(row -> ((Row) row).get(field.pos()).toString());
    } else {
      throw new HoodieException("Cannot get comparator: unsupported data type, " + field.schema().getType());
    }

    // Compare the sort column are sorted just with the bucket file
    Row lastRow = null;
    String lastFileName = null;
    for (Row row : rows) {
      String currentFileName = row.get(fileNameFiled.pos()).toString();
      if (!(lastFileName == null || !currentFileName.equals(lastFileName) || lastRow == null)) {
        Assertions.assertTrue(lastRow == null || comparator.compare(lastRow, row) <= 0,
            "The rows are not sorted based on the column: " + sortColumn);
      }
      lastRow = row;
      lastFileName = currentFileName;
    }
  }

  /**
   * 1. If there is any ongoing writing, cannot schedule clustering
   * 2. If the clustering is scheduled, it cannot block incoming new writers
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testConcurrentWrite(boolean rowWriterEnable) throws IOException {
    setUp(5120, 16);
    config.setValue("hoodie.datasource.write.row.writer.enable", String.valueOf(rowWriterEnable));
    String writeTime = writeClient.createNewInstantTime();
    List<WriteStatus> writeStatues = writeData(writeTime, 2000, false);
    // Cannot schedule clustering if there is in-flight writer
    Assertions.assertFalse(writeClient.scheduleClustering(Option.empty()).isPresent());
    Assertions.assertTrue(writeClient.commitStats(writeTime, context.parallelize(writeStatues, 1), writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        Option.empty(), metaClient.getCommitActionType()));
    metaClient = HoodieTableMetaClient.reload(metaClient);

    // Schedule clustering
    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    // Concurrent is not blocked by the clustering
    writeData(writeClient.createNewInstantTime(), 2000, true);
    // The records are immediately visible when the writer completes
    List<Row> expectedRows = readRecordsSortedByPK();
    assertEquals(4000, expectedRows.size());
    // Clustering finished, check the number of records (there will be file group switch in the background)
    writeClient.cluster(clusteringTime, true);
    List<Row> actualRows = readRecordsSortedByPK();
    verifyRows(expectedRows, actualRows);
  }

  private HoodieRecord generateHoodieRecord(String recordKey, String partitionPath, String salt) {
    GenericRecord record = dateGen.generateGenericRecord(recordKey, partitionPath, "rider" + salt, "driver" + salt, 0);
    return new HoodieAvroRecord<>(new HoodieKey(recordKey, partitionPath), new HoodieAvroPayload(Option.of(record)));
  }

  private List<HoodieRecord> generateCornerCaseInitialRecords(String commitTime) {
    return Arrays.asList(
        /**
         * partition_0
         *        - bucket_0_0
         *          - key0
         *          - key2
         *          - key4
         *        - bucket_1_0
         *          - key1
         */
        generateHoodieRecord("key0", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, commitTime),
        generateHoodieRecord("key1", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, commitTime),
        generateHoodieRecord("key2", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, commitTime),
        generateHoodieRecord("key4", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, commitTime),

        /**
         * partition_1
         *        - bucket_0_0
         *          - key0
         *          - key2
         *          - key4
         *        - bucket_1_0 [not exist]
         *          - empty
         */
        generateHoodieRecord("key0", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, commitTime),
        generateHoodieRecord("key2", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, commitTime),
        generateHoodieRecord("key4", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, commitTime));
  }

  @Test
  public void testCornerCase() throws IOException {
    // initial bucket num with 2
    setUp(5120, 4, Collections.singletonMap(HoodieTableConfig.INITIAL_BUCKET_NUM_FOR_NEW_PARTITION.key(), "2"));
    String commitTime = writeClient.createNewInstantTime();
    List<HoodieRecord> records = generateCornerCaseInitialRecords(commitTime);

    writeData(records, commitTime, true);

    // expect 3 file group
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    List<FileSlice> firstPartitionView = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(2, firstPartitionView.size());
    List<FileSlice> secondPartitionView = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionView.size());

    List<Row> expectedRows = readRecordsSortedByPK();

    // start clustering
    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    writeClient.cluster(clusteringTime, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieSparkTable.create(config, context, metaClient);

    // expect 5 file group
    /**
     * partition_0
     *       - bucket_0_1
     *          - key0
     *          - key4
     *       - bucket_1_1
     *          - key1
     *       - bucket_2_1
     *          - key2
     *       - bucket_3_1 [not exist]
     *          - empty
     *
     * partition_1
     *       - bucket_0_1
     *          - key0
     *          - key4
     *       - bucket_1_1 [not exist]
     *          - empty
     *       - bucket_2_1
     *          - key2
     *       - bucket_3_1 [not exist]
     *          - empty
     */
    List<FileSlice> firstPartitionViewAfterClustering = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(3, firstPartitionViewAfterClustering.size());
    List<FileSlice> secondPartitionViewAfterClustering = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(2, secondPartitionViewAfterClustering.size());

    List<Row> actualRows = readRecordsSortedByPK();
    verifyRows(expectedRows, actualRows);
  }

  @Test
  public void testCornerCaseWithConcurrentWrite() throws IOException {
    // initial bucket num with 2
    setUp(5120, 4, Collections.singletonMap(HoodieTableConfig.INITIAL_BUCKET_NUM_FOR_NEW_PARTITION.key(), "2"));
    String commitTime = writeClient.createNewInstantTime();
    List<HoodieRecord> records = generateCornerCaseInitialRecords(commitTime);

    writeData(records, commitTime, true);

    // expect 3 file group
    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);
    List<FileSlice> firstPartitionView = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(2, firstPartitionView.size());
    List<FileSlice> secondPartitionView = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionView.size());

    // schedule clustering
    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();

    String concurrentWriteTime = writeClient.createNewInstantTime();

    // concurrent write
    List<HoodieRecord> updatedRecords = Arrays.asList(

        generateHoodieRecord("key0", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, concurrentWriteTime),
        generateHoodieRecord("key1", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, concurrentWriteTime),
        generateHoodieRecord("key2", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, concurrentWriteTime),
        generateHoodieRecord("key3", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, concurrentWriteTime),
        generateHoodieRecord("key4", HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, concurrentWriteTime),

        generateHoodieRecord("key0", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, concurrentWriteTime),
        generateHoodieRecord("key1", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, concurrentWriteTime),
        generateHoodieRecord("key2", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, concurrentWriteTime),
        generateHoodieRecord("key3", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, concurrentWriteTime),
        generateHoodieRecord("key4", HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, concurrentWriteTime));

    writeData(updatedRecords, concurrentWriteTime, true);
    // expect 4 file group, partition_1: bucket_1_0 also has records
    table = HoodieSparkTable.create(config, context, metaClient);
    firstPartitionView = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(2, firstPartitionView.size());
    secondPartitionView = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(2, secondPartitionView.size());
    List<Row> expectedRows = readRecordsSortedByPK();

    // execute clustering
    writeClient.cluster(clusteringTime, true);

    metaClient = HoodieTableMetaClient.reload(metaClient);
    table = HoodieSparkTable.create(config, context, metaClient);

    // expect 8 file group
    /**
     * partition_0
     *       - bucket_0_1
     *          - key0
     *          - key4
     *       - bucket_1_1
     *          - key1
     *       - bucket_2_1
     *          - key2
     *       - bucket_3_1
     *          - key3
     *
     * partition_1
     *       - bucket_0_1
     *          - key0
     *          - key4
     *       - bucket_1_1
     *          - key1
     *       - bucket_2_1
     *          - key2
     *       - bucket_3_1
     *          - key3
     */
    List<FileSlice> firstPartitionViewAfterClustering = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(4, firstPartitionViewAfterClustering.size());
    List<FileSlice> secondPartitionViewAfterClustering = table.getSliceView().getLatestFileSlices(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(4, secondPartitionViewAfterClustering.size());

    List<Row> actualRows = readRecordsSortedByPK();
    verifyRows(expectedRows, actualRows);
  }

  private void verifyRows(List<Row> expectedRows, List<Row> actualRows) {
    assertEquals(expectedRows.size(), actualRows.size());
    for (int i = 0; i < expectedRows.size(); i++) {
      assertEquals(expectedRows.get(i), actualRows.get(i));
    }
  }

  /**
   * Insert `num` records into table given the commitTime
   *
   * @param commitTime
   * @param totalRecords
   */
  private List<WriteStatus> writeData(String commitTime, int totalRecords, boolean doCommit) {
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, totalRecords);
    return writeData(records, commitTime, doCommit);
  }

  private List<WriteStatus> writeData(List<HoodieRecord> records, String commitTime, boolean doCommit) {
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 2);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    writeClient.startCommitWithTime(commitTime);
    List<WriteStatus> writeStatues = writeClient.upsert(writeRecords, commitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);
    if (doCommit) {
      Assertions.assertTrue(writeClient.commitStats(commitTime, context.parallelize(writeStatues, 1), writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType()));
    }
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatues;
  }

  private List<Row> readRecordsSortedByPK() {
    return readRecords(false, Lists.newArrayList("partition_path", "_row_key"));
  }

  private List<Row> readRecords() {
    return readRecords(false);
  }

  private List<Row> readRecords(boolean containsMetadata) {
    return readRecords(containsMetadata, null);
  }

  private List<Row> readRecords(boolean containsMetadata, List<String> sortColumns) {
    Dataset<Row> roViewDF = sparkSession
        .read()
        .format("hudi")
        .load(basePath + "/*/*/*/*");
    if (sortColumns != null && !sortColumns.isEmpty()) {
      roViewDF = roViewDF.orderBy(sortColumns.stream().map(Column::new).toArray(Column[]::new));
    }
    roViewDF.createOrReplaceTempView("hudi_ro_table");
    if (containsMetadata) {
      return sparkSession.sqlContext().sql("select * from hudi_ro_table").collectAsList();
    }
    return sparkSession.sqlContext().sql("select _row_key, partition_path, timestamp, trip_type, rider, driver, begin_lat, begin_lon, end_lat, end_lon,"
        + "distance_in_meters, seconds_since_epoch, weight, nation, current_date, current_ts, height, city_to_state, fare, tip_history from hudi_ro_table").collectAsList();
  }

}
