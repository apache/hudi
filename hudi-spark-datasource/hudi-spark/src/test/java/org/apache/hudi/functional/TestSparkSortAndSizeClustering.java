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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.run.strategy.MultipleSparkJobExecutionStrategy;
import org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy;
import org.apache.hudi.client.utils.FileSliceMetricUtils;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.MetadataMergeWriteStatus;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import java.util.stream.Stream;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSparkSortAndSizeClustering extends HoodieSparkClientTestHarness {

  private HoodieWriteConfig config;
  private HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0);

  public void setup(int maxFileSize) throws IOException {
    setup(maxFileSize, Collections.emptyMap());
  }

  public void setup(int maxFileSize, Map<String, String> options) throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initHoodieStorage();
    Properties props = getPropertiesForKeyGen(true);
    props.putAll(options);
    props.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, props);
    config = getConfigBuilder().withProps(props)
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(maxFileSize).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanPartitionFilterMode(ClusteringPlanPartitionFilterMode.RECENT_DAYS)
            .build())
        .build();

    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  class DummySparkJobExecutionStrategy<T> extends SparkSortAndSizeExecutionStrategy<T> {
    final int executorCount;

    public DummySparkJobExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig, int executorCount) {
      super(table, engineContext, writeConfig);
      this.executorCount = executorCount;
    }

    public int getExecutorCount() {
      return this.executorCount;
    }

    @Override
    public HoodieData<WriteStatus> performClusteringWithRecordsAsRow(Dataset<Row> inputRecords, int numOutputGroups, String instantTime, Map<String, String> strategyParams,
                                                                     Schema schema, List<HoodieFileGroupId> fileGroupIdList, boolean shouldPreserveHoodieMetadata, Map<String, String> extraMetadata) {
      ArrayList<WriteStatus> statuses = new ArrayList<>();

      for (HoodieFileGroupId fileGroupId : fileGroupIdList) {
        WriteStatus writeStatus = new WriteStatus(false, 0.0);
        writeStatus.setFileId(fileGroupId.getFileId());
        writeStatus.setPartitionPath(fileGroupId.getPartitionPath());
        writeStatus.setTotalRecords(inputRecords.count());

        statuses.add(writeStatus);
      }

      return HoodieJavaRDD.of(jsc.parallelize(statuses));
    }

    @Override
    public HoodieData<WriteStatus> performClusteringWithRecordsRDD(HoodieData<HoodieRecord<T>> inputRecords, int numOutputGroups, String instantTime,
                                                                   Map<String, String> strategyParams, Schema schema, List<HoodieFileGroupId> fileGroupIdList,
                                                                   boolean shouldPreserveHoodieMetadata, Map<String, String> extraMetadata) {
      ArrayList<WriteStatus> statuses = new ArrayList<>();

      for (HoodieFileGroupId fileGroupId : fileGroupIdList) {
        WriteStatus writeStatus = new WriteStatus(false, 0.0);
        writeStatus.setFileId(fileGroupId.getFileId());
        writeStatus.setPartitionPath(fileGroupId.getPartitionPath());
        writeStatus.setTotalRecords(inputRecords.count());

        statuses.add(writeStatus);
      }

      return HoodieJavaRDD.of(jsc.parallelize(statuses));
    }

    protected CompletableFuture<HoodieData<WriteStatus>> runClusteringForGroupAsyncAsRow(HoodieClusteringGroup clusteringGroup,
                                                                               Map<String, String> strategyParams,
                                                                               boolean shouldPreserveHoodieMetadata,
                                                                               String instantTime,
                                                                               ExecutorService clusteringExecutorService) {
      ArrayList<WriteStatus> statuses = new ArrayList<>();

      clusteringGroup.getSlices().forEach(hoodieSliceInfo -> {
        WriteStatus writeStatus = new WriteStatus(false, 0.0);
        writeStatus.setFileId(hoodieSliceInfo.getFileId());
        writeStatus.setPartitionPath(hoodieSliceInfo.getPartitionPath());
        writeStatus.setTotalRecords(0);

        statuses.add(writeStatus);
      });

      return CompletableFuture.completedFuture(HoodieJavaRDD.of(jsc.parallelize(statuses)));
    }
  }

  @Test
  public void testClusteringWithRDD() throws IOException {
    writeAndClustering(false);
  }

  @Test
  public void testClusteringWithRow() throws IOException {
    writeAndClustering(true);
  }

  public void writeAndClustering(boolean isRow) throws IOException {
    setup(102400);
    config.setValue("hoodie.datasource.write.row.writer.enable", String.valueOf(isRow));
    config.setValue("hoodie.metadata.enable", "false");
    config.setValue("hoodie.clustering.plan.strategy.daybased.lookback.partitions", "1");
    config.setValue("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(1024 * 1024));
    config.setValue("hoodie.clustering.plan.strategy.max.bytes.per.group", String.valueOf(2 * 1024 * 1024));

    int numRecords = 1000;
    writeData(writeClient.createNewInstantTime(), numRecords, true, dataGen);

    String clusteringTime = (String) writeClient.scheduleClustering(Option.empty()).get();
    HoodieClusteringPlan plan = ClusteringUtils.getClusteringPlan(
        metaClient, INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringTime)).map(Pair::getRight).get();

    List<HoodieClusteringGroup> inputGroups = plan.getInputGroups();
    assertEquals(1, inputGroups.size(), "Clustering plan will contain 1 input group");

    Integer outputFileGroups = plan.getInputGroups().get(0).getNumOutputFileGroups();
    assertEquals(2, outputFileGroups, "Clustering plan will generate 2 output groups");

    HoodieWriteMetadata writeMetadata = writeClient.cluster(clusteringTime, true);
    List<HoodieWriteStat> writeStats = (List<HoodieWriteStat>)writeMetadata.getWriteStats().get();
    assertEquals(2, writeStats.size(), "Clustering should write 2 files");

    List<Row> rows = readRecords();
    assertEquals(numRecords, rows.size());
    validateDecimalTypeAfterClustering(writeStats);
  }

  // Validate that clustering produces decimals in legacy format
  private void validateDecimalTypeAfterClustering(List<HoodieWriteStat> writeStats) {
    writeStats.stream().map(writeStat -> new StoragePath(metaClient.getBasePath(), writeStat.getPath())).forEach(writtenPath -> {
      MessageType schema = ParquetUtils.readMetadata(storage, writtenPath)
          .getFileMetaData().getSchema();
      int index = schema.getFieldIndex("height");
      Type decimalType = schema.getFields().get(index);
      assertEquals("DECIMAL", decimalType.getOriginalType().toString());
      assertEquals("FIXED_LEN_BYTE_ARRAY", decimalType.asPrimitiveType().getPrimitiveTypeName().toString());
    });
  }

  private List<WriteStatus> writeData(String commitTime, int totalRecords, boolean doCommit, HoodieTestDataGenerator dataGen) {
    List<HoodieRecord> records = dataGen.generateInserts(commitTime, totalRecords);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records);
    metaClient = HoodieTableMetaClient.reload(metaClient);

    WriteClientTestUtils.startCommitWithTime(writeClient, commitTime);
    List<WriteStatus> writeStatues = writeClient.insert(writeRecords, commitTime).collect();
    org.apache.hudi.testutils.Assertions.assertNoWriteErrors(writeStatues);

    if (doCommit) {
      Assertions.assertTrue(writeClient.commitStats(commitTime, writeStatues.stream().map(WriteStatus::getStat).collect(Collectors.toList()),
          Option.empty(), metaClient.getCommitActionType()));
    }

    metaClient = HoodieTableMetaClient.reload(metaClient);
    return writeStatues;
  }

  private List<Row> readRecords() {
    Dataset<Row> roViewDF = sparkSession.read().format("hudi").load(basePath);
    roViewDF.createOrReplaceTempView("clutering_table");
    return sparkSession.sqlContext().sql("select * from clutering_table").collectAsList();
  }

  public HoodieWriteConfig.Builder getConfigBuilder() {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withWriteStatusClass(MetadataMergeWriteStatus.class)
        .forTable("clustering-table")
        .withEmbeddedTimelineServerEnabled(true);
  }

  @Test
  public void testPerformClustering() throws Exception {
    setup(102400);
    config.setValue("hoodie.datasource.write.row.writer.enable", String.valueOf(true));
    config.setValue("hoodie.metadata.enable", "false");
    config.setValue("hoodie.clustering.plan.strategy.daybased.lookback.partitions", "1");
    config.setValue("hoodie.clustering.plan.strategy.target.file.max.bytes", String.valueOf(1024 * 1024));
    config.setValue("hoodie.clustering.plan.strategy.max.bytes.per.group", String.valueOf(2 * 1024 * 1024));
    config.setValue("hoodie.clustering.max.parallelism", String.valueOf(30));

    int numRecords = 1000;
    int numInputGroups = 100;
    int numOutputGroups = 400;
    int sliceCount = 410;

    writeData(writeClient.createNewInstantTime(), numRecords, true, dataGen);

    ArrayList<HoodieSliceInfo> sliceInfos = new ArrayList<>(sliceCount);

    for (int i = 0; i < sliceCount; i++) {
      sliceInfos.add(HoodieSliceInfo.newBuilder()
          .setPartitionPath("part")
          .setFileId("file")
          .setDeltaFilePaths(Collections.emptyList())
          .setBootstrapFilePath("bootstrap")
          .setVersion(0)
          .build());
    }

    HoodieClusteringGroup group = HoodieClusteringGroup.newBuilder()
        .setSlices(sliceInfos)
        .setNumOutputFileGroups(numOutputGroups)
        .setMetrics(Collections.emptyMap())
        .setVersion(1)
        .build();

    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(SparkSortAndSizeExecutionStrategy.class.getName())
        .setStrategyParams(Collections.emptyMap())
        .build();

    ArrayList<HoodieClusteringGroup> groups = new ArrayList<>();

    for (int i = 0; i < numInputGroups; i++) {
      groups.add(group);
    }

    HoodieClusteringPlan plan = HoodieClusteringPlan.newBuilder()
        .setInputGroups(groups)
        .setStrategy(strategy)
        .build();

    Schema avroSchema = new Schema.Parser().parse(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA);
    String instantTime = "20250611133000";

    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    MultipleSparkJobExecutionStrategy<?> clusteringStrategy =
        new DummySparkJobExecutionStrategy<>(table, context, config, 0);


    HoodieWriteMetadata<HoodieData<WriteStatus>> metadata =
        clusteringStrategy.performClustering(plan, avroSchema, instantTime);

    assertEquals(numOutputGroups, metadata.getWriteStatuses().getNumPartitions());
  }

  // executorCount, sliceCount, numInputGroups, numOutputGroups, writeIOMB, expectedParallelism, isMetricsAvailable
  private static Stream<Arguments> getClusteringMaxParallelismByDiskIO() {
    return Stream.of(
        Arguments.of(6, 20, 100, 400, 50.0 * 1024, 8, true), // generic use case
        Arguments.of(1, 20, 100, 400, 1024, 50, true), // atleast half of groups to check more than one batch count
        Arguments.of(6, 20, 100, 400, 1024, 100, true), // total write io can be filled by available disk space
        Arguments.of(6, 20, 100, 400, 1024, 100, false) // metrics are not available
    );
  }

  @ParameterizedTest
  @MethodSource("getClusteringMaxParallelismByDiskIO")
  public void testGetClusteringMaxParallelismByDiskIO(
      int executorCount, int sliceCount, int numInputGroups,
      int numOutputGroups, double writeIOMB, int expectedParallelism,
      boolean isMetricsAvailable) throws Exception {

    setup(1024);

    ArrayList<HoodieSliceInfo> sliceInfos = new ArrayList<>(sliceCount);

    for (int i = 0; i < sliceCount; i++) {
      sliceInfos.add(HoodieSliceInfo.newBuilder()
          .setPartitionPath("part")
          .setFileId("file")
          .setDeltaFilePaths(Collections.emptyList())
          .setBootstrapFilePath("bootstrap")
          .setVersion(0)
          .build());
    }

    Map<String, Double> metricsMap = isMetricsAvailable
        ? Collections.singletonMap(FileSliceMetricUtils.TOTAL_IO_WRITE_MB, writeIOMB)
        : Collections.emptyMap();

    HoodieClusteringGroup group = HoodieClusteringGroup.newBuilder()
        .setSlices(sliceInfos)
        .setNumOutputFileGroups(numOutputGroups)
        .setMetrics(metricsMap)
        .setVersion(1)
        .build();


    HoodieClusteringStrategy strategy = HoodieClusteringStrategy.newBuilder()
        .setStrategyClassName(SparkSortAndSizeExecutionStrategy.class.getName())
        .setStrategyParams(Collections.emptyMap())
        .build();

    ArrayList<HoodieClusteringGroup> groups = new ArrayList<>();

    for (int i = 0; i < numInputGroups; i++) {
      groups.add(group);
    }

    HoodieClusteringPlan plan = HoodieClusteringPlan.newBuilder()
        .setInputGroups(groups)
        .setStrategy(strategy)
        .build();

    HoodieTable table = HoodieSparkTable.create(config, context, metaClient);

    MultipleSparkJobExecutionStrategy<?> clusteringStrategy =
        new DummySparkJobExecutionStrategy<>(table, context, config, executorCount);

    assertEquals(expectedParallelism, clusteringStrategy.getClusteringMaxParallelismByDiskIO(config, plan));
  }
}
