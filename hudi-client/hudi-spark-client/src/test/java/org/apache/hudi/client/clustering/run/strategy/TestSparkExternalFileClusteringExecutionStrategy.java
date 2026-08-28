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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.ExternalFileClusteringTestExecutionStrategy;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.clustering.plan.strategy.SparkSingleFileSortPlanStrategy;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link SparkExternalFileClusteringExecutionStrategy} and the
 * {@link org.apache.hudi.io.ExternalFileClusteringWriteHandle} it drives, using the
 * {@link ExternalFileClusteringTestExecutionStrategy} which transforms a file by copying it as is.
 */
public class TestSparkExternalFileClusteringExecutionStrategy extends HoodieSparkClientTestHarness {

  private static final String PARTITION_PATH = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;

  private HoodieWriteConfig config;

  @BeforeEach
  public void setUp() throws IOException {
    initPath();
    initSparkContexts();
    initTestDataGenerator();
    initHoodieStorage();
    Properties props = getPropertiesForKeyGen(true);
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, props);
    config = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withProps(props)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .forTable("external_file_clustering_table")
        // The write handle creates its marker file in the constructor, so keep markers direct and
        // avoid the embedded timeline server.
        .withEmbeddedTimelineServerEnabled(false)
        .withMarkersType(MarkerType.DIRECT.name())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().parquetMaxFileSize(1024 * 1024).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            // Single file sort plan strategy emits exactly one file slice per clustering group,
            // which is what the external file strategy requires.
            .withClusteringPlanStrategyClass(SparkSingleFileSortPlanStrategy.class.getName())
            .withClusteringExecutionStrategyClass(ExternalFileClusteringTestExecutionStrategy.class.getName())
            .build())
        .build();
    writeClient = getHoodieWriteClient(config);
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupResources();
  }

  /**
   * Schedules a real clustering plan over two file groups and runs every group of that plan through
   * the strategy, asserting that each clustered file is a faithful copy of its input and that the
   * write status the handle derives from it is complete.
   *
   * <p>NOTE: the groups are executed through {@code performClusteringForGroup} rather than through
   * {@code SparkRDDWriteClient#cluster}, because {@code SingleSparkJobExecutionStrategy#performClustering}
   * eagerly builds a Spark reader context, which needs the engine specific
   * {@code org.apache.spark.sql.adapter.*Adapter} that only lives in the downstream
   * hudi-spark-datasource modules and hence is not on this module's test classpath.
   */
  @Test
  public void testClusteringCopiesFileContentsAndPreservesMetadata() throws Exception {
    List<HoodieRecord> records = new ArrayList<>();
    records.addAll(bulkInsertBatch(60));
    records.addAll(bulkInsertBatch(40));
    List<HoodieBaseFile> sourceFiles = listBaseFiles();
    assertEquals(2, sourceFiles.size(), "Each bulk insert should have written its own file group");

    String clusteringInstant = (String) writeClient.scheduleClustering(Option.empty()).get();
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieClusteringPlan plan = ClusteringUtils.getClusteringPlan(
        metaClient, INSTANT_GENERATOR.getClusteringCommitRequestedInstant(clusteringInstant)).map(Pair::getRight).get();
    // Both preconditions of the strategy have to be met by the plan it is paired with.
    assertTrue(plan.getPreserveHoodieMetadata(), "The plan strategy must preserve the Hudi metadata fields");
    assertEquals(2, plan.getInputGroups().size(), "One clustering group per input file group");

    SparkExternalFileClusteringExecutionStrategy<HoodieAvroPayload> strategy =
        new ExternalFileClusteringTestExecutionStrategy<>(createTable(), context, config);
    List<StoragePath> clusteredPaths = new ArrayList<>();
    long totalWrites = 0;
    for (HoodieClusteringGroup group : plan.getInputGroups()) {
      ClusteringGroupInfo groupInfo = ClusteringGroupInfo.create(group);
      assertEquals(1, groupInfo.getOperations().size(), "The strategy only accepts single operation groups");
      HoodieBaseFile sourceFile = sourceFiles.stream()
          .filter(baseFile -> baseFile.getFileId().equals(groupInfo.getOperations().get(0).getFileId()))
          .findFirst().get();

      List<WriteStatus> writeStatuses = strategy.performClusteringForGroup(null, groupInfo,
          plan.getStrategy().getStrategyParams(), plan.getPreserveHoodieMetadata(), null,
          new LocalTaskContextSupplier(), clusteringInstant);

      assertEquals(1, writeStatuses.size(), "One clustering operation produces one write status");
      HoodieWriteStat stat = writeStatuses.get(0).getStat();
      assertEquals(PARTITION_PATH, stat.getPartitionPath());
      assertEquals(sourceFile.getCommitTime(), stat.getPrevCommit(), "Previous commit is derived from the input file");
      assertEquals(stat.getFileSizeInBytes(), stat.getTotalWriteBytes());
      assertTrue(stat.getFileSizeInBytes() > 0);
      assertTrue(stat.getRuntimeStats().getTotalCreateTime() >= 0);
      assertEquals(stat.getNumWrites(), stat.getNumInserts());
      totalWrites += stat.getNumWrites();

      StoragePath clusteredPath = new StoragePath(metaClient.getBasePath(), stat.getPath());
      assertTrue(storage.exists(clusteredPath), "Clustered file must land under the table base path");
      assertEquals(clusteringInstant, FSUtils.getCommitTime(clusteredPath.getName()));
      assertEquals(stat.getFileId(), FSUtils.getFileId(clusteredPath.getName()));
      assertEquals(writeStatuses.get(0).getFileId(), stat.getFileId());
      assertEquals(stat.getNumWrites(), new ParquetUtils().getRowCount(storage, clusteredPath));
      // The transformation is a plain file copy, so each clustered file must equal its input exactly.
      assertEquals(readParquetAsJson(sourceFile.getPath()), readParquetAsJson(clusteredPath.toString()),
          "Clustered file contents must equal the input file contents");
      clusteredPaths.add(clusteredPath);
    }
    assertEquals(records.size(), totalWrites, "Every input record must be accounted for");

    // The write handle registers each output file with a CREATE marker.
    Set<String> markerPaths = WriteMarkersFactory.get(MarkerType.DIRECT, createTable(), clusteringInstant).allMarkerFilePaths();
    clusteredPaths.forEach(clusteredPath ->
        assertTrue(markerPaths.stream().anyMatch(marker -> marker.startsWith(PARTITION_PATH + "/" + clusteredPath.getName())
            && marker.endsWith(IOType.CREATE.name())), "Expected a CREATE marker for " + clusteredPath + ", got " + markerPaths));

    List<Row> clusteredRows = sqlContext.read()
        .parquet(clusteredPaths.stream().map(StoragePath::toString).toArray(String[]::new)).collectAsList();
    assertEquals(records.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toSet()),
        clusteredRows.stream().map(row -> row.<String>getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD)).collect(Collectors.toSet()));
    // The metadata columns are copied as is, so the rows still carry the commit times that inserted them.
    assertEquals(sourceFiles.stream().map(HoodieBaseFile::getCommitTime).collect(Collectors.toSet()),
        clusteredRows.stream().map(row -> row.<String>getAs(HoodieRecord.COMMIT_TIME_METADATA_FIELD)).collect(Collectors.toSet()),
        "_hoodie_commit_time must not be rewritten by clustering");
  }

  @Test
  public void testPerformClusteringForGroupRejectsMetadataRewrite() {
    SparkExternalFileClusteringExecutionStrategy<HoodieAvroPayload> strategy =
        new ExternalFileClusteringTestExecutionStrategy<>(createTable(), context, config);
    ClusteringGroupInfo groupInfo = clusteringGroup(sliceInfo(newBaseFileName()));

    HoodieClusteringException exception = assertThrows(HoodieClusteringException.class,
        () -> strategy.performClusteringForGroup(null, groupInfo, Collections.emptyMap(), false, null,
            new LocalTaskContextSupplier(), WriteClientTestUtils.createNewInstantTime()));
    assertTrue(exception.getMessage().contains("preserveHoodieMetadata must be true"), exception.getMessage());
  }

  @Test
  public void testPerformClusteringForGroupRejectsMultipleOperations() {
    SparkExternalFileClusteringExecutionStrategy<HoodieAvroPayload> strategy =
        new ExternalFileClusteringTestExecutionStrategy<>(createTable(), context, config);
    ClusteringGroupInfo groupInfo = clusteringGroup(sliceInfo(newBaseFileName()), sliceInfo(newBaseFileName()));

    HoodieClusteringException exception = assertThrows(HoodieClusteringException.class,
        () -> strategy.performClusteringForGroup(null, groupInfo, Collections.emptyMap(), true, null,
            new LocalTaskContextSupplier(), WriteClientTestUtils.createNewInstantTime()));
    assertTrue(exception.getMessage().contains("Expect only one clustering operation during rewrite"), exception.getMessage());
  }

  @Test
  public void testPerformClusteringForGroupCleansUpPartialOutputOnFailure() throws IOException {
    FailingTransformStrategy strategy = new FailingTransformStrategy(createTable(), context, config);
    String dataFileName = newBaseFileName();
    ClusteringGroupInfo groupInfo = clusteringGroup(sliceInfo(dataFileName));

    HoodieClusteringException exception = assertThrows(HoodieClusteringException.class,
        () -> strategy.performClusteringForGroup(null, groupInfo, Collections.emptyMap(), true, null,
            new LocalTaskContextSupplier(), WriteClientTestUtils.createNewInstantTime()));
    assertTrue(exception.getMessage().contains("Failed to transform file: "), exception.getMessage());
    assertTrue(exception.getMessage().contains(dataFileName), exception.getMessage());
    assertNotNull(strategy.outputPath, "Transformation must have been handed the output path");
    assertFalse(storage.exists(strategy.outputPath), "Partial output file must be cleaned up");
  }

  @Test
  public void testPerformClusteringForGroupFailsWhenTransformationWritesNothing() {
    NoOpTransformStrategy strategy = new NoOpTransformStrategy(createTable(), context, config);
    ClusteringGroupInfo groupInfo = clusteringGroup(sliceInfo(newBaseFileName()));

    HoodieClusteringException exception = assertThrows(HoodieClusteringException.class,
        () -> strategy.performClusteringForGroup(null, groupInfo, Collections.emptyMap(), true, null,
            new LocalTaskContextSupplier(), WriteClientTestUtils.createNewInstantTime()));
    assertTrue(exception.getMessage().contains("Output file does not exist"), exception.getMessage());
  }

  private HoodieTable createTable() {
    return HoodieSparkTable.create(config, context, HoodieTableMetaClient.reload(metaClient));
  }

  private String newInstantTime() throws InterruptedException {
    // Hudi instant times have millisecond resolution, so keep back-to-back instants strictly ordered.
    Thread.sleep(2);
    return WriteClientTestUtils.createNewInstantTime();
  }

  private List<HoodieRecord> bulkInsertBatch(int numRecords) throws InterruptedException {
    String instantTime = newInstantTime();
    List<HoodieRecord> records = dataGen.generateInsertsForPartition(instantTime, numRecords, PARTITION_PATH);
    WriteClientTestUtils.startCommitWithTime(writeClient, instantTime);
    JavaRDD<WriteStatus> writeStatuses = writeClient.bulkInsert(jsc.parallelize(records, 1), instantTime);
    List<WriteStatus> statusList = writeStatuses.collect();
    assertNoWriteErrors(statusList);
    writeClient.commit(instantTime, jsc.parallelize(statusList, 1));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    return records;
  }

  private List<HoodieBaseFile> listBaseFiles() {
    return HoodieClientTestUtils.getLatestBaseFiles(basePath, storage, basePath + "/" + PARTITION_PATH + "/*");
  }

  private List<String> readParquetAsJson(String path) {
    List<String> rows = new ArrayList<>(sqlContext.read().parquet(path).toJSON().collectAsList());
    Collections.sort(rows);
    return rows;
  }

  /**
   * Builds a Hudi conforming base file name. The write handle derives the previous commit from the
   * old file name, so it has to be parseable even when the file itself is never read.
   */
  private String newBaseFileName() {
    return FSUtils.makeBaseFileName(WriteClientTestUtils.createNewInstantTime(), "1-0-1",
        FSUtils.createNewFileIdPfx(), HoodieFileFormat.PARQUET.getFileExtension());
  }

  private HoodieSliceInfo sliceInfo(String dataFileName) {
    return HoodieSliceInfo.newBuilder()
        .setPartitionPath(PARTITION_PATH)
        .setFileId(FSUtils.getFileId(dataFileName))
        .setDataFilePath(basePath + "/" + PARTITION_PATH + "/" + dataFileName)
        .setDeltaFilePaths(Collections.emptyList())
        .setBootstrapFilePath("")
        .build();
  }

  private static ClusteringGroupInfo clusteringGroup(HoodieSliceInfo... slices) {
    return ClusteringGroupInfo.create(HoodieClusteringGroup.newBuilder()
        .setSlices(Arrays.asList(slices))
        .setNumOutputFileGroups(1)
        .setMetrics(new HashMap<>())
        .build());
  }

  /**
   * Writes a partial output file and then fails, to exercise the cleanup path of the strategy.
   */
  private static class FailingTransformStrategy extends SparkExternalFileClusteringExecutionStrategy<HoodieAvroPayload> {

    private StoragePath outputPath;

    FailingTransformStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    @Override
    protected void transformFile(StoragePath oldFilePath, StoragePath newFilePath) {
      this.outputPath = newFilePath;
      try (OutputStream outputStream = getHoodieTable().getStorage().create(newFilePath)) {
        outputStream.write("partial output".getBytes(StandardCharsets.UTF_8));
      } catch (IOException e) {
        throw new HoodieIOException("Failed to write partial output file", e);
      }
      throw new RuntimeException("transformation failed");
    }
  }

  /**
   * Silently does nothing, to exercise the missing output file check of the write handle.
   */
  private static class NoOpTransformStrategy extends SparkExternalFileClusteringExecutionStrategy<HoodieAvroPayload> {

    NoOpTransformStrategy(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
      super(table, engineContext, writeConfig);
    }

    @Override
    protected void transformFile(StoragePath oldFilePath, StoragePath newFilePath) {
      // intentionally writes nothing
    }
  }
}
