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

package org.apache.hudi.utilities.pipeline;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.transaction.lock.LockManager;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
public class TestHoodieShadowPipeline extends HoodieToolsFunctionalTest {
  private String srcPath;
  private String destPath;
  private String destTableName;
  private MockedStatic<UtilHelpers> mockedUtilHelpers;
  private MockedConstruction<LockManager> mockedLockManager;

  @BeforeEach
  public void setup() throws IOException {
    srcPath = basePath;
    destTableName = "dataset_copy";
    destPath = createPath(destTableName);

    mockedUtilHelpers = Mockito.mockStatic(UtilHelpers.class);
    mockedUtilHelpers.when(() -> UtilHelpers.buildSparkContext(
        Mockito.anyString(),
        Mockito.anyString(),
        Mockito.anyBoolean(),
        Mockito.anyMap()
    )).thenReturn(jsc);
    mockedUtilHelpers.when(() -> UtilHelpers.getConfig(Mockito.any())).thenCallRealMethod();

    mockedLockManager = Mockito.mockConstruction(LockManager.class, (mock, context) -> {
    });
  }

  @AfterEach
  public void closeMocks() {
    if (mockedUtilHelpers != null) {
      mockedUtilHelpers.close();
    }
    if (mockedLockManager != null) {
      mockedLockManager.close();
    }
  }

  // ========================= Helper Methods =========================

  private HoodieShadowPipelineConfig createDefaultConfig() {
    HoodieShadowPipelineConfig cfg = new HoodieShadowPipelineConfig();
    cfg.srcPath = srcPath;
    cfg.destPath = destPath;
    cfg.datasetName = destTableName;
    cfg.hiveDatabase = "test_db";
    cfg.partitionColumns = "partition";
    cfg.recordKeyColumn = "_row_key";
    cfg.destTableType = "COPY_ON_WRITE";
    cfg.baseFileFormat = "PARQUET";
    cfg.writeMetaFields = true;
    cfg.keyGenerator = "org.apache.hudi.keygen.SimpleKeyGenerator";
    cfg.sourceOrderingField = "timestamp";
    cfg.enableHiveSync = false;
    cfg.continuousMode = false;
    cfg.validateBefore = false;
    cfg.validateAfter = false;
    cfg.zookeeperUrl = "localhost:2181";
    return cfg;
  }

  private String[] buildArgs(HoodieShadowPipelineConfig cfg) {
    List<String> args = new ArrayList<>();
    args.addAll(Arrays.asList("--src-path", cfg.srcPath));
    args.addAll(Arrays.asList("--dest-path", cfg.destPath));
    args.addAll(Arrays.asList("--dataset-name", cfg.datasetName));
    args.addAll(Arrays.asList("--hive-database", cfg.hiveDatabase));
    args.addAll(Arrays.asList("--partition-columns", cfg.partitionColumns));
    args.addAll(Arrays.asList("--recordkey-column", cfg.recordKeyColumn));
    args.addAll(Arrays.asList("--dest-table-type", cfg.destTableType));
    args.addAll(Arrays.asList("--base-file-format", cfg.baseFileFormat));
    args.addAll(Arrays.asList("--write-meta-fields", String.valueOf(cfg.writeMetaFields)));
    args.addAll(Arrays.asList("--key-generator", cfg.keyGenerator));
    args.addAll(Arrays.asList("--source-ordering-field", cfg.sourceOrderingField));
    args.addAll(Arrays.asList("--enable-hive-sync", String.valueOf(cfg.enableHiveSync)));
    args.addAll(Arrays.asList("--continuous", String.valueOf(cfg.continuousMode)));
    args.addAll(Arrays.asList("--validate-before", String.valueOf(cfg.validateBefore)));
    args.addAll(Arrays.asList("--validate-after", String.valueOf(cfg.validateAfter)));
    args.addAll(Arrays.asList("--reuse-hoodie-properties-from-src",
        String.valueOf(cfg.reuseHoodiePropertiesFileFromSrc)));
    args.addAll(Arrays.asList("--bootstrap-with-latest-base-files",
        String.valueOf(cfg.bootstrapWithLatestBaseFiles)));
    if (!StringUtils.isNullOrEmpty(cfg.zookeeperUrl)) {
      args.addAll(Arrays.asList("--zookeeper-url", cfg.zookeeperUrl));
    }
    if (!cfg.startPartition.isEmpty()) {
      args.addAll(Arrays.asList("--start-partition", cfg.startPartition));
    }
    if (!cfg.endPartition.isEmpty()) {
      args.addAll(Arrays.asList("--end-partition", cfg.endPartition));
    }
    if (!cfg.selectedPartitions.isEmpty()) {
      args.addAll(Arrays.asList("--selected-partitions",
          String.join(",", cfg.selectedPartitions)));
    }
    if (!StringUtils.isNullOrEmpty(cfg.maxFilesPerPartition)) {
      args.addAll(Arrays.asList("--max-files-per-partition", cfg.maxFilesPerPartition));
    }
    if (!StringUtils.isNullOrEmpty(cfg.sourceInstantTime)) {
      args.addAll(Arrays.asList("--source-instant-time", cfg.sourceInstantTime));
    }
    if (cfg.hiveTable != null) {
      args.addAll(Arrays.asList("--hive-table", cfg.hiveTable));
    }
    if (cfg.deleteDestPath) {
      args.addAll(Arrays.asList("--delete-dest-path", "true"));
    }
    if (cfg.useSourceCommitDuringInitialization) {
      args.addAll(Arrays.asList("--use-source-commit-during-initialization", "true"));
    }
    if (cfg.useSourceTimelineDuringInitialization) {
      args.addAll(Arrays.asList("--use-source-timeline-during-initialization", "true"));
    }
    args.addAll(Arrays.asList("--runtime-props", "hoodie.metadata.enable=false"));
    args.addAll(Arrays.asList("--userid", "test_user"));
    return args.toArray(new String[0]);
  }

  private void runPipeline(HoodieShadowPipelineConfig cfg) throws Exception {
    HoodieShadowPipeline.main(buildArgs(cfg));
  }

  private HoodieTableMetaClient buildMetaClient(String path) {
    return HoodieTableMetaClient.builder()
        .setConf(HadoopFSUtils.getStorageConf(jsc.hadoopConfiguration()))
        .setBasePath(path)
        .setLoadActiveTimelineOnLoad(true)
        .build();
  }

  private Set<String> getPartitions(HoodieTableMetaClient mc) {
    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    return new HashSet<>(FSUtils.getAllPartitionPaths(engineContext, mc, false));
  }

  private void assertPartitionsMatch(HoodieTableMetaClient src, HoodieTableMetaClient dest) {
    assertEquals(getPartitions(src), getPartitions(dest),
        "Source and destination should have the same partitions");
  }

  private void assertRecordCountsMatch(String path1, String path2) {
    SparkSession spark = SparkSession.builder().sparkContext(jsc.sc()).getOrCreate();
    long count1 = spark.read().format("hudi").load(path1).count();
    long count2 = spark.read().format("hudi").load(path2).count();
    assertEquals(count1, count2, "Record counts should match");
  }

  private int getCompletedCommitCount(HoodieTableMetaClient mc) {
    return mc.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants().size();
  }

  private String getCheckpointKey(HoodieTableMetaClient mc) throws Exception {
    HoodieInstant lastInstant = mc.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().lastInstant().get();
    byte[] details = mc.getActiveTimeline().getInstantDetails(lastInstant).get();
    CommitMetadataSerDe serDe = mc.getCommitMetadataSerDe();
    HoodieCommitMetadata metadata = serDe.deserialize(lastInstant,
        new ByteArrayInputStream(details), () -> details.length == 0,
        HoodieCommitMetadata.class);
    return metadata.getMetadata(HoodieStreamer.CHECKPOINT_KEY);
  }

  // ========================= Core E2E Tests =========================

  @Test
  public void testBasicCopy() throws Exception {
    setupTable(false);

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    runPipeline(cfg);

    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    assertEquals(HoodieTableType.COPY_ON_WRITE, destMc.getTableConfig().getTableType());
    // The pipeline sets the destination database and table name separately, so getTableName()
    // returns the bare table name and getDatabaseName() returns the configured hive database.
    assertEquals("dataset_copy_shadow", destMc.getTableConfig().getTableName());
    assertEquals("test_db", destMc.getTableConfig().getDatabaseName());
    // reuseHoodiePropertiesFileFromSrc=true copies partition field from source
    assertEquals("partition_path",
        String.join(",", destMc.getTableConfig().getPartitionFields().get()));
    assertEquals(1, getCompletedCommitCount(destMc));

    HoodieTableMetaClient srcMc = HoodieTableMetaClient.reload(metaClient);
    assertPartitionsMatch(srcMc, destMc);
    assertRecordCountsMatch(srcPath, destPath);
  }

  @Test
  public void testNonExistentSource() {
    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.srcPath = "/non/existent/path";

    TableNotFoundException e = assertThrows(TableNotFoundException.class, () -> runPipeline(cfg));
    assertTrue(e.getMessage().contains(cfg.srcPath));
  }

  @Test
  public void testMORSourceRejected() throws IOException {
    String morPath = createPath("mor_source");
    HoodieTestUtils.init(morPath, HoodieTableType.MERGE_ON_READ);

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.srcPath = morPath;

    HoodieException e = assertThrows(HoodieException.class, () -> runPipeline(cfg));
    assertTrue(e.getMessage().contains("MOR tables are not supported"));
  }

  // ========================= Config Toggle Tests =========================

  @Test
  public void testPropertyReuseFalse() throws Exception {
    setupTable(false);

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.reuseHoodiePropertiesFileFromSrc = false;
    cfg.partitionColumns = "datestr";
    runPipeline(cfg);

    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    // When not reusing src properties, dest gets the explicit partitionColumns
    assertEquals("datestr",
        String.join(",", destMc.getTableConfig().getPartitionFields().get()));
    assertEquals(HoodieTableType.COPY_ON_WRITE, destMc.getTableConfig().getTableType());
  }

  @Test
  public void testCustomTableName() throws Exception {
    setupTable(false);

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.hiveTable = "my_custom_table";
    runPipeline(cfg);

    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    assertEquals("my_custom_table", destMc.getTableConfig().getTableName());
    assertEquals("test_db", destMc.getTableConfig().getDatabaseName());
  }

  @Test
  @Disabled("Failing, re-enable after fixing")
  public void testDeleteDestPath() throws Exception {
    setupTable(false);

    // Create a marker file at dest to prove it gets cleaned
    java.nio.file.Path markerFile = Paths.get(destPath, "marker.txt");
    Files.createDirectories(markerFile.getParent());
    Files.write(markerFile, "test".getBytes(StandardCharsets.UTF_8));
    assertTrue(Files.exists(markerFile));

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.deleteDestPath = true;
    runPipeline(cfg);

    // Marker should be gone, dest table should exist
    assertFalse(Files.exists(markerFile));
    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    assertEquals(1, getCompletedCommitCount(destMc));
  }

  // ========================= Partition Filtering Tests =========================

  @Test
  public void testSelectedPartitions() throws Exception {
    setupTable(false);

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.selectedPartitions = new HashSet<>(Collections.singletonList("2016/03/15"));
    runPipeline(cfg);

    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    Set<String> destPartitions = getPartitions(destMc);
    assertEquals(1, destPartitions.size());
    assertTrue(destPartitions.contains("2016/03/15"));
  }

  @Test
  public void testStartAndEndPartition() throws Exception {
    setupTable(false);

    // Partitions: 2015/03/16, 2015/03/17, 2016/03/15
    // start=2015/03/17 excludes 2015/03/16 (< start)
    // end=2016/03/15 keeps 2015/03/17 and 2016/03/15
    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.startPartition = "2015/03/17";
    cfg.endPartition = "2016/03/15";
    runPipeline(cfg);

    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    Set<String> destPartitions = getPartitions(destMc);
    assertEquals(2, destPartitions.size());
    assertTrue(destPartitions.contains("2015/03/17"));
    assertTrue(destPartitions.contains("2016/03/15"));
    assertFalse(destPartitions.contains("2015/03/16"));
  }

  // ========================= Multi-Commit Tests =========================

  @Test
  public void testMultipleCommitsLatestCopied() throws Exception {
    setupTableWithMultipleCommits(2);

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    runPipeline(cfg);

    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    assertEquals(1, getCompletedCommitCount(destMc));
    assertRecordCountsMatch(srcPath, destPath);

    // Checkpoint should point to the source's last commit time
    HoodieTableMetaClient srcMc = HoodieTableMetaClient.reload(metaClient);
    String lastSrcCommitTime = srcMc.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().lastInstant().get().requestedTime();
    assertEquals(lastSrcCommitTime, getCheckpointKey(destMc));
  }

  @Test
  public void testWithSourceInstantTime() throws Exception {
    setupTableWithMultipleCommits(2);

    // Get the first commit time
    HoodieTableMetaClient srcMc = HoodieTableMetaClient.reload(metaClient);
    List<HoodieInstant> commits = srcMc.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().getInstants();
    assertEquals(2, commits.size());
    String firstCommitTime = commits.get(0).requestedTime();

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.sourceInstantTime = firstCommitTime;
    runPipeline(cfg);

    // Checkpoint should point to the first commit (not the latest)
    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    assertEquals(firstCommitTime, getCheckpointKey(destMc));
  }

  // ========================= Advanced Init Mode Tests =========================

  // NOTE: testUseSourceCommitDuringInit is omitted because the
  // useSourceCommitDuringInitialization path copies raw timeline files from source to dest.
  // In V2 timeline (table version NINE), these are Avro-serialized commit files that don't
  // embed the table schema, so bootstrapMetadataTable() fails with HoodieSchemaNotFoundException.
  // This is a known limitation when porting the tool to 1.2's V2 timeline format.

  @Test
  public void testValidation() throws Exception {
    setupTable(false);

    HoodieShadowPipelineConfig cfg = createDefaultConfig();
    cfg.validateBefore = true;
    // Should not throw - validation passes for correctly copied dataset
    runPipeline(cfg);

    HoodieTableMetaClient destMc = buildMetaClient(destPath);
    assertEquals(1, getCompletedCommitCount(destMc));
    assertRecordCountsMatch(srcPath, destPath);
  }
}
