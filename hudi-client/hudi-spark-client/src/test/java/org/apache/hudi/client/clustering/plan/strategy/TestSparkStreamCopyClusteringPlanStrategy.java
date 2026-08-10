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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.config.HoodieParquetConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.engine.LocalTaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.hadoop.HoodieAvroParquetWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;

import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Tests the schema aware grouping of {@link SparkStreamCopyClusteringPlanStrategy}. Files are written for real
 * because the grouping keys off the parquet schema hash read back from storage.
 */
public class TestSparkStreamCopyClusteringPlanStrategy {

  private static final String PARTITION_PATH = "p0";
  private static final String SCHEMA_ONE_FIELD = "{\"type\":\"record\",\"name\":\"triprec\",\"fields\":["
      + "{\"name\":\"_row_key\",\"type\":\"string\"}]}";
  private static final String SCHEMA_TWO_FIELDS = "{\"type\":\"record\",\"name\":\"triprec\",\"fields\":["
      + "{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"rider\",\"type\":\"string\"}]}";

  @TempDir
  Path tempDir;

  private HoodieSparkCopyOnWriteTable table;
  private HoodieLocalEngineContext context;

  @BeforeEach
  public void setUp() {
    table = Mockito.mock(HoodieSparkCopyOnWriteTable.class);
    context = new HoodieLocalEngineContext(HoodieTestUtils.getDefaultStorageConf());
    HoodieStorage storage = HoodieTestUtils.getStorage(tempDir.toAbsolutePath().toString());
    Mockito.when(table.getStorage()).thenReturn(storage);
  }

  @Test
  public void testSchemaAwareGroupingSplitsOnSchemaMismatch() throws IOException {
    SparkStreamCopyClusteringPlanStrategy planStrategy =
        new SparkStreamCopyClusteringPlanStrategy(table, context, defaultConfigBuilder().build());

    List<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSlice(400, writeParquetFile("001", SCHEMA_ONE_FIELD)));
    fileSlices.add(createFileSlice(300, writeParquetFile("002", SCHEMA_TWO_FIELDS)));

    Pair<Stream<HoodieClusteringGroup>, Boolean> result =
        planStrategy.buildClusteringGroupsForPartition(PARTITION_PATH, fileSlices);
    List<HoodieClusteringGroup> clusteringGroups = collect(result);

    // Both slices fit in one group size wise, but the schemas differ so a group break is forced.
    Assertions.assertEquals(2, clusteringGroups.size());
    Assertions.assertEquals(1, clusteringGroups.get(0).getSlices().size());
    Assertions.assertEquals(1, clusteringGroups.get(1).getSlices().size());
    Assertions.assertFalse(result.getRight());
  }

  @Test
  public void testSizeOnlyGroupingWhenSchemaEvolutionEnabled() throws IOException {
    HoodieWriteConfig config = defaultConfigBuilder()
        .withClusteringConfig(clusteringConfigBuilder()
            .withFileStitchingBinaryCopySchemaEvolutionEnabled(true)
            .build())
        .build();
    SparkStreamCopyClusteringPlanStrategy planStrategy =
        new SparkStreamCopyClusteringPlanStrategy(table, context, config);

    List<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSlice(400, writeParquetFile("001", SCHEMA_ONE_FIELD)));
    fileSlices.add(createFileSlice(300, writeParquetFile("002", SCHEMA_TWO_FIELDS)));

    List<HoodieClusteringGroup> clusteringGroups =
        collect(planStrategy.buildClusteringGroupsForPartition(PARTITION_PATH, fileSlices));

    // Schema evolution enabled falls back to the parent size only grouping, so the schema change is ignored.
    Assertions.assertEquals(1, clusteringGroups.size());
    Assertions.assertEquals(2, clusteringGroups.get(0).getSlices().size());
  }

  @Test
  public void testMaxNumGroupsReachedMarksPartialScheduling() throws IOException {
    HoodieWriteConfig config = defaultConfigBuilder()
        .withClusteringConfig(clusteringConfigBuilder()
            .withClusteringMaxBytesInGroup(500)
            .withClusteringMaxNumGroups(1)
            .build())
        .build();
    SparkStreamCopyClusteringPlanStrategy planStrategy =
        new SparkStreamCopyClusteringPlanStrategy(table, context, config);

    List<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSlice(400, writeParquetFile("001", SCHEMA_ONE_FIELD)));
    fileSlices.add(createFileSlice(300, writeParquetFile("002", SCHEMA_ONE_FIELD)));
    fileSlices.add(createFileSlice(200, writeParquetFile("003", SCHEMA_ONE_FIELD)));

    Pair<Stream<HoodieClusteringGroup>, Boolean> result =
        planStrategy.buildClusteringGroupsForPartition(PARTITION_PATH, fileSlices);
    List<HoodieClusteringGroup> clusteringGroups = collect(result);

    // The first group closes at 400 bytes which already hits the max group count, the rest are left behind.
    Assertions.assertEquals(1, clusteringGroups.size());
    Assertions.assertEquals(1, clusteringGroups.get(0).getSlices().size());
    Assertions.assertTrue(result.getRight(), "Remaining slices were not scheduled, expecting partial scheduling");
  }

  @Test
  public void testTrailingGroupHonoursSingleGroupClusteringConfig() throws IOException {
    String filePath = writeParquetFile("001", SCHEMA_ONE_FIELD);

    SparkStreamCopyClusteringPlanStrategy enabledStrategy =
        new SparkStreamCopyClusteringPlanStrategy(table, context, defaultConfigBuilder().build());
    List<HoodieClusteringGroup> enabledGroups = collect(enabledStrategy.buildClusteringGroupsForPartition(
        PARTITION_PATH, Collections.singletonList(createFileSlice(200, filePath))));
    Assertions.assertEquals(1, enabledGroups.size());
    Assertions.assertEquals(1, enabledGroups.get(0).getSlices().size());
    Assertions.assertEquals(1, enabledGroups.get(0).getNumOutputFileGroups());

    HoodieWriteConfig disabledConfig = defaultConfigBuilder()
        .withClusteringConfig(clusteringConfigBuilder()
            .withSingleGroupClusteringEnabled(false)
            .build())
        .build();
    SparkStreamCopyClusteringPlanStrategy disabledStrategy =
        new SparkStreamCopyClusteringPlanStrategy(table, context, disabledConfig);
    List<HoodieClusteringGroup> disabledGroups = collect(disabledStrategy.buildClusteringGroupsForPartition(
        PARTITION_PATH, Collections.singletonList(createFileSlice(200, filePath))));
    Assertions.assertEquals(0, disabledGroups.size());
  }

  @Test
  public void testSchemaHashFallsBackToZeroForMissingFiles() throws IOException {
    HoodieWriteConfig config = defaultConfigBuilder()
        .withClusteringConfig(clusteringConfigBuilder()
            // large enough that a slice without a base file (sized as one parquet max file size) still fits
            .withClusteringMaxBytesInGroup(1024 * 1024 * 1024L)
            .withClusteringTargetFileMaxBytes(1024 * 1024 * 1024L)
            .build())
        .build();
    SparkStreamCopyClusteringPlanStrategy planStrategy =
        new SparkStreamCopyClusteringPlanStrategy(table, context, config);

    List<FileSlice> fileSlices = new ArrayList<>();
    // no base file at all, sorts first since it is sized as one parquet max file size
    fileSlices.add(new FileSlice(PARTITION_PATH, "001", FSUtils.createNewFileId(FSUtils.createNewFileIdPfx(), 0)));
    fileSlices.add(createFileSlice(400, writeParquetFile("002", SCHEMA_ONE_FIELD)));
    fileSlices.add(createFileSlice(300, new StoragePath(
        tempDir.toAbsolutePath().toString(),
        FSUtils.makeBaseFileName("003", "1-0-1", FSUtils.createNewFileId(FSUtils.createNewFileIdPfx(), 0), ".parquet")).toString()));

    List<HoodieClusteringGroup> clusteringGroups =
        collect(planStrategy.buildClusteringGroupsForPartition(PARTITION_PATH, fileSlices));

    // hash 0 (no base file), the real schema hash and hash 0 again (unreadable file) - every neighbour mismatches
    Assertions.assertEquals(3, clusteringGroups.size());
    clusteringGroups.forEach(group -> Assertions.assertEquals(1, group.getSlices().size()));
  }

  @Test
  public void testSchemaHashFallsBackToZeroWhenStorageIsUnavailable() throws IOException {
    List<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSlice(400, writeParquetFile("001", SCHEMA_ONE_FIELD)));
    fileSlices.add(createFileSlice(300, writeParquetFile("002", SCHEMA_TWO_FIELDS)));

    Mockito.doThrow(new HoodieIOException("storage is down")).when(table).getStorage();
    SparkStreamCopyClusteringPlanStrategy planStrategy =
        new SparkStreamCopyClusteringPlanStrategy(table, context, defaultConfigBuilder().build());

    List<HoodieClusteringGroup> clusteringGroups =
        collect(planStrategy.buildClusteringGroupsForPartition(PARTITION_PATH, fileSlices));

    // both hashes fall back to 0, so the differing schemas no longer break the group
    Assertions.assertEquals(1, clusteringGroups.size());
    Assertions.assertEquals(2, clusteringGroups.get(0).getSlices().size());
  }

  @Test
  public void testGetStrategyParams() {
    SparkStreamCopyClusteringPlanStrategy withoutSortColumns =
        new SparkStreamCopyClusteringPlanStrategy(table, context, defaultConfigBuilder().build());
    Assertions.assertTrue(withoutSortColumns.getStrategyParams().isEmpty());

    HoodieWriteConfig config = defaultConfigBuilder()
        .withClusteringConfig(clusteringConfigBuilder()
            .withClusteringSortColumns("col1,col2")
            .build())
        .build();
    Map<String, String> params =
        new SparkStreamCopyClusteringPlanStrategy(table, context, config).getStrategyParams();
    Assertions.assertEquals(1, params.size());
    Assertions.assertEquals("col1,col2", params.get(PLAN_STRATEGY_SORT_COLUMNS.key()));
  }

  @Test
  public void testGenerateClusteringPlanOverridesExecutionStrategy() throws IOException {
    HoodieWriteConfig config = defaultConfigBuilder()
        .withClusteringConfig(clusteringConfigBuilder()
            .withClusteringSortColumns("col1")
            .build())
        .build();

    FileSlice slice1 = createFileSlice(400, writeParquetFile("001", SCHEMA_ONE_FIELD));
    FileSlice slice2 = createFileSlice(300, writeParquetFile("002", SCHEMA_ONE_FIELD));

    HoodieTableMetaClient metaClient = Mockito.mock(HoodieTableMetaClient.class);
    Mockito.when(table.getMetaClient()).thenReturn(metaClient);
    SyncableFileSystemView sliceView = Mockito.mock(SyncableFileSystemView.class);
    Mockito.when(table.getSliceView()).thenReturn(sliceView);
    Mockito.when(sliceView.getPendingCompactionOperations()).thenAnswer(invocation -> Stream.empty());
    Mockito.when(sliceView.getPendingLogCompactionOperations()).thenAnswer(invocation -> Stream.empty());
    Mockito.when(sliceView.getFileGroupsInPendingClustering()).thenAnswer(invocation -> Stream.empty());
    Mockito.when(sliceView.getLatestFileSlicesStateless(PARTITION_PATH))
        .thenReturn(Stream.of(slice1, slice2))
        .thenAnswer(invocation -> Stream.empty());

    SparkStreamCopyClusteringPlanStrategy planStrategy =
        new SparkStreamCopyClusteringPlanStrategy(table, context, config);
    Option<HoodieClusteringPlan> planOption =
        planStrategy.generateClusteringPlan(null, Lazy.eagerly(Collections.singletonList(PARTITION_PATH)));

    Assertions.assertTrue(planOption.isPresent());
    HoodieClusteringPlan plan = planOption.get();
    Assertions.assertEquals(HoodieClusteringConfig.SPARK_STREAM_COPY_CLUSTERING_EXECUTION_STRATEGY,
        plan.getStrategy().getStrategyClassName());
    Assertions.assertEquals("col1", plan.getStrategy().getStrategyParams().get(PLAN_STRATEGY_SORT_COLUMNS.key()));
    Assertions.assertTrue(plan.getPreserveHoodieMetadata());
    Assertions.assertEquals(1, plan.getInputGroups().size());
    Assertions.assertEquals(2, plan.getInputGroups().get(0).getSlices().size());

    // nothing eligible on the second pass, the empty plan of the parent is passed through untouched
    Assertions.assertFalse(planStrategy.generateClusteringPlan(
        null, Lazy.eagerly(Collections.singletonList(PARTITION_PATH))).isPresent());
  }

  private static HoodieWriteConfig.Builder defaultConfigBuilder() {
    return HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(clusteringConfigBuilder().build());
  }

  private static HoodieClusteringConfig.Builder clusteringConfigBuilder() {
    return HoodieClusteringConfig.newBuilder()
        .withClusteringPlanStrategyClass(HoodieClusteringConfig.SPARK_STREAM_COPY_CLUSTERING_PLAN_STRATEGY)
        .withClusteringMaxBytesInGroup(2000)
        .withClusteringTargetFileMaxBytes(1000)
        .withClusteringPlanSmallFileLimit(1000);
  }

  private static List<HoodieClusteringGroup> collect(Pair<Stream<HoodieClusteringGroup>, Boolean> result) {
    return result.getLeft().collect(Collectors.toList());
  }

  private FileSlice createFileSlice(long baseFileSize, String filePath) {
    HoodieBaseFile baseFile = new HoodieBaseFile(filePath);
    baseFile.setFileSize(baseFileSize);
    FileSlice fileSlice = new FileSlice(PARTITION_PATH, baseFile.getCommitTime(), baseFile.getFileId());
    fileSlice.setBaseFile(baseFile);
    return fileSlice;
  }

  /**
   * Writes an empty parquet file with the given schema, so that the strategy has a real schema hash to read back.
   */
  private String writeParquetFile(String commitTime, String schemaStr) throws IOException {
    Schema avroSchema = new Schema.Parser().parse(schemaStr);
    MessageType messageType = new AvroSchemaConverter().convert(avroSchema);
    HoodieAvroWriteSupport writeSupport = new HoodieAvroWriteSupport(
        messageType, HoodieSchema.fromAvroSchema(avroSchema), Option.empty(), new Properties());
    String fileName = FSUtils.makeBaseFileName(
        commitTime, "1-0-1", FSUtils.createNewFileId(FSUtils.createNewFileIdPfx(), 0), ".parquet");
    StoragePath filePath = new StoragePath(tempDir.resolve(fileName).toAbsolutePath().toString());
    HoodieParquetConfig<HoodieAvroWriteSupport> parquetConfig = new HoodieParquetConfig<>(
        writeSupport,
        CompressionCodecName.GZIP,
        ParquetWriter.DEFAULT_BLOCK_SIZE,
        ParquetWriter.DEFAULT_PAGE_SIZE,
        1024 * 1024 * 1024,
        HoodieTestUtils.getDefaultStorageConf(),
        0.1,
        true);
    try (HoodieAvroParquetWriter writer =
             new HoodieAvroParquetWriter(filePath, parquetConfig, commitTime, new LocalTaskContextSupplier(), true)) {
      // nothing to write, only the schema in the footer matters here
    }
    return filePath.toString();
  }
}
