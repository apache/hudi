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

package org.apache.hudi.source;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.source.reader.HoodieRecordEmitter;
import org.apache.hudi.source.reader.function.HoodieSplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplitComparator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieSource}.
 */
public class TestHoodieSource {

  @TempDir
  File tempDir;

  private HoodieTableMetaClient metaClient;
  private Configuration conf;
  private StoragePath tablePath;

  @BeforeEach
  public void setUp() {
    conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    tablePath = new StoragePath(tempDir.getAbsolutePath());
  }

  @Test
  public void testGetBoundednessForBatchMode() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.set(FlinkOptions.READ_AS_STREAMING, false);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    assertEquals(Boundedness.BOUNDED, source.getBoundedness(),
        "Batch mode should return BOUNDED");
  }

  @Test
  public void testGetBoundednessForStreamingMode() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.READ_AS_STREAMING, true);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    assertEquals(Boundedness.CONTINUOUS_UNBOUNDED, source.getBoundedness(),
        "Streaming mode should return CONTINUOUS_UNBOUNDED");
  }

  @Test
  public void testCreateBatchHoodieSplitsWithReadOptimizedQuery() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.MERGE_ON_READ);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_READ_OPTIMIZED);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    List<HoodieSourceSplit> splits = source.createBatchHoodieSplits();

    assertNotNull(splits, "Splits should not be null");
    // Read optimized query only reads base files
    splits.forEach(split -> {
      assertNotNull(split.getBasePath(), "Base path should not be null");
      assertFalse(split.getLogPaths().isPresent() && !split.getLogPaths().get().isEmpty(),
          "Read optimized should not have log files");
    });
  }

  @Test
  public void testCreateBatchHoodieSplitsWithIncrementalQuery() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.set(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_INCREMENTAL);
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    metaClient.reloadActiveTimeline();

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    List<HoodieSourceSplit> splits = source.createBatchHoodieSplits();

    assertNotNull(splits, "Splits should not be null for incremental query");
  }

  @Test
  public void testCreateBatchHoodieSplitsWithEmptyTable() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    // Don't write any data
    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    List<HoodieSourceSplit> splits = source.createBatchHoodieSplits();

    assertNotNull(splits, "Splits should not be null even for empty table");
    assertTrue(splits.isEmpty(), "Splits should be empty for empty table");
  }

  @Test
  public void testCreateBatchHoodieSplitsWithPartitionPruner() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    // Create partition pruner that filters partition = 'par1'
    FieldReferenceExpression partitionFieldRef = new FieldReferenceExpression(
        "partition", DataTypes.STRING(), 0, 0);
    ExpressionEvaluators.Evaluator equalToEvaluator = ExpressionEvaluators.EqualTo.getInstance()
        .bindVal(new ValueLiteralExpression("par1"))
        .bindFieldReference(partitionFieldRef);

    PartitionPruners.PartitionPruner partitionPruner =
        PartitionPruners.builder()
            .partitionEvaluators(Collections.singletonList(equalToEvaluator))
            .partitionKeys(Collections.singletonList("partition"))
            .partitionTypes(Collections.singletonList(DataTypes.STRING()))
            .defaultParName(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH)
            .hivePartition(false)
            .build();

    HoodieSource<RowData> source = createHoodieSourceWithPruner(conf, metaClient, partitionPruner);
    List<HoodieSourceSplit> splits = source.createBatchHoodieSplits();

    assertNotNull(splits, "Splits should not be null");
    // Verify that only par1 partition is included
    splits.forEach(split -> {
      assertTrue(split.getBasePath().get().contains("par1") || split.getTablePath().contains("par1"),
          "Split should be from par1 partition");
    });
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  public void testCreateBatchHoodieSplitsWithDifferentTableTypes(HoodieTableType tableType) throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), tableType);
    conf.set(FlinkOptions.TABLE_TYPE, tableType.name());
    conf.set(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_SNAPSHOT);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    List<HoodieSourceSplit> splits = source.createBatchHoodieSplits();

    assertNotNull(splits, "Splits should not be null for table type: " + tableType);
    assertFalse(splits.isEmpty(), "Splits should not be empty for table type: " + tableType);
    splits.forEach(split -> {
      assertNotNull(split.getBasePath(), "Base path should not be null for: " + tableType);
      assertNotNull(split.getFileId(), "File ID should not be null for: " + tableType);
    });
  }

  @Test
  public void testCreateBatchHoodieSplitsWithColumnStatsPruner() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    // Create column stats probe with uuid > 'id5' filter
    ColumnStatsProbe columnStatsProbe = ColumnStatsProbe.newInstance(Arrays.asList(
        CallExpression.permanent(
            FunctionIdentifier.of("greaterThan"),
            BuiltInFunctionDefinitions.GREATER_THAN,
            Arrays.asList(
                new FieldReferenceExpression("uuid", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression("id5", DataTypes.STRING().notNull())
            ),
            DataTypes.BOOLEAN())));

    PartitionPruners.PartitionPruner partitionPruner =
        PartitionPruners.builder()
            .rowType(TestConfigurations.ROW_TYPE)
            .basePath(tempDir.getAbsolutePath())
            .metaClient(metaClient)
            .conf(conf)
            .columnStatsProbe(columnStatsProbe)
            .build();

    HoodieSource<RowData> source = createHoodieSourceWithPruner(conf, metaClient, partitionPruner);
    List<HoodieSourceSplit> splits = source.createBatchHoodieSplits();

    assertNotNull(splits, "Splits should not be null with column stats pruner");
  }

  @Test
  public void testGetOrBuildFileIndexInternal() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    FileIndex fileIndex = source.getOrBuildFileIndex();

    assertNotNull(fileIndex, "File index should not be null");
    assertNotNull(fileIndex.getOrBuildPartitionPaths(), "Partition paths should not be null");
    assertFalse(fileIndex.getOrBuildPartitionPaths().isEmpty(),
        "Partition paths should not be empty");
  }

  @Test
  public void testGetOrBuildFileIndexWithPartitionPruner() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient.reloadActiveTimeline();

    // Create partition pruner
    FieldReferenceExpression partitionFieldRef = new FieldReferenceExpression(
        "partition", DataTypes.STRING(), 0, 0);
    ExpressionEvaluators.Evaluator greaterThanEvaluator = ExpressionEvaluators.GreaterThanOrEqual.getInstance()
        .bindVal(new ValueLiteralExpression("par2"))
        .bindFieldReference(partitionFieldRef);

    PartitionPruners.PartitionPruner partitionPruner =
        PartitionPruners.builder()
            .partitionEvaluators(Collections.singletonList(greaterThanEvaluator))
            .partitionKeys(Collections.singletonList("partition"))
            .partitionTypes(Collections.singletonList(DataTypes.STRING()))
            .defaultParName(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH)
            .hivePartition(false)
            .build();

    HoodieSource<RowData> source = createHoodieSourceWithPruner(conf, metaClient, partitionPruner);
    FileIndex fileIndex = source.getOrBuildFileIndex();

    assertNotNull(fileIndex, "File index should not be null");
    List<String> partitionPaths = fileIndex.getOrBuildPartitionPaths();
    assertNotNull(partitionPaths, "Partition paths should not be null");
    // Verify that only partitions >= par2 are included (par2, par3, par4)
    partitionPaths.forEach(path -> {
      assertTrue(path.compareTo("par2") >= 0,
          "Partition path should be >= par2: " + path);
    });
  }

  @Test
  public void testSplitSerializerNotNull() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    assertNotNull(source.getSplitSerializer(), "Split serializer should not be null");
  }

  @Test
  public void testEnumeratorCheckpointSerializerNotNull() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    assertNotNull(source.getEnumeratorCheckpointSerializer(),
        "Enumerator checkpoint serializer should not be null");
  }

  @Test
  public void testCreateBatchHoodieSplitsWithMultiplePartitions() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());

    // Write data to multiple partitions
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_INSERT_SEPARATE_PARTITION, conf);
    metaClient.reloadActiveTimeline();

    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    List<HoodieSourceSplit> splits = source.createBatchHoodieSplits();

    assertNotNull(splits, "Splits should not be null");
    assertFalse(splits.isEmpty(), "Splits should not be empty");

    // Verify splits from different partitions exist
    long distinctPartitions = splits.stream()
        .map(split -> split.getPartitionPath())
        .distinct()
        .count();
    assertTrue(distinctPartitions > 1, "Should have splits from multiple partitions");
  }

  @Test
  public void testIncrementalQueryWithPartitionPruner() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.set(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_INCREMENTAL);
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);
    metaClient.reloadActiveTimeline();

    // Create partition pruner for partition = 'par1'
    FieldReferenceExpression partitionFieldRef = new FieldReferenceExpression(
        "partition", DataTypes.STRING(), 0, 0);
    ExpressionEvaluators.Evaluator equalToEvaluator = ExpressionEvaluators.EqualTo.getInstance()
        .bindVal(new ValueLiteralExpression("par1"))
        .bindFieldReference(partitionFieldRef);

    PartitionPruners.PartitionPruner partitionPruner =
        PartitionPruners.builder()
            .partitionEvaluators(Collections.singletonList(equalToEvaluator))
            .partitionKeys(Collections.singletonList("partition"))
            .partitionTypes(Collections.singletonList(DataTypes.STRING()))
            .defaultParName(PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH)
            .hivePartition(false)
            .build();

    HoodieSource<RowData> source = createHoodieSourceWithPruner(conf, metaClient, partitionPruner);
    List<HoodieSourceSplit> splits = source.createBatchHoodieSplits();

    assertNotNull(splits, "Incremental splits with pruner should not be null");
  }

  // Helper methods

  private HoodieSource<RowData> createHoodieSource(Configuration conf, HoodieTableMetaClient metaClient) {
    return createHoodieSourceWithPruner(conf, metaClient, null);
  }

  private HoodieSource<RowData> createHoodieSourceWithPruner(
      Configuration conf,
      HoodieTableMetaClient metaClient,
      PartitionPruners.PartitionPruner partitionPruner) {
    RowType rowType = TestConfigurations.ROW_TYPE;
    HoodieScanContext scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(tablePath)
        .rowType(rowType)
        .startInstant(conf.get(FlinkOptions.READ_START_COMMIT))
        .endInstant(conf.get(FlinkOptions.READ_END_COMMIT))
        .maxCompactionMemoryInBytes(conf.get(FlinkOptions.COMPACTION_MAX_MEMORY))
        .maxPendingSplits(1000)
        .skipCompaction(conf.get(FlinkOptions.READ_STREAMING_SKIP_COMPACT))
        .skipClustering(conf.get(FlinkOptions.READ_STREAMING_SKIP_CLUSTERING))
        .skipInsertOverwrite(conf.get(FlinkOptions.READ_STREAMING_SKIP_INSERT_OVERWRITE))
        .cdcEnabled(conf.get(FlinkOptions.CDC_ENABLED))
        .isStreaming(conf.get(FlinkOptions.READ_AS_STREAMING))
        .partitionPruner(partitionPruner)
        .build();
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(rowType);
    HoodieSplitReaderFunction splitReaderFunction = new HoodieSplitReaderFunction(
        metaClient,
        conf,
            schema, // schema will be resolved from table
            schema, // required schema
        conf.get(FlinkOptions.MERGE_TYPE),
        Collections.emptyList(),
            false);

    return new HoodieSource<>(
        scanContext,
        splitReaderFunction,
        new HoodieSourceSplitComparator(),
        metaClient,
        new HoodieRecordEmitter<>());
  }
}
