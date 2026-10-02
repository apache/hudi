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

import org.apache.hudi.adapter.DataStreamScanProviderAdapter;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.function.SerializableSupplier;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem;
import org.apache.hudi.hadoop.fs.RecordingLocalFileSystem.Call;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.source.enumerator.HoodieSplitEnumeratorState;
import org.apache.hudi.source.enumerator.HoodieStaticSplitEnumerator;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.source.reader.BatchRecords;
import org.apache.hudi.source.reader.HoodieRecordEmitter;
import org.apache.hudi.source.reader.HoodieRecordWithPosition;
import org.apache.hudi.source.reader.function.HoodieSplitReaderFunction;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.HoodieSourceSplitComparator;
import org.apache.hudi.source.split.SerializableComparator;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.HoodieTableSource;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.SerializableSchema;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");

    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient = StreamerUtil.createMetaClient(conf);

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

    // get full splits
    HoodieSource<RowData> source1 = createHoodieSourceWithPruner(conf, metaClient, null, null);
    List<HoodieSourceSplit> fullSplits = source1.createBatchHoodieSplits();

    // pruned by partition stats
    HoodieSource<RowData> source2 = createHoodieSourceWithPruner(conf, metaClient, partitionPruner, null);
    List<HoodieSourceSplit> splits2 = source2.createBatchHoodieSplits();
    assertTrue(splits2.size() < fullSplits.size());

    // pruned by column stats
    HoodieSource<RowData> source3 = createHoodieSourceWithPruner(conf, metaClient, null, columnStatsProbe);
    List<HoodieSourceSplit> splits3 = source3.createBatchHoodieSplits();
    assertTrue(splits3.size() < fullSplits.size());
  }

  @Test
  public void testCreateBatchHoodieSplitsWithBucketPruner() throws Exception {
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.BUCKET.name());
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    metaClient = StreamerUtil.createMetaClient(conf);

    HoodieSource<RowData> source1 = createHoodieSourceWithPruner(conf, metaClient, null, null);
    List<HoodieSourceSplit> fullSplits = source1.createBatchHoodieSplits();

    int targetBucketId = 1;
    String targetBucketIdStr = BucketIdentifier.bucketIdStr(targetBucketId);
    HoodieSource<RowData> source = createHoodieSourceWithPruner(
        conf, metaClient, null, null, partitionPath -> targetBucketId);
    List<HoodieSourceSplit> prunedSplits = source.createBatchHoodieSplits();

    assertTrue(prunedSplits.size() < fullSplits.size());
    prunedSplits.forEach(split -> assertTrue(
        split.getFileId().contains(targetBucketIdStr),
        "Pruned split should belong to bucket " + targetBucketId));
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

  @Test
  @SuppressWarnings("unchecked")
  public void testConstructorRejectsNullCollaborators() {
    HoodieScanContext scanContext = mock(HoodieScanContext.class);
    SerializableSupplier<SplitReaderFunction<RowData>> readerSupplier = mock(SerializableSupplier.class);
    SerializableComparator<HoodieSourceSplit> comparator = mock(SerializableComparator.class);
    HoodieTableMetaClient client = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    HoodieRecordEmitter<RowData> emitter = mock(HoodieRecordEmitter.class);
    when(client.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getTableName()).thenReturn("test_table");

    assertThrows(IllegalArgumentException.class,
        () -> new HoodieSource<>(null, readerSupplier, comparator, client, emitter));
    assertThrows(IllegalArgumentException.class,
        () -> new HoodieSource<>(scanContext, null, comparator, client, emitter));
    assertThrows(IllegalArgumentException.class,
        () -> new HoodieSource<>(scanContext, readerSupplier, null, client, emitter));
    assertThrows(IllegalArgumentException.class,
        () -> new HoodieSource<>(scanContext, readerSupplier, comparator, null, emitter));
    assertThrows(IllegalArgumentException.class,
        () -> new HoodieSource<>(scanContext, readerSupplier, comparator, client, null));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCreateAndRestoreStaticEnumerator() throws Exception {
    metaClient = HoodieTestUtils.init(tempDir.getAbsolutePath(), HoodieTableType.COPY_ON_WRITE);
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.COPY_ON_WRITE.name());
    HoodieSource<RowData> source = createHoodieSource(conf, metaClient);
    SplitEnumeratorContext<HoodieSourceSplit> context = mock(SplitEnumeratorContext.class);
    when(context.currentParallelism()).thenReturn(1);

    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> created =
        source.createEnumerator(context);
    HoodieSplitEnumeratorState state = new HoodieSplitEnumeratorState(
        Collections.emptyList(), Option.empty(), Option.empty());
    SplitEnumerator<HoodieSourceSplit, HoodieSplitEnumeratorState> restored =
        source.restoreEnumerator(context, state);

    assertInstanceOf(HoodieStaticSplitEnumerator.class, created);
    assertInstanceOf(HoodieStaticSplitEnumerator.class, restored);
  }

  // Helper methods

  /**
   * The reader functions of the source read their splits without touching the table's .hoodie folder: the table state
   * is captured when the source is created and shipped with the functions.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testReadSplitsWithoutMetaFolderAccess(boolean streaming) throws Exception {
    conf.set(FlinkOptions.TABLE_TYPE, HoodieTableType.MERGE_ON_READ.name());
    conf.set(FlinkOptions.READ_SOURCE_V2_ENABLED, true);
    conf.set(FlinkOptions.READ_AS_STREAMING, streaming);
    conf.set(FlinkOptions.READ_START_COMMIT, FlinkOptions.START_COMMIT_EARLIEST);
    conf.setString("hadoop." + RecordingLocalFileSystem.FILE_IMPL_KEY, RecordingLocalFileSystem.class.getName());
    conf.setString("hadoop." + RecordingLocalFileSystem.DISABLE_CACHE_KEY, "true");
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    TestData.writeData(TestData.DATA_SET_UPDATE_INSERT, conf);

    Map<String, List<Integer>> ages = readAgesWithSourceV2(streaming, true);

    assertTrue(RecordingLocalFileSystem.count(Call.inScope()) > 0, "The readers must go through the recording file system");
    assertEquals(0, RecordingLocalFileSystem.count(Call.inScope().and(Call.underMetaFolder())),
        () -> RecordingLocalFileSystem.describe(Call.inScope().and(Call.underMetaFolder())));
    assertEquals(Collections.singletonList(24), ages.get("id1"));
  }

  /**
   * A bounded read of a merge-on-read table of version 6 skips the log block of a delta commit that did not complete
   * and reads the one of a completed delta commit, through the committed instants captured when the source is created.
   */
  @Test
  void testBoundedReadOfVersionSixSkipsUncommittedLogBlock() throws Exception {
    conf.set(FlinkOptions.READ_SOURCE_V2_ENABLED, true);
    TestData.writeVersionSixWithUncommittedLogBlock(conf);

    Map<String, List<Integer>> ages = readAgesWithSourceV2(false, false);
    assertEquals(Collections.singletonList(24), ages.get("id1"));
    assertEquals(Collections.singletonList(34), ages.get("id2"));
  }

  /**
   * Plans the read with a table source wired as in a job, then reads every split with a copy of the reader function
   * as shipped to a task, returning the ages read per record key.
   */
  private Map<String, List<Integer>> readAgesWithSourceV2(boolean streaming, boolean recordTaskAccesses) throws Exception {
    HoodieTableSource tableSource = new HoodieTableSource(
        SerializableSchema.create(TestConfigurations.TABLE_SCHEMA), tablePath,
        Arrays.asList(conf.get(FlinkOptions.PARTITION_PATH_FIELD).split(",")), "default-par", conf);
    DataStream<RowData> stream = ((DataStreamScanProviderAdapter) tableSource.getScanRuntimeProvider(
        mock(ScanTableSource.ScanContext.class))).produceDataStream(StreamExecutionEnvironment.getExecutionEnvironment());
    @SuppressWarnings("unchecked")
    HoodieSource<RowData> source = (HoodieSource<RowData>) ((SourceTransformation<?, ?, ?>) stream.getTransformation()).getSource();
    List<HoodieSourceSplit> splits = streaming
        ? new ArrayList<>(IncrementalInputSplits.builder().conf(conf).path(new Path(tempDir.getAbsolutePath()))
            .rowType(TestConfigurations.ROW_TYPE).build()
            .inputHoodieSourceSplits(StreamerUtil.createMetaClient(conf), null, false).getSplits())
        : source.createBatchHoodieSplits();
    assertFalse(splits.isEmpty());
    Field supplierField = HoodieSource.class.getDeclaredField("readerFunctionSupplier");
    supplierField.setAccessible(true);
    @SuppressWarnings("unchecked")
    SerializableSupplier<SplitReaderFunction<RowData>> supplier = InstantiationUtil.clone(
        (SerializableSupplier<SplitReaderFunction<RowData>>) supplierField.get(source), getClass().getClassLoader());
    SplitReaderFunction<RowData> function = supplier.get();

    RecordingLocalFileSystem.reset();
    Map<String, List<Integer>> ages = new HashMap<>();
    try (RecordingLocalFileSystem.Scope ignored = RecordingLocalFileSystem.withScope(() -> recordTaskAccesses)) {
      for (HoodieSourceSplit split : splits) {
        function.open(split);
        BatchRecords<RowData> batch;
        while ((batch = function.readBatch(split, 1024, () -> false)) != null) {
          HoodieRecordWithPosition<RowData> record;
          while ((record = batch.nextRecordFromSplit()) != null) {
            ages.computeIfAbsent(record.record().getString(0).toString(), key -> new ArrayList<>()).add(record.record().getInt(2));
          }
        }
        function.closeCurrentSplit();
      }
      function.close();
    }
    return ages;
  }

  private HoodieSource<RowData> createHoodieSource(Configuration conf, HoodieTableMetaClient metaClient) {
    return createHoodieSourceWithPruner(conf, metaClient, null);
  }

  private HoodieSource<RowData> createHoodieSourceWithPruner(
      Configuration conf,
      HoodieTableMetaClient metaClient,
      PartitionPruners.PartitionPruner partitionPruner) {
    return createHoodieSourceWithPruner(conf, metaClient, partitionPruner, null);
  }

  private HoodieSource<RowData> createHoodieSourceWithPruner(
      Configuration conf,
      HoodieTableMetaClient metaClient,
      PartitionPruners.PartitionPruner partitionPruner,
      ColumnStatsProbe columnStatsProbe) {
    return createHoodieSourceWithPruner(conf, metaClient, partitionPruner, columnStatsProbe, null);
  }

  private HoodieSource<RowData> createHoodieSourceWithPruner(
      Configuration conf,
      HoodieTableMetaClient metaClient,
      PartitionPruners.PartitionPruner partitionPruner,
      ColumnStatsProbe columnStatsProbe,
      Function<String, Integer> partitionBucketIdFunc) {
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
        .columnStatsProbe(columnStatsProbe)
        .partitionBucketIdFunc(partitionBucketIdFunc)
        .build();
    HoodieSchema schema = HoodieSchemaConverter.convertToSchema(rowType);
    HadoopStorageConfiguration hadoopConf = new HadoopStorageConfiguration(HadoopConfigurations.getHadoopConf(conf));
    InternalSchemaManager internalSchemaManager = InternalSchemaManager.get(hadoopConf, this.metaClient);

    return new HoodieSource<>(
        scanContext,
        () -> new HoodieSplitReaderFunction(
            conf,
            schema, // schema will be resolved from table
            schema, // required schema
            internalSchemaManager,
            conf.get(FlinkOptions.MERGE_TYPE),
            Collections.emptyList(),
            false),
        new HoodieSourceSplitComparator(),
        metaClient,
        new HoodieRecordEmitter<>());
  }
}
