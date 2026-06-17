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

package org.apache.hudi.source.stats;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.record.HoodieRecordIndex;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.utils.TestConfigurations.ROW_DATA_TYPE_HOODIE_KEY_SPECIAL_DATA_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link RecordLevelIndex}.
 */
@ExtendWith(MockitoExtension.class)
public class TestRecordLevelIndex {
  private final HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
  private final HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);

  private static final DataType ROW_DATA_TYPE_MULTI_KEYS = DataTypes.ROW(
          DataTypes.FIELD("key1", DataTypes.VARCHAR(20)),// record key
          DataTypes.FIELD("key2", DataTypes.VARCHAR(20)),// record key
          DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
          DataTypes.FIELD("age", DataTypes.INT()),
          DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
          DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
      .notNull();
  private static final RowType ROW_TYPE_MULTI_KEYS = (RowType) ROW_DATA_TYPE_MULTI_KEYS.getLogicalType();

  @Test
  void testPartitionedRliGroupsKeysByShardWithinPartition() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    List<String> recordKeys = keysAcrossShards(2);
    RecordLevelIndex recordLevelIndex = createRecordLevelIndex(metadataTable, recordKeys);
    List<FileSlice> fileSlices = Arrays.asList(
        fileSlice("par1", "file1"),
        fileSlice("par1", "file2"));
    Map<String, List<FileSlice>> partitionedRliFileGroups = new HashMap<>();
    partitionedRliFileGroups.put("par1", Arrays.asList(
        fileSlice("par1", "rli-file1"),
        fileSlice("par1", "rli-file2")));
    when(metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX))
        .thenReturn(partitionedRliFileGroups);
    doReturn(HoodieListPairData.eager(Collections.singletonList(
        Pair.of(recordKeys.get(0), new HoodieRecordGlobalLocation("par1", "001", "file1")))))
        .when(metadataTable).readRecordIndexLocationsWithKeys(any(), eq(Option.of("par1")));

    List<FileSlice> result = recordLevelIndex.computeCandidateFileSlices(fileSlices);

    assertEquals(Collections.singletonList(fileSlices.get(0)), result);
    Set<Set<String>> expectedKeyGroups = recordKeys.stream()
        .collect(Collectors.groupingBy(key -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, 2)))
        .values().stream()
        .map(HashSet::new)
        .collect(Collectors.toSet());
    verify(metadataTable, times(expectedKeyGroups.size())).readRecordIndexLocationsWithKeys(
        argThat(keys -> expectedKeyGroups.contains(new HashSet<>(((HoodieData<String>) keys).collectAsList()))),
        eq(Option.of("par1")));
    verify(metadataTable, never()).readRecordIndexLocationsWithKeys(any());
  }

  @Test
  void testPartitionedRliPrunesAtPartitionThreshold() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    RecordLevelIndex recordLevelIndex = createRecordLevelIndex(metadataTable);
    List<FileSlice> fileSlices = Arrays.asList(
        fileSlice("par1", "file1"),
        fileSlice("par2", "file2"),
        fileSlice("par3", "file3"));
    when(metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX))
        .thenReturn(partitionedRliFileGroups("par1", "par2", "par3"));
    doAnswer(invocation -> {
      String partition = invocation.<Option<String>>getArgument(1).get();
      return HoodieListPairData.eager(Collections.singletonList(
          Pair.of("id1", new HoodieRecordGlobalLocation(
              partition, "001", "file" + partition.substring(3)))));
    })
        .when(metadataTable).readRecordIndexLocationsWithKeys(any(), any());

    List<FileSlice> result = recordLevelIndex.computeCandidateFileSlices(fileSlices);

    assertEquals(fileSlices, result);
    verify(metadataTable).readRecordIndexLocationsWithKeys(any(), eq(Option.of("par1")));
    verify(metadataTable).readRecordIndexLocationsWithKeys(any(), eq(Option.of("par2")));
    verify(metadataTable).readRecordIndexLocationsWithKeys(any(), eq(Option.of("par3")));
    verify(metadataTable, never()).readRecordIndexLocationsWithKeys(any());
  }

  @Test
  void testPartitionedRliSkipsPruningAbovePartitionThreshold() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    RecordLevelIndex recordLevelIndex = createRecordLevelIndex(metadataTable);
    List<FileSlice> fileSlices = Arrays.asList(
        fileSlice("par1", "file1"),
        fileSlice("par2", "file2"),
        fileSlice("par3", "file3"),
        fileSlice("par4", "file4"));

    List<FileSlice> result = recordLevelIndex.computeCandidateFileSlices(fileSlices);

    assertSame(fileSlices, result);
    verify(metadataTable, never()).getBucketizedFileGroupsForPartitionedRLI(any());
    verify(metadataTable, never()).readRecordIndexLocationsWithKeys(any(), any());
    verify(metadataTable, never()).readRecordIndexLocationsWithKeys(any());
  }

  @Test
  void testPartitionedRliFallsBackWhenPartitionLookupFails() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    RecordLevelIndex recordLevelIndex = createRecordLevelIndex(metadataTable);
    List<FileSlice> fileSlices = Arrays.asList(
        fileSlice("par1", "file1"),
        fileSlice("par2", "file2"));
    when(metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX))
        .thenReturn(partitionedRliFileGroups("par1", "par2"));
    doReturn(HoodieListPairData.eager(Collections.singletonList(
        Pair.of("id1", new HoodieRecordGlobalLocation("par1", "001", "file1")))))
        .when(metadataTable).readRecordIndexLocationsWithKeys(any(), eq(Option.of("par1")));
    doThrow(new RuntimeException("lookup failure"))
        .when(metadataTable).readRecordIndexLocationsWithKeys(any(), eq(Option.of("par2")));

    List<FileSlice> result = recordLevelIndex.computeCandidateFileSlices(fileSlices);

    assertSame(fileSlices, result);
    verify(metadataTable).readRecordIndexLocationsWithKeys(any(), eq(Option.of("par2")));
  }

  @Test
  void testPartitionedRliSkipsPruningForNoCandidateFiles() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    RecordLevelIndex recordLevelIndex = createRecordLevelIndex(metadataTable);
    List<FileSlice> fileSlices = Collections.emptyList();

    List<FileSlice> result = recordLevelIndex.computeCandidateFileSlices(fileSlices);

    assertSame(fileSlices, result);
    verify(metadataTable, never()).getBucketizedFileGroupsForPartitionedRLI(any());
    verify(metadataTable, never()).readRecordIndexLocationsWithKeys(any(), any());
    verify(metadataTable, never()).readRecordIndexLocationsWithKeys(any());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1})
  void testRejectsNonPositivePartitionLookupThreshold(int maxPartitions) {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.READ_DATA_SKIPPING_RLI_PARTITIONS_MAX_NUM, maxPartitions);

    assertThrows(IllegalArgumentException.class,
        () -> new RecordLevelIndex("", conf, metaClient, Collections.singletonList("id1")));
  }

  @Test
  void testGlobalRliMatchesPartitionAndFileId() {
    HoodieTableMetadata metadataTable = mock(HoodieTableMetadata.class);
    GlobalRecordLevelIndex recordLevelIndex = createGlobalRecordLevelIndex(metadataTable);
    List<FileSlice> fileSlices = Arrays.asList(
        fileSlice("par1", "file1"),
        fileSlice("par2", "file1"));
    when(metadataTable.readRecordIndexLocationsWithKeys(any()))
        .thenReturn(HoodieListPairData.eager(Collections.singletonList(
            Pair.of("id1", new HoodieRecordGlobalLocation("par1", "001", "file1")))));

    List<FileSlice> result = recordLevelIndex.computeCandidateFileSlices(fileSlices);

    assertEquals(Collections.singletonList(fileSlices.get(0)), result);
    verify(metadataTable).readRecordIndexLocationsWithKeys(any());
    verify(metadataTable, never()).readRecordIndexLocationsWithKeys(any(), any());
  }

  private RecordLevelIndex createRecordLevelIndex(HoodieTableMetadata metadataTable) {
    return createRecordLevelIndex(metadataTable, Collections.singletonList("id1"));
  }

  private RecordLevelIndex createRecordLevelIndex(HoodieTableMetadata metadataTable, List<String> recordKeys) {
    mockAvailableRecordIndex();
    RecordLevelIndex recordLevelIndex = spy(
        new RecordLevelIndex("", new Configuration(), metaClient, recordKeys));
    lenient().doReturn(metadataTable).when(recordLevelIndex).getMetadataTable();
    return recordLevelIndex;
  }

  private GlobalRecordLevelIndex createGlobalRecordLevelIndex(HoodieTableMetadata metadataTable) {
    mockAvailableRecordIndex();
    GlobalRecordLevelIndex recordLevelIndex = spy(
        new GlobalRecordLevelIndex("", new Configuration(), metaClient, Collections.singletonList("id1")));
    lenient().doReturn(metadataTable).when(recordLevelIndex).getMetadataTable();
    return recordLevelIndex;
  }

  private void mockAvailableRecordIndex() {
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isMetadataTableAvailable()).thenReturn(true);
    when(tableConfig.getMetadataPartitions()).thenReturn(Collections.singleton(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX));
  }

  private static HoodieIndexDefinition indexDefinition(boolean partitioned) {
    return HoodieIndexDefinition.newBuilder()
        .withIndexName(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
        .withIndexType(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX)
        .withIndexOptions(Collections.singletonMap(
            HoodieRecordIndex.IS_PARTITIONED_OPTION, String.valueOf(partitioned)))
        .build();
  }

  private static FileSlice fileSlice(String partition, String fileId) {
    return new FileSlice(new HoodieFileGroupId(partition, fileId), "001");
  }

  private static Map<String, List<FileSlice>> partitionedRliFileGroups(String... partitions) {
    Map<String, List<FileSlice>> partitionedRliFileGroups = new HashMap<>();
    Arrays.stream(partitions).forEach(partition ->
        partitionedRliFileGroups.put(partition, Collections.singletonList(fileSlice(partition, "rli-" + partition))));
    return partitionedRliFileGroups;
  }

  private static List<String> keysAcrossShards(int fileGroupCount) {
    Map<Integer, String> keysByShard = new HashMap<>();
    for (int i = 0; keysByShard.size() < fileGroupCount; i++) {
      String key = "id" + i;
      keysByShard.putIfAbsent(HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, fileGroupCount), key);
    }
    return keysByShard.values().stream().collect(Collectors.toList());
  }

  private List<ExpressionEvaluators.Evaluator> createColumnStatsProbe(BuiltInFunctionDefinition func, String refName, List<String> vals) {
    List<ResolvedExpression> args = vals.stream().map(
        val -> new ValueLiteralExpression(val, DataTypes.STRING().notNull())).collect(Collectors.toList());
    args.add(0, new FieldReferenceExpression(refName, DataTypes.STRING(), 0, 0));
    CallExpression callExpression = CallExpression.permanent(func, args, DataTypes.BOOLEAN());
    return ExpressionEvaluators.fromExpression(Collections.singletonList(callExpression));
  }

  private List<ExpressionEvaluators.Evaluator> createOrColumnStatsProbe(String refName, List<String> vals) {
    // Create multiple EQUALS expressions and join them with OR
    CallExpression outerExpr = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(new FieldReferenceExpression(refName, DataTypes.STRING(), 0, 0),
            new ValueLiteralExpression(vals.get(0), DataTypes.STRING().notNull())),
        DataTypes.BOOLEAN());
    CallExpression leftExpr = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(new FieldReferenceExpression(refName, DataTypes.STRING(), 0, 0),
            new ValueLiteralExpression(vals.get(1), DataTypes.STRING().notNull())),
        DataTypes.BOOLEAN());
    CallExpression rightExpr = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(new FieldReferenceExpression(refName, DataTypes.STRING(), 0, 0),
            new ValueLiteralExpression(vals.get(2), DataTypes.STRING().notNull())),
        DataTypes.BOOLEAN());

    CallExpression orExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.OR,
        Arrays.asList(leftExpr, rightExpr),
        DataTypes.BOOLEAN());
    CallExpression outerOrExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.OR,
        Arrays.asList(outerExpr, orExpression),
        DataTypes.BOOLEAN());
    return ExpressionEvaluators.fromExpression(Collections.singletonList(outerOrExpression));
  }

  @Test
  public void testComputeHoodieKeyFromFiltersWithSimpleRecordKey() {
    // Setup mock table config with single record key field
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"uuid"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[]{"partition"}));

    Configuration conf = new Configuration();

    // Test with EqualTo evaluator on record key
    List<ExpressionEvaluators.Evaluator> evaluators = createColumnStatsProbe(
        BuiltInFunctionDefinitions.EQUALS, "uuid", Collections.singletonList("id1"));
    String[] recordKeyFields = {"uuid"};
    List<String> result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, evaluators, recordKeyFields, TestConfigurations.ROW_TYPE, false);
    assertEquals(Collections.singletonList("id1"), result, "Should return the simple record key value");
  }

  @Test
  public void testComputeHoodieKeyFromFiltersWithInOperator() {
    // Setup mock table config with single record key field
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"uuid"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[]{"partition"}));

    Configuration conf = new Configuration();

    // Test with IN operator
    List<ExpressionEvaluators.Evaluator> evaluators = createColumnStatsProbe(BuiltInFunctionDefinitions.IN, "uuid", Arrays.asList("id1", "id2", "id3"));
    String[] recordKeyFields = {"uuid"};
    List<String> result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, evaluators, recordKeyFields, TestConfigurations.ROW_TYPE, false);
    assertEquals(Arrays.asList("id1", "id2", "id3"), result, "Should return all the IN operator values");
  }

  @Test
  public void testComputeHoodieKeyFromFiltersWithOrOperator() {
    // Setup mock table config with single record key field
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"uuid"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[]{"partition"}));

    Configuration conf = new Configuration();

    // Test with OR operator (which should be converted to IN)
    List<ExpressionEvaluators.Evaluator> evaluators = createOrColumnStatsProbe("uuid", Arrays.asList("id1", "id2", "id3"));
    String[] recordKeyFields = {"uuid"};
    List<String> result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, evaluators, recordKeyFields, TestConfigurations.ROW_TYPE, false);
    // Note: OR with two values "id1" and "id2" should result in the literals from both evaluators
    assertEquals(Arrays.asList("id1", "id2", "id3"), result, "Should return values from OR operator");
  }

  @Test
  public void testComputeHoodieKeyFromFiltersWithComplexRecordKey() {
    // Setup mock table config with multiple record key fields
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"key1", "key2"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[]{"partition"}));

    Configuration conf = new Configuration();

    // Test with filters on multiple record key fields
    List<ResolvedExpression> expressions = Arrays.asList(
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS,
            Arrays.asList(new FieldReferenceExpression("key1", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression("val1", DataTypes.STRING().notNull())),
            DataTypes.BOOLEAN()),
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS,
            Arrays.asList(new FieldReferenceExpression("key2", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression("val2", DataTypes.STRING().notNull())),
            DataTypes.BOOLEAN())
    );
    String[] recordKeyFields = {"key1", "key2"};
    List<String> result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, ExpressionEvaluators.fromExpression(expressions), recordKeyFields, ROW_TYPE_MULTI_KEYS, false);
    // For complex keys, the format should be key1:val1,key2:val2
    assertEquals(Arrays.asList("key1:val1" + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + "key2:val2"), result,
        "Should return composite key with complex record keys");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testComputeHoodieKeyFromFiltersWithSpecialTypeRecordKey(boolean consistentLogicalTimestampEnabled) {
    // Setup mock table config with multiple record key fields
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"f_timestamp", "f_decimal"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[]{"partition"}));

    Configuration conf = new Configuration();
    // Test with filters on multiple record key fields
    List<ResolvedExpression> expressions = Arrays.asList(
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS,
            Arrays.asList(
                new FieldReferenceExpression("f_timestamp", DataTypes.TIMESTAMP(3), 0, 0),
                new ValueLiteralExpression(LocalDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")), DataTypes.TIMESTAMP(3).notNull())
            ),
            DataTypes.BOOLEAN()),
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS,
            Arrays.asList(
                new FieldReferenceExpression("f_decimal", DataTypes.DECIMAL(3, 2), 0, 2),
                new ValueLiteralExpression(new BigDecimal("1.1"), DataTypes.DECIMAL(3, 2).notNull())
            ),
            DataTypes.BOOLEAN())
    );
    String[] recordKeyFields = {"f_timestamp", "f_decimal"};
    RowType rowType = (RowType) ROW_DATA_TYPE_HOODIE_KEY_SPECIAL_DATA_TYPE.getLogicalType();
    List<String> result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, ExpressionEvaluators.fromExpression(expressions), recordKeyFields, rowType, consistentLogicalTimestampEnabled);
    String expectedTimestampVal = RowDataKeyGen.getRecordKey(TimestampData.fromEpochMillis(1), "f_timestamp", consistentLogicalTimestampEnabled);
    assertEquals(Arrays.asList("f_timestamp:" + expectedTimestampVal + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + "f_decimal:1.10"), result,
        "Should return composite key with complex record keys");
  }

  @Test
  public void testComputeHoodieKeyFromFiltersWithMultipleSimpleKeys() {
    // Setup mock table config with single record key field
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"uuid"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[]{"partition"}));

    Configuration conf = new Configuration();

    // Test with multiple equal filters on same record key field
    // This scenario might not be typical, but testing the method behavior
    List<ResolvedExpression> expressions = Arrays.asList(
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS,
            Arrays.asList(new FieldReferenceExpression("uuid", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression("id1", DataTypes.STRING().notNull())),
            DataTypes.BOOLEAN()),
        CallExpression.permanent(
            BuiltInFunctionDefinitions.EQUALS,
            Arrays.asList(new FieldReferenceExpression("uuid", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression("id2", DataTypes.STRING().notNull())),
            DataTypes.BOOLEAN())
    );
    String[] recordKeyFields = {"uuid"};
    List<String> result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, ExpressionEvaluators.fromExpression(expressions), recordKeyFields, TestConfigurations.ROW_TYPE, false);
    // This should return both values
    assertEquals(Arrays.asList("id1", "id2"), result, "Should return multiple values for same field");
  }

  @Test
  public void testComputeHoodieKeyFromFiltersWithEmptyResult() {
    // Setup mock table config with single record key field
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"uuid"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[]{"partition"}));

    Configuration conf = new Configuration();

    // Test with filter on non-record key field - should return empty list
    List<ExpressionEvaluators.Evaluator> evaluators = createColumnStatsProbe(
        BuiltInFunctionDefinitions.EQUALS, "nonKeyField", Collections.singletonList("val1"));
    String[] recordKeyFields = {"uuid"};
    List<String> result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, evaluators, recordKeyFields, TestConfigurations.ROW_TYPE, false);
    assertEquals(Collections.emptyList(), result, "Should return empty list when filtering on non-record key field");

    CallExpression keyExpr = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(new FieldReferenceExpression("uuid", DataTypes.STRING(), 0, 0),
            new ValueLiteralExpression("key1", DataTypes.STRING().notNull())),
        DataTypes.BOOLEAN());
    CallExpression nonKeyExpr = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(new FieldReferenceExpression("name", DataTypes.STRING(), 0, 0),
            new ValueLiteralExpression("Bob", DataTypes.STRING().notNull())),
        DataTypes.BOOLEAN());
    CallExpression orExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.OR,
        Arrays.asList(keyExpr, nonKeyExpr),
        DataTypes.BOOLEAN());

    evaluators = ExpressionEvaluators.fromExpression(Collections.singletonList(orExpression));
    result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, evaluators, recordKeyFields, TestConfigurations.ROW_TYPE, false);
    assertEquals(Collections.emptyList(), result, "Should return empty list when filtering on or predicate including multiple fields");
  }

  @Test
  public void testComputeHoodieKeyFromFiltersWithComplexRecordKeyMultipleValues() {
    // Setup mock table config with multiple record key fields
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.getRecordKeyFields()).thenReturn(Option.of(new String[]{"key1", "key2"}));
    when(tableConfig.getPartitionFields()).thenReturn(Option.of(new String[]{"partition"}));

    Configuration conf = new Configuration();

    // Test with multiple values for each record key field to test combinations
    // key1 in ['val1', 'val2'] AND key2 in ['val3', 'val4'] should produce 4 combinations
    List<ResolvedExpression> expressions = Arrays.asList(
        CallExpression.permanent(
            BuiltInFunctionDefinitions.IN,
            Arrays.asList(new FieldReferenceExpression("key1", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression("val1", DataTypes.STRING().notNull()),
                new ValueLiteralExpression("val2", DataTypes.STRING().notNull())),
            DataTypes.BOOLEAN()),
        CallExpression.permanent(
            BuiltInFunctionDefinitions.IN,
            Arrays.asList(new FieldReferenceExpression("key2", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression("val3", DataTypes.STRING().notNull()),
                new ValueLiteralExpression("val4", DataTypes.STRING().notNull())),
            DataTypes.BOOLEAN())
    );
    String[] recordKeyFields = {"key1", "key2"};
    List<String> result = BaseRecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, ExpressionEvaluators.fromExpression(expressions), recordKeyFields, ROW_TYPE_MULTI_KEYS, false);
    // Should have 4 combinations: (val1,val3), (val1,val4), (val2,val3), (val2,val4)
    List<String> expected = Arrays.asList(
        "key1:val1" + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + "key2:val3",
        "key1:val1" + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + "key2:val4",
        "key1:val2" + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + "key2:val3",
        "key1:val2" + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + "key2:val4"
    );
    assertEquals(expected, result, "Should return all combinations of complex record keys");
  }
}
