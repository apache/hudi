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

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
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
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
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
    List<String> result = RecordLevelIndex.computeHoodieKeyFromFilters(
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
    List<String> result = RecordLevelIndex.computeHoodieKeyFromFilters(
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
    List<String> result = RecordLevelIndex.computeHoodieKeyFromFilters(
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
    List<String> result = RecordLevelIndex.computeHoodieKeyFromFilters(
        conf, metaClient, ExpressionEvaluators.fromExpression(expressions), recordKeyFields, ROW_TYPE_MULTI_KEYS, false);
    // For complex keys, the format should be key1:val1,key2:val2
    assertEquals(Arrays.asList("key1:val1" + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + "key2:val2"), result,
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
    List<String> result = RecordLevelIndex.computeHoodieKeyFromFilters(
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
    List<String> result = RecordLevelIndex.computeHoodieKeyFromFilters(
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
    result = RecordLevelIndex.computeHoodieKeyFromFilters(
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
    List<String> result = RecordLevelIndex.computeHoodieKeyFromFilters(
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