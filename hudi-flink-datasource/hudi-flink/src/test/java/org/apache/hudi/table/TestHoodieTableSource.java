/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
import org.apache.hudi.util.SerializableSchema;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.avro.Schema;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.fs.Path;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for HoodieTableSource.
 */
public class TestHoodieTableSource {
  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieTableSource.class);

  private Configuration conf;

  @TempDir
  File tempFile;

  void beforeEach() throws Exception {
    final String path = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(path);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
  }

  @Test
  void testGetReadPaths() throws Exception {
    beforeEach();
    HoodieTableSource tableSource = getEmptyStreamingSource();
    List<StoragePathInfo> fileList = tableSource.getReadFiles();
    assertNotNull(fileList);
    assertThat(fileList.size(), is(4));
    // apply partition pruning
    FieldReferenceExpression partRef = new FieldReferenceExpression("partition", DataTypes.STRING(), 4, 4);
    ValueLiteralExpression partLiteral = new ValueLiteralExpression("par1", DataTypes.STRING().notNull());
    CallExpression partFilter = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(partRef, partLiteral),
        DataTypes.BOOLEAN());
    HoodieTableSource tableSource2 = getEmptyStreamingSource();
    tableSource2.applyFilters(Arrays.asList(partFilter));

    List<StoragePathInfo> fileList2 = tableSource2.getReadFiles();
    assertNotNull(fileList2);
    assertThat(fileList2.size(), is(1));
  }

  @Test
  void testGetInputFormat() throws Exception {
    beforeEach();
    // write some data to let the TableSchemaResolver get the right instant
    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    HoodieTableSource tableSource = new HoodieTableSource(
        SerializableSchema.create(TestConfigurations.TABLE_SCHEMA),
        new StoragePath(tempFile.getPath()),
        Arrays.asList(conf.get(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);
    InputFormat<RowData, ?> inputFormat = tableSource.getInputFormat();
    assertThat(inputFormat, is(instanceOf(FileInputFormat.class)));
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    inputFormat = tableSource.getInputFormat();
    assertThat(inputFormat, is(instanceOf(MergeOnReadInputFormat.class)));
    conf.set(FlinkOptions.QUERY_TYPE, FlinkOptions.QUERY_TYPE_INCREMENTAL);
    assertDoesNotThrow(
        (ThrowingSupplier<? extends InputFormat<RowData, ?>>) tableSource::getInputFormat,
        "Query type: 'incremental' should be supported");
  }

  @Test
  void testGetTableAvroSchema() {
    HoodieTableSource tableSource = getEmptyStreamingSource();
    assertNull(tableSource.getMetaClient(), "Streaming source with empty table path is allowed");
    final String schemaFields = tableSource.getTableAvroSchema().getFields().stream()
        .map(Schema.Field::name)
        .collect(Collectors.joining(","));
    final String expected = "_hoodie_commit_time,"
        + "_hoodie_commit_seqno,"
        + "_hoodie_record_key,"
        + "_hoodie_partition_path,"
        + "_hoodie_file_name,"
        + "uuid,name,age,ts,partition";
    assertThat(schemaFields, is(expected));
  }

  @Test
  void testDataSkippingFilterShouldBeNotNullWhenTableSourceIsCopied() {
    HoodieTableSource tableSource = getEmptyStreamingSource();
    FieldReferenceExpression ref = new FieldReferenceExpression("uuid", DataTypes.STRING(), 0, 0);
    ValueLiteralExpression literal = new ValueLiteralExpression("1", DataTypes.STRING().notNull());
    ResolvedExpression filterExpr = CallExpression.permanent(
        BuiltInFunctionDefinitions.IN,
        Arrays.asList(ref, literal),
        DataTypes.BOOLEAN());
    List<ResolvedExpression> expectedFilters = Collections.singletonList(filterExpr);
    tableSource.applyFilters(expectedFilters);
    HoodieTableSource copiedSource = (HoodieTableSource) tableSource.copy();
    ColumnStatsProbe columnStatsProbe = copiedSource.getColumnStatsProbe();
    assertNotNull(columnStatsProbe);
  }

  @ParameterizedTest
  @MethodSource("filtersAndResults")
  void testDataSkippingWithPartitionStatsPruning(List<ResolvedExpression> filters, List<String> expectedPartitions) throws Exception {
    final String path = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(path);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "true");
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    HoodieTableSource hoodieTableSource = createHoodieTableSource(conf);
    hoodieTableSource.applyFilters(filters);
    assertEquals(expectedPartitions, hoodieTableSource.getReadPartitions());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBucketPruning(boolean hiveStylePartitioning) throws Exception {
    String tablePath1 = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    Configuration conf1 = TestConfigurations.getDefaultConf(tablePath1);
    conf1.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf1.set(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);

    // test single primary key filtering
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf1);
    HoodieTableSource tableSource1 = createHoodieTableSource(conf1);
    tableSource1.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));

    int numBuckets = (int)FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.defaultValue();

    assertThat(tableSource1.getDataBucketFunc().get().apply(numBuckets), is(1));
    List<StoragePathInfo> fileList = tableSource1.getReadFiles();
    assertThat("Files should be pruned by bucket id 1", fileList.size(), CoreMatchers.is(2));

    // test multiple primary keys filtering
    Configuration conf2 = conf1.clone();
    String tablePath2 = new Path(tempFile.getAbsolutePath(), "tbl2").toString();
    conf2.set(FlinkOptions.PATH, tablePath2);
    conf2.set(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf2.set(FlinkOptions.KEYGEN_TYPE, "COMPLEX");
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf2);
    HoodieTableSource tableSource2 = createHoodieTableSource(conf2);
    tableSource2.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1"),
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny")));
    assertThat(tableSource2.getDataBucketFunc().get().apply(numBuckets), is(3));
    List<StoragePathInfo> fileList2 = tableSource2.getReadFiles();
    assertThat("Files should be pruned by bucket id 3", fileList2.size(), CoreMatchers.is(3));

    // apply the filters in different order and test again.
    tableSource2.reset();
    tableSource2.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny"),
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));
    assertThat(tableSource2.getDataBucketFunc().get().apply(numBuckets), is(3));
    assertThat("Files should be pruned by bucket id 3", tableSource2.getReadFiles().size(),
        CoreMatchers.is(3));

    // test partial primary keys filtering
    Configuration conf3 = conf1.clone();
    String tablePath3 = new Path(tempFile.getAbsolutePath(), "tbl3").toString();
    conf3.set(FlinkOptions.PATH, tablePath3);
    conf3.set(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf3.set(FlinkOptions.KEYGEN_TYPE, "COMPLEX");
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf3);
    HoodieTableSource tableSource3 = createHoodieTableSource(conf3);
    tableSource3.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));

    assertTrue(tableSource3.getDataBucketFunc().isEmpty());
    List<StoragePathInfo> fileList3 = tableSource3.getReadFiles();
    assertThat("Partial pk filtering does not prune any files", fileList3.size(),
        CoreMatchers.is(7));

    // test single primary keys filtering together with non-primary key predicate
    Configuration conf4 = conf1.clone();
    String tablePath4 = new Path(tempFile.getAbsolutePath(), "tbl4").toString();
    conf4.set(FlinkOptions.PATH, tablePath4);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf4);
    HoodieTableSource tableSource4 = createHoodieTableSource(conf4);
    tableSource4.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1"),
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny")));

    assertThat(tableSource4.getDataBucketFunc().get().apply(numBuckets), is(1));
    List<StoragePathInfo> fileList4 = tableSource4.getReadFiles();
    assertThat("Files should be pruned by bucket id 1", fileList4.size(), CoreMatchers.is(2));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBucketPruningSpecialKeyDataType(boolean logicalTimestamp) throws Exception {
    String tablePath1 = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    Configuration conf1 = TestConfigurations.getDefaultConf(tablePath1, TestConfigurations.ROW_DATA_TYPE_HOODIE_KEY_SPECIAL_DATA_TYPE);
    final String f1 = "f_timestamp";
    conf1.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf1.set(FlinkOptions.RECORD_KEY_FIELD, f1);
    conf1.set(FlinkOptions.PRECOMBINE_FIELD, f1);
    conf1.removeConfig(FlinkOptions.PARTITION_PATH_FIELD);
    conf1.setString(KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), logicalTimestamp + "");
    int numBuckets = (int)FlinkOptions.BUCKET_INDEX_NUM_BUCKETS.defaultValue();

    // test timestamp filtering
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT_HOODIE_KEY_SPECIAL_DATA_TYPE, conf1);
    HoodieTableSource tableSource1 = createHoodieTableSource(conf1);
    tableSource1.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr(f1, 0, DataTypes.TIMESTAMP(3).notNull(),
            LocalDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))));

    assertThat(tableSource1.getDataBucketFunc().get().apply(numBuckets), is(logicalTimestamp ? 1 : 0));
    List<StoragePathInfo> fileList = tableSource1.getReadFiles();
    assertThat("Files should be pruned", fileList.size(), CoreMatchers.is(1));

    // test date filtering
    Configuration conf2 = conf1.clone();
    final String f2 = "f_date";
    String tablePath2 = new Path(tempFile.getAbsolutePath(), "tbl2").toString();
    conf2.set(FlinkOptions.PATH, tablePath2);
    conf2.set(FlinkOptions.RECORD_KEY_FIELD, f2);
    conf2.set(FlinkOptions.PRECOMBINE_FIELD, f2);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT_HOODIE_KEY_SPECIAL_DATA_TYPE, conf2);
    HoodieTableSource tableSource2 = createHoodieTableSource(conf2);
    tableSource2.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr(f2, 1, DataTypes.DATE().notNull(), LocalDate.ofEpochDay(1))));

    assertThat(tableSource2.getDataBucketFunc().get().apply(numBuckets), is(1));
    List<StoragePathInfo> fileList2 = tableSource2.getReadFiles();
    assertThat("Files should be pruned", fileList2.size(), CoreMatchers.is(1));

    // test decimal filtering
    Configuration conf3 = conf1.clone();
    final String f3 = "f_decimal";
    String tablePath3 = new Path(tempFile.getAbsolutePath(), "tbl3").toString();
    conf3.set(FlinkOptions.PATH, tablePath3);
    conf3.set(FlinkOptions.RECORD_KEY_FIELD, f3);
    conf3.set(FlinkOptions.PRECOMBINE_FIELD, f3);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT_HOODIE_KEY_SPECIAL_DATA_TYPE, conf3);
    HoodieTableSource tableSource3 = createHoodieTableSource(conf3);
    tableSource3.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr(f3, 1, DataTypes.DECIMAL(3, 2).notNull(),
            new BigDecimal("1.11"))));

    assertThat(tableSource3.getDataBucketFunc().get().apply(numBuckets), is(0));
    List<StoragePathInfo> fileList3 = tableSource3.getReadFiles();
    assertThat("Files should be pruned", fileList3.size(), CoreMatchers.is(1));
  }

  @Test
  void testHoodieSourceCachedMetaClient() {
    HoodieTableSource tableSource = getEmptyStreamingSource();
    HoodieTableMetaClient metaClient = tableSource.getMetaClient();
    HoodieTableSource tableSourceCopy = (HoodieTableSource) tableSource.copy();
    assertThat(metaClient, is(tableSourceCopy.getMetaClient()));
  }

  @Test
  void testFilterPushDownWithParquetPredicates() {
    HoodieTableSource tableSource = getEmptyStreamingSource();
    List<ResolvedExpression> expressions = new ArrayList<>();
    expressions.add(new FieldReferenceExpression("f_int", DataTypes.INT(), 0, 0));
    expressions.add(new ValueLiteralExpression(10));
    ResolvedExpression equalsExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS, expressions, DataTypes.BOOLEAN());
    CallExpression greaterThanExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.GREATER_THAN, expressions, DataTypes.BOOLEAN());
    CallExpression orExpression = CallExpression.permanent(
        BuiltInFunctionDefinitions.OR,
        Arrays.asList(equalsExpression, greaterThanExpression),
        DataTypes.BOOLEAN());
    List<ResolvedExpression> expectedFilters = Arrays.asList(equalsExpression, greaterThanExpression, orExpression);
    tableSource.applyFilters(expectedFilters);
    String actualPredicates = tableSource.getPredicates().toString();
    assertEquals(ExpressionPredicates.fromExpression(expectedFilters).toString(), actualPredicates);
  }

  /**
   * test single primary key filtering
   * @throws Exception
   */
  @Test
  void testPartitionBucketPruningWithSinglePK() throws Exception {
    String tablePath1 = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    int bucketNumber = 10000;
    String expression = "par1|par2|par3|par4,4";
    String rule = "regex";
    Configuration conf1 = TestConfigurations.getDefaultConf(tablePath1);
    conf1.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf1.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, bucketNumber);
    conf1.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, expression);
    conf1.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, rule);

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf1);
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expression, rule, bucketNumber, null);

    // test single primary key filtering
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf1);
    HoodieTableSource tableSource1 = createHoodieTableSource(conf1);
    tableSource1.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));

    assertThat(tableSource1.getDataBucketFunc().get().apply(4), is(1));
    List<StoragePathInfo> fileList = tableSource1.getReadFiles();
    assertThat("Files should be pruned by bucket id 1", fileList.size(), CoreMatchers.is(2));
  }

  /**
   * test multiple primary keys filtering
   * @throws Exception
   */
  @Test
  void testPartitionBucketPruningWithMultiPK() throws Exception {
    String tablePath = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    int bucketNumber = 10000;
    String expression = "par1|par2|par3|par4,4";
    String rule = "regex";
    Configuration conf = TestConfigurations.getDefaultConf(tablePath);
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, bucketNumber);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, expression);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, rule);
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf.set(FlinkOptions.KEYGEN_TYPE, "COMPLEX");

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expression, rule, bucketNumber, null);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    HoodieTableSource tableSource = createHoodieTableSource(conf);
    tableSource.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1"),
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny")));
    assertThat(tableSource.getDataBucketFunc().get().apply(4), is(3));
    List<StoragePathInfo> fileList = tableSource.getReadFiles();
    assertThat("Files should be pruned by bucket id 3", fileList.size(), CoreMatchers.is(3));
  }

  /**
   * test partial primary keys filtering
   * @throws Exception
   */
  @Test
  void testPartialPartitionBucketPruningWithMultiPK() throws Exception {
    String tablePath = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    int bucketNumber = 10000;
    String expression = "par1|par2|par3|par4,4";
    String rule = "regex";
    Configuration conf = TestConfigurations.getDefaultConf(tablePath);
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, bucketNumber);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, expression);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, rule);
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf.set(FlinkOptions.KEYGEN_TYPE, "COMPLEX");

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expression, rule, bucketNumber, null);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    HoodieTableSource tableSource = createHoodieTableSource(conf);
    tableSource.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));

    assertTrue(tableSource.getDataBucketFunc().isEmpty());
    List<StoragePathInfo> fileList = tableSource.getReadFiles();
    assertThat("Partial pk filtering does not prune any files", fileList.size(),
        CoreMatchers.is(7));
  }

  /**
   * test single primary keys filtering together with non-primary key predicate
   * @throws Exception
   */
  @Test
  void testPartitionBucketPruningWithMixedFilter() throws Exception {
    String tablePath = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    int bucketNumber = 10000;
    String expression = "par1|par2|par3|par4,4";
    String rule = "regex";
    Configuration conf = TestConfigurations.getDefaultConf(tablePath);
    conf.set(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, bucketNumber);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, expression);
    conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, rule);

    HoodieTableMetaClient metaClient = StreamerUtil.initTableIfNotExists(conf);
    PartitionBucketIndexHashingConfig.saveHashingConfig(metaClient, expression, rule, bucketNumber, null);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf);
    HoodieTableSource tableSource = createHoodieTableSource(conf);
    tableSource.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1"),
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny")));

    assertThat(tableSource.getDataBucketFunc().get().apply(4), is(1));
    List<StoragePathInfo> fileList = tableSource.getReadFiles();
    assertThat("Files should be pruned by bucket id 1", fileList.size(), CoreMatchers.is(2));
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  private static Stream<Arguments> filtersAndResults() {
    CallExpression filter1 =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.GREATER_THAN,
            Arrays.asList(
                new FieldReferenceExpression("uuid", DataTypes.STRING(), 0, 0),
                new ValueLiteralExpression("id5", DataTypes.STRING().notNull())),
            DataTypes.BOOLEAN());

    CallExpression filter2 =
        CallExpression.permanent(
            BuiltInFunctionDefinitions.LESS_THAN,
            Arrays.asList(
                new FieldReferenceExpression("partition", DataTypes.STRING(), 4, 4),
                new ValueLiteralExpression("par4", DataTypes.STRING().notNull())),
            DataTypes.BOOLEAN());

    Object[][] data = new Object[][] {
        // pruned by partition stats pruner only.
        {Arrays.asList(filter1), Arrays.asList("par3", "par4")},
        // pruned by dynamic partition pruner only.
        {Arrays.asList(filter2), Arrays.asList("par1", "par2", "par3")},
        // pruned by dynamic pruner and stats pruner.
        {Arrays.asList(filter1, filter2), Arrays.asList("par3")},
    };
    return Stream.of(data).map(Arguments::of);
  }

  private HoodieTableSource getEmptyStreamingSource() {
    final String path = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(path);
    conf.set(FlinkOptions.READ_AS_STREAMING, true);
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);

    return createHoodieTableSource(conf);
  }

  private HoodieTableSource createHoodieTableSource(Configuration conf) {
    return new HoodieTableSource(
        SerializableSchema.create(TestConfigurations.TABLE_SCHEMA),
        new StoragePath(conf.get(FlinkOptions.PATH)),
        Arrays.asList(conf.get(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);
  }

  private ResolvedExpression createLitEquivalenceExpr(String fieldName, int fieldIdx, DataType dataType, Object val) {
    FieldReferenceExpression ref = new FieldReferenceExpression(fieldName, dataType, fieldIdx, fieldIdx);
    ValueLiteralExpression literal = new ValueLiteralExpression(val, dataType);
    return CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(ref, literal),
        DataTypes.BOOLEAN());
  }
}
