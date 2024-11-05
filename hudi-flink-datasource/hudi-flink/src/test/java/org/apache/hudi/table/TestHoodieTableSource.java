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

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.source.prune.DataPruner;
import org.apache.hudi.source.prune.PrimaryKeyPruners;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.format.mor.MergeOnReadInputFormat;
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

import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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
    CallExpression partFilter = new CallExpression(
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
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getPath()),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);
    InputFormat<RowData, ?> inputFormat = tableSource.getInputFormat();
    assertThat(inputFormat, is(instanceOf(FileInputFormat.class)));
    conf.setString(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_MERGE_ON_READ);
    inputFormat = tableSource.getInputFormat();
    assertThat(inputFormat, is(instanceOf(MergeOnReadInputFormat.class)));
    conf.setString(FlinkOptions.QUERY_TYPE.key(), FlinkOptions.QUERY_TYPE_INCREMENTAL);
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
    ResolvedExpression filterExpr = new CallExpression(
        BuiltInFunctionDefinitions.IN,
        Arrays.asList(ref, literal),
        DataTypes.BOOLEAN());
    List<ResolvedExpression> expectedFilters = Collections.singletonList(filterExpr);
    tableSource.applyFilters(expectedFilters);
    HoodieTableSource copiedSource = (HoodieTableSource) tableSource.copy();
    DataPruner dataPruner = copiedSource.getDataPruner();
    assertNotNull(dataPruner);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBucketPruning(boolean hiveStylePartitioning) throws Exception {
    String tablePath1 = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    Configuration conf1 = TestConfigurations.getDefaultConf(tablePath1);
    conf1.setString(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf1.setBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING, hiveStylePartitioning);

    // test single primary key filtering
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf1);
    HoodieTableSource tableSource1 = createHoodieTableSource(conf1);
    tableSource1.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));

    assertThat(tableSource1.getDataBucket(), is(1));
    List<StoragePathInfo> fileList = tableSource1.getReadFiles();
    assertThat("Files should be pruned by bucket id 1", fileList.size(), CoreMatchers.is(2));

    // test multiple primary keys filtering
    Configuration conf2 = conf1.clone();
    String tablePath2 = new Path(tempFile.getAbsolutePath(), "tbl2").toString();
    conf2.setString(FlinkOptions.PATH, tablePath2);
    conf2.setString(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf2.setString(FlinkOptions.KEYGEN_TYPE, "COMPLEX");
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf2);
    HoodieTableSource tableSource2 = createHoodieTableSource(conf2);
    tableSource2.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1"),
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny")));
    assertThat(tableSource2.getDataBucket(), is(3));
    List<StoragePathInfo> fileList2 = tableSource2.getReadFiles();
    assertThat("Files should be pruned by bucket id 3", fileList2.size(), CoreMatchers.is(3));

    // apply the filters in different order and test again.
    tableSource2.reset();
    tableSource2.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny"),
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));
    assertThat(tableSource2.getDataBucket(), is(3));
    assertThat("Files should be pruned by bucket id 3", tableSource2.getReadFiles().size(),
        CoreMatchers.is(3));

    // test partial primary keys filtering
    Configuration conf3 = conf1.clone();
    String tablePath3 = new Path(tempFile.getAbsolutePath(), "tbl3").toString();
    conf3.setString(FlinkOptions.PATH, tablePath3);
    conf3.setString(FlinkOptions.RECORD_KEY_FIELD, "uuid,name");
    conf3.setString(FlinkOptions.KEYGEN_TYPE, "COMPLEX");
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf3);
    HoodieTableSource tableSource3 = createHoodieTableSource(conf3);
    tableSource3.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1")));

    assertThat(tableSource3.getDataBucket(), is(PrimaryKeyPruners.BUCKET_ID_NO_PRUNING));
    List<StoragePathInfo> fileList3 = tableSource3.getReadFiles();
    assertThat("Partial pk filtering does not prune any files", fileList3.size(),
        CoreMatchers.is(7));

    // test single primary keys filtering together with non-primary key predicate
    Configuration conf4 = conf1.clone();
    String tablePath4 = new Path(tempFile.getAbsolutePath(), "tbl4").toString();
    conf4.setString(FlinkOptions.PATH, tablePath4);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT, conf4);
    HoodieTableSource tableSource4 = createHoodieTableSource(conf4);
    tableSource4.applyFilters(Arrays.asList(
        createLitEquivalenceExpr("uuid", 0, DataTypes.STRING().notNull(), "id1"),
        createLitEquivalenceExpr("name", 1, DataTypes.STRING().notNull(), "Danny")));

    assertThat(tableSource4.getDataBucket(), is(1));
    List<StoragePathInfo> fileList4 = tableSource4.getReadFiles();
    assertThat("Files should be pruned by bucket id 1", fileList4.size(), CoreMatchers.is(2));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testBucketPruningSpecialKeyDataType(boolean logicalTimestamp) throws Exception {
    String tablePath1 = new Path(tempFile.getAbsolutePath(), "tbl1").toString();
    Configuration conf1 = TestConfigurations.getDefaultConf(tablePath1, TestConfigurations.ROW_DATA_TYPE_HOODIE_KEY_SPECIAL_DATA_TYPE);
    final String f1 = "f_timestamp";
    conf1.setString(FlinkOptions.INDEX_TYPE, "BUCKET");
    conf1.setString(FlinkOptions.RECORD_KEY_FIELD, f1);
    conf1.setString(FlinkOptions.PRECOMBINE_FIELD, f1);
    conf1.removeConfig(FlinkOptions.PARTITION_PATH_FIELD);
    conf1.setBoolean(KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(), logicalTimestamp);

    // test timestamp filtering
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT_HOODIE_KEY_SPECIAL_DATA_TYPE, conf1);
    HoodieTableSource tableSource1 = createHoodieTableSource(conf1);
    tableSource1.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr(f1, 0, DataTypes.TIMESTAMP(3).notNull(),
            LocalDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")))));

    assertThat(tableSource1.getDataBucket(), is(logicalTimestamp ? 1 : 0));
    List<StoragePathInfo> fileList = tableSource1.getReadFiles();
    assertThat("Files should be pruned", fileList.size(), CoreMatchers.is(1));

    // test date filtering
    Configuration conf2 = conf1.clone();
    final String f2 = "f_date";
    String tablePath2 = new Path(tempFile.getAbsolutePath(), "tbl2").toString();
    conf2.setString(FlinkOptions.PATH, tablePath2);
    conf2.setString(FlinkOptions.RECORD_KEY_FIELD, f2);
    conf2.setString(FlinkOptions.PRECOMBINE_FIELD, f2);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT_HOODIE_KEY_SPECIAL_DATA_TYPE, conf2);
    HoodieTableSource tableSource2 = createHoodieTableSource(conf2);
    tableSource2.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr(f2, 1, DataTypes.DATE().notNull(), LocalDate.ofEpochDay(1))));

    assertThat(tableSource2.getDataBucket(), is(1));
    List<StoragePathInfo> fileList2 = tableSource2.getReadFiles();
    assertThat("Files should be pruned", fileList2.size(), CoreMatchers.is(1));

    // test decimal filtering
    Configuration conf3 = conf1.clone();
    final String f3 = "f_decimal";
    String tablePath3 = new Path(tempFile.getAbsolutePath(), "tbl3").toString();
    conf3.setString(FlinkOptions.PATH, tablePath3);
    conf3.setString(FlinkOptions.RECORD_KEY_FIELD, f3);
    conf3.setString(FlinkOptions.PRECOMBINE_FIELD, f3);
    TestData.writeDataAsBatch(TestData.DATA_SET_INSERT_HOODIE_KEY_SPECIAL_DATA_TYPE, conf3);
    HoodieTableSource tableSource3 = createHoodieTableSource(conf3);
    tableSource3.applyFilters(Collections.singletonList(
        createLitEquivalenceExpr(f3, 1, DataTypes.DECIMAL(3, 2).notNull(),
            new BigDecimal("1.11"))));

    assertThat(tableSource3.getDataBucket(), is(0));
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
    ResolvedExpression equalsExpression = new CallExpression(
        BuiltInFunctionDefinitions.EQUALS, expressions, DataTypes.BOOLEAN());
    CallExpression greaterThanExpression = new CallExpression(
        BuiltInFunctionDefinitions.GREATER_THAN, expressions, DataTypes.BOOLEAN());
    CallExpression orExpression = new CallExpression(
        BuiltInFunctionDefinitions.OR,
        Arrays.asList(equalsExpression, greaterThanExpression),
        DataTypes.BOOLEAN());
    List<ResolvedExpression> expectedFilters = Arrays.asList(equalsExpression, greaterThanExpression, orExpression);
    tableSource.applyFilters(expectedFilters);
    String actualPredicates = tableSource.getPredicates().toString();
    assertEquals(ExpressionPredicates.fromExpression(expectedFilters).toString(), actualPredicates);
  }

  private HoodieTableSource getEmptyStreamingSource() {
    final String path = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(path);
    conf.setBoolean(FlinkOptions.READ_AS_STREAMING, true);
    conf.setBoolean(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);

    return createHoodieTableSource(conf);
  }

  private HoodieTableSource createHoodieTableSource(Configuration conf) {
    return new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(conf.getString(FlinkOptions.PATH)),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);
  }

  private ResolvedExpression createLitEquivalenceExpr(String fieldName, int fieldIdx, DataType dataType, Object val) {
    FieldReferenceExpression ref = new FieldReferenceExpression(fieldName, dataType, fieldIdx, fieldIdx);
    ValueLiteralExpression literal = new ValueLiteralExpression(val, dataType);
    return new CallExpression(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(ref, literal),
        DataTypes.BOOLEAN());
  }
}
