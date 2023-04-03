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
import org.apache.hudi.source.prune.DataPruner;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.ThrowingSupplier;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
    FileStatus[] fileStatuses = tableSource.getReadFiles();
    assertNotNull(fileStatuses);
    assertThat(fileStatuses.length, is(4));
    // apply partition pruning
    FieldReferenceExpression partRef = new FieldReferenceExpression("partition", DataTypes.STRING(), 4, 4);
    ValueLiteralExpression partLiteral = new ValueLiteralExpression("par1", DataTypes.STRING().notNull());
    CallExpression partFilter = new CallExpression(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(partRef, partLiteral),
        DataTypes.BOOLEAN());
    HoodieTableSource tableSource2 = getEmptyStreamingSource();
    tableSource2.applyFilters(Arrays.asList(partFilter));

    FileStatus[] fileStatuses2 = tableSource2.getReadFiles();
    assertNotNull(fileStatuses2);
    assertThat(fileStatuses2.length, is(1));
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

  @Test
  void testHoodieSourceCachedMetaClient() {
    HoodieTableSource tableSource = getEmptyStreamingSource();
    HoodieTableMetaClient metaClient = tableSource.getMetaClient();
    HoodieTableSource tableSourceCopy = (HoodieTableSource) tableSource.copy();
    assertThat(metaClient, is(tableSourceCopy.getMetaClient()));
  }

  private HoodieTableSource getEmptyStreamingSource() {
    final String path = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(path);
    conf.setBoolean(FlinkOptions.READ_AS_STREAMING, true);
    conf.setBoolean(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);

    return new HoodieTableSource(
        TestConfigurations.TABLE_SCHEMA,
        new Path(tempFile.getPath()),
        Arrays.asList(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")),
        "default-par",
        conf);
  }
}
