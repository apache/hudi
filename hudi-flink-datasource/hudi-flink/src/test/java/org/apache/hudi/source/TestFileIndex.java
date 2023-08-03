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

package org.apache.hudi.source;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.source.prune.DataPruner;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.configuration.FlinkOptions.HIVE_STYLE_PARTITIONING;
import static org.apache.hudi.configuration.FlinkOptions.KEYGEN_CLASS_NAME;
import static org.apache.hudi.configuration.FlinkOptions.METADATA_ENABLED;
import static org.apache.hudi.configuration.FlinkOptions.PARTITION_DEFAULT_NAME;
import static org.apache.hudi.configuration.FlinkOptions.PARTITION_PATH_FIELD;
import static org.apache.hudi.utils.TestData.insertRow;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link FileIndex}.
 */
public class TestFileIndex {
  @TempDir
  File tempFile;

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFileListingUsingMetadata(boolean hiveStylePartitioning) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(METADATA_ENABLED, true);
    conf.setBoolean(HIVE_STYLE_PARTITIONING, hiveStylePartitioning);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    FileIndex fileIndex = FileIndex.builder().path(new Path(tempFile.getAbsolutePath())).conf(conf).rowType(TestConfigurations.ROW_TYPE).build();
    List<String> partitionKeys = Collections.singletonList("partition");
    List<Map<String, String>> partitions = fileIndex.getPartitions(partitionKeys, PARTITION_DEFAULT_NAME.defaultValue(), hiveStylePartitioning);
    assertTrue(partitions.stream().allMatch(m -> m.size() == 1));
    String partitionPaths = partitions.stream()
        .map(Map::values).flatMap(Collection::stream).sorted().collect(Collectors.joining(","));
    assertThat("should have 4 partitions", partitionPaths, is("par1,par2,par3,par4"));

    FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
    assertThat(fileStatuses.length, is(4));
    assertTrue(Arrays.stream(fileStatuses)
        .allMatch(fileStatus -> fileStatus.getPath().toString().endsWith(HoodieFileFormat.PARQUET.getFileExtension())));
  }

  @Test
  void testFileListingUsingMetadataNonPartitionedTable() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setString(PARTITION_PATH_FIELD, "");
    conf.setString(KEYGEN_CLASS_NAME, NonpartitionedAvroKeyGenerator.class.getName());
    conf.setBoolean(METADATA_ENABLED, true);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    FileIndex fileIndex = FileIndex.builder().path(new Path(tempFile.getAbsolutePath())).conf(conf).rowType(TestConfigurations.ROW_TYPE).build();
    List<String> partitionKeys = Collections.singletonList("");
    List<Map<String, String>> partitions = fileIndex.getPartitions(partitionKeys, PARTITION_DEFAULT_NAME.defaultValue(), false);
    assertThat(partitions.size(), is(0));

    FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
    assertThat(fileStatuses.length, is(1));
    assertTrue(fileStatuses[0].getPath().toString().endsWith(HoodieFileFormat.PARQUET.getFileExtension()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFileListingEmptyTable(boolean enableMetadata) {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.setBoolean(METADATA_ENABLED, enableMetadata);
    FileIndex fileIndex = FileIndex.builder().path(new Path(tempFile.getAbsolutePath())).conf(conf).rowType(TestConfigurations.ROW_TYPE).build();
    List<String> partitionKeys = Collections.singletonList("partition");
    List<Map<String, String>> partitions = fileIndex.getPartitions(partitionKeys, PARTITION_DEFAULT_NAME.defaultValue(), false);
    assertThat(partitions.size(), is(0));

    FileStatus[] fileStatuses = fileIndex.getFilesInPartitions();
    assertThat(fileStatuses.length, is(0));
  }

  @Test
  void testFileListingWithDataSkipping() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath(), TestConfigurations.ROW_DATA_TYPE_BIGINT);
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
    conf.setBoolean(FlinkOptions.METADATA_ENABLED, true);
    conf.setBoolean(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.setBoolean(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), true);

    writeBigintDataset(conf);

    FileIndex fileIndex =
        FileIndex.builder()
            .path(new Path(tempFile.getAbsolutePath()))
            .conf(conf).rowType(TestConfigurations.ROW_TYPE_BIGINT)
            .dataPruner(DataPruner.newInstance(Collections.singletonList(new CallExpression(
                FunctionIdentifier.of("greaterThan"),
                BuiltInFunctionDefinitions.GREATER_THAN,
                Arrays.asList(
                    new FieldReferenceExpression("uuid", DataTypes.BIGINT(), 0, 0), 
                    new ValueLiteralExpression((byte) 5, DataTypes.TINYINT().notNull())),
                DataTypes.BOOLEAN()
            ))))
            .partitionPruner(null)
            .build();

    FileStatus[] files = fileIndex.getFilesInPartitions();
    assertThat(files.length, is(2));
  }

  private void writeBigintDataset(Configuration conf) throws Exception {
    List<RowData> dataset = Arrays.asList(
        insertRow(TestConfigurations.ROW_TYPE_BIGINT, 1L, StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
        insertRow(TestConfigurations.ROW_TYPE_BIGINT, 2L, StringData.fromString("Stephen"), 33,
            TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
        insertRow(TestConfigurations.ROW_TYPE_BIGINT, 3L, StringData.fromString("Julian"), 53,
            TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
        insertRow(TestConfigurations.ROW_TYPE_BIGINT, 4L, StringData.fromString("Fabian"), 31,
            TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
        insertRow(TestConfigurations.ROW_TYPE_BIGINT, 5L, StringData.fromString("Sophia"), 18,
            TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
        insertRow(TestConfigurations.ROW_TYPE_BIGINT, 6L, StringData.fromString("Emma"), 20,
            TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
        insertRow(TestConfigurations.ROW_TYPE_BIGINT, 7L, StringData.fromString("Bob"), 44,
            TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
        insertRow(TestConfigurations.ROW_TYPE_BIGINT, 8L, StringData.fromString("Han"), 56,
            TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
    );

    TestData.writeData(dataset, conf);
  }
}
