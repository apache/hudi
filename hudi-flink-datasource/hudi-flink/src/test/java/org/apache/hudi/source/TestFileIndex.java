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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.keygen.NonpartitionedAvroKeyGenerator;
import org.apache.hudi.source.prune.ColumnStatsProbe;
import org.apache.hudi.source.prune.PartitionPruners;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.util.StreamerUtil;
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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.configuration.FlinkOptions.HIVE_STYLE_PARTITIONING;
import static org.apache.hudi.configuration.FlinkOptions.KEYGEN_CLASS_NAME;
import static org.apache.hudi.configuration.FlinkOptions.METADATA_ENABLED;
import static org.apache.hudi.configuration.FlinkOptions.PARTITION_DEFAULT_NAME;
import static org.apache.hudi.configuration.FlinkOptions.PARTITION_PATH_FIELD;
import static org.apache.hudi.configuration.FlinkOptions.READ_DATA_SKIPPING_ENABLED;
import static org.apache.hudi.configuration.FlinkOptions.TABLE_TYPE;
import static org.apache.hudi.utils.TestData.insertRow;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
    conf.set(METADATA_ENABLED, true);
    conf.set(HIVE_STYLE_PARTITIONING, hiveStylePartitioning);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    FileIndex fileIndex = FileIndex.builder().path(new StoragePath(tempFile.getAbsolutePath())).conf(conf)
        .rowType(TestConfigurations.ROW_TYPE).metaClient(metaClient).build();
    List<String> partitionKeys = Collections.singletonList("partition");
    List<Map<String, String>> partitions =
        fileIndex.getPartitions(partitionKeys, PARTITION_DEFAULT_NAME.defaultValue(),
            hiveStylePartitioning);
    assertTrue(partitions.stream().allMatch(m -> m.size() == 1));
    String partitionPaths = partitions.stream()
        .map(Map::values).flatMap(Collection::stream).sorted().collect(Collectors.joining(","));
    assertThat("should have 4 partitions", partitionPaths, is("par1,par2,par3,par4"));

    List<FileSlice> fileSlices = getFilteredFileSlices(metaClient, fileIndex);
    assertThat(fileSlices.size(), is(4));
    assertTrue(fileSlices.stream().allMatch(fileSlice -> fileSlice.getBaseFile().isPresent() && fileSlice.getLogFiles().count() == 0));
  }

  @Test
  void testFileListingUsingMetadataNonPartitionedTable() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(PARTITION_PATH_FIELD, "");
    conf.set(KEYGEN_CLASS_NAME, NonpartitionedAvroKeyGenerator.class.getName());
    conf.set(METADATA_ENABLED, true);
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    FileIndex fileIndex = FileIndex.builder().path(new StoragePath(tempFile.getAbsolutePath())).conf(conf)
        .rowType(TestConfigurations.ROW_TYPE).metaClient(metaClient).build();
    List<String> partitionKeys = Collections.singletonList("");
    List<Map<String, String>> partitions =
        fileIndex.getPartitions(partitionKeys, PARTITION_DEFAULT_NAME.defaultValue(), false);
    assertThat(partitions.size(), is(0));

    List<FileSlice> fileSlices = getFilteredFileSlices(metaClient, fileIndex);

    assertThat(fileSlices.size(), is(1));
    assertTrue(fileSlices.get(0).getBaseFile().isPresent() && fileSlices.get(0).getLogFiles().count() == 0);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testFileListingEmptyTable(boolean enableMetadata) {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(METADATA_ENABLED, enableMetadata);
    FileIndex fileIndex = FileIndex.builder().path(new StoragePath(tempFile.getAbsolutePath())).conf(conf)
        .rowType(TestConfigurations.ROW_TYPE).build();
    List<String> partitionKeys = Collections.singletonList("partition");
    List<Map<String, String>> partitions =
        fileIndex.getPartitions(partitionKeys, PARTITION_DEFAULT_NAME.defaultValue(), false);
    assertThat(partitions.size(), is(0));
  }

  @Test
  void testFileListingWithDataSkipping() throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath(), TestConfigurations.ROW_DATA_TYPE_BIGINT);
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");

    writeBigintDataset(conf);

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    FileIndex fileIndex =
        FileIndex.builder()
            .path(new StoragePath(tempFile.getAbsolutePath()))
            .conf(conf)
            .rowType(TestConfigurations.ROW_TYPE_BIGINT)
            .metaClient(metaClient)
            .columnStatsProbe(ColumnStatsProbe.newInstance(Collections.singletonList(CallExpression.permanent(
                FunctionIdentifier.of("greaterThan"),
                BuiltInFunctionDefinitions.GREATER_THAN,
                Arrays.asList(
                    new FieldReferenceExpression("uuid", DataTypes.BIGINT(), 0, 0),
                    new ValueLiteralExpression((byte) 5, DataTypes.TINYINT().notNull())),
                DataTypes.BOOLEAN()
            ))))
            .partitionPruner(null)
            .build();

    List<FileSlice> fileSlices = getFilteredFileSlices(metaClient, fileIndex);
    assertThat(fileSlices.size(), is(2));
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableType.class)
  void testFileListingWithPartitionStatsPruning(HoodieTableType tableType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    conf.set(READ_DATA_SKIPPING_ENABLED, true);
    conf.set(METADATA_ENABLED, true);
    conf.set(TABLE_TYPE, tableType.name());
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key(), "true");
    conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      // enable CSI for MOR table to collect col stats for delta write stats,
      // which will be used to construct partition stats then.
      conf.setString(HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key(), "true");
    }

    TestData.writeData(TestData.DATA_SET_INSERT, conf);

    // uuid > 'id5' and age < 30, only column stats of 'par3' matches the filter.
    ColumnStatsProbe columnStatsProbe =
        ColumnStatsProbe.newInstance(Arrays.asList(
            CallExpression.permanent(
                FunctionIdentifier.of("greaterThan"),
                BuiltInFunctionDefinitions.GREATER_THAN,
                Arrays.asList(
                    new FieldReferenceExpression("uuid", DataTypes.STRING(), 0, 0),
                    new ValueLiteralExpression("id5", DataTypes.STRING().notNull())
                ),
                DataTypes.BOOLEAN()),
            CallExpression.permanent(
                FunctionIdentifier.of("lessThan"),
                BuiltInFunctionDefinitions.LESS_THAN,
                Arrays.asList(
                    new FieldReferenceExpression("age", DataTypes.INT(), 2, 2),
                    new ValueLiteralExpression(30, DataTypes.INT().notNull())
                ),
                DataTypes.BOOLEAN())));

    FileIndex fileIndex =
        FileIndex.builder()
            .path(new StoragePath(tempFile.getAbsolutePath()))
            .conf(conf)
            .rowType(TestConfigurations.ROW_TYPE)
            .metaClient(StreamerUtil.createMetaClient(conf))
            .partitionPruner(PartitionPruners.builder().rowType(TestConfigurations.ROW_TYPE).basePath(tempFile.getAbsolutePath()).conf(conf).columnStatsProbe(columnStatsProbe).build())
            .build();

    List<String> p = fileIndex.getOrBuildPartitionPaths();
    assertEquals(Arrays.asList("par3"), p);
  }

  @ParameterizedTest
  @MethodSource("filtersAndResults")
  void testFileListingWithRecordLevelIndex(String recordFields, ColumnStatsProbe probe, int maxKeyCnt, int expectedCnt) throws Exception {
    DataType dataType = TestConfigurations.ROW_DATA_TYPE_WITH_ATOMIC_TYPES;
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath(), dataType);
    conf.set(FlinkOptions.TABLE_TYPE, FlinkOptions.TABLE_TYPE_COPY_ON_WRITE);
    conf.set(FlinkOptions.METADATA_ENABLED, true);
    conf.set(FlinkOptions.READ_DATA_SKIPPING_ENABLED, true);
    conf.set(FlinkOptions.RECORD_KEY_FIELD, recordFields);
    conf.set(FlinkOptions.READ_DATA_SKIPPING_RLI_KEYS_MAX_NUM, maxKeyCnt);
    // Enable record level index specifically for this test
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");

    // Write test data
    TestData.writeData(TestData.DATA_SET_WITH_ATOMIC_TYPES, conf);

    HoodieTableMetaClient metaClient = StreamerUtil.createMetaClient(conf);
    // Create a filter on the record key 'uuid' with EQUALS operator to trigger record-level index
    FileIndex fileIndex =
        FileIndex.builder()
            .path(new StoragePath(tempFile.getAbsolutePath()))
            .conf(conf)
            .rowType((RowType) dataType.getLogicalType())
            .metaClient(metaClient)
            .columnStatsProbe(probe)
            .build();

    // Get filtered file slices - this should use record-level index data skipping
    List<FileSlice> fileSlices = getFilteredFileSlices(metaClient, fileIndex);
    assertThat(fileSlices.size(), is(expectedCnt));
  }

  private static Stream<Arguments> filtersAndResults() {
    CallExpression equalTinyInt = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_tinyint", DataTypes.TINYINT(), 0, 0),
            new ValueLiteralExpression((byte) 1, DataTypes.TINYINT().notNull())
        ),
        DataTypes.BOOLEAN());
    CallExpression equalSmallInt = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_smallint", DataTypes.SMALLINT(), 0, 0),
            new ValueLiteralExpression((short) 11, DataTypes.SMALLINT().notNull())
        ),
        DataTypes.BOOLEAN());
    CallExpression equalInt = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_int", DataTypes.INT(), 0, 0),
            new ValueLiteralExpression(111, DataTypes.INT().notNull())
        ),
        DataTypes.BOOLEAN());
    CallExpression equalBigInt = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_bigint", DataTypes.BIGINT(), 0, 0),
            new ValueLiteralExpression(1111L, DataTypes.BIGINT().notNull())
        ),
        DataTypes.BOOLEAN());
    CallExpression equalFloat = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_float", DataTypes.FLOAT(), 0, 0),
            new ValueLiteralExpression(10.11f, DataTypes.FLOAT().notNull())
        ),
        DataTypes.BOOLEAN());
    CallExpression equalDouble = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_double", DataTypes.DOUBLE(), 0, 0),
            new ValueLiteralExpression(11.111, DataTypes.DOUBLE().notNull())
        ),
        DataTypes.BOOLEAN());

    CallExpression equalExpr = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_str", DataTypes.STRING(), 0, 0),
            new ValueLiteralExpression("str1", DataTypes.STRING().notNull())
        ),
        DataTypes.BOOLEAN());
    CallExpression inExpr = CallExpression.permanent(
        BuiltInFunctionDefinitions.IN,
        Arrays.asList(
            new FieldReferenceExpression("f_str", DataTypes.STRING(), 0, 0),
            new ValueLiteralExpression("str2", DataTypes.STRING().notNull()),
            new ValueLiteralExpression("str3", DataTypes.STRING().notNull())
        ),
        DataTypes.BOOLEAN());
    CallExpression orExpression = CallExpression.permanent(BuiltInFunctionDefinitions.OR, Arrays.asList(inExpr, equalExpr), DataTypes.BOOLEAN());

    CallExpression equalExpr1 = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_int", DataTypes.INT(), 0, 0),
            new ValueLiteralExpression(111, DataTypes.INT().notNull())
        ),
        DataTypes.BOOLEAN());
    CallExpression inExpr1 = CallExpression.permanent(
        BuiltInFunctionDefinitions.IN,
        Arrays.asList(
            new FieldReferenceExpression("f_int", DataTypes.INT(), 0, 0),
            new ValueLiteralExpression(333, DataTypes.INT().notNull()),
            new ValueLiteralExpression(222, DataTypes.INT().notNull())
        ),
        DataTypes.BOOLEAN());

    // record predicate with IN, number of filtered file slices is 1.
    ColumnStatsProbe probe1 = ColumnStatsProbe.newInstance(Collections.singletonList(equalExpr));
    // record predicate with EQUALS, number of filtered file slices is 2.
    ColumnStatsProbe probe2 = ColumnStatsProbe.newInstance(Collections.singletonList(inExpr));
    // record predicate with OR, number of filtered file slices is 3.
    ColumnStatsProbe probe3 = ColumnStatsProbe.newInstance(Collections.singletonList(orExpression));

    // predicate for two record keys
    // id = id3 and name in ('Bob', 'Han'), number of filtered file slices is 0.
    ColumnStatsProbe probe4 = ColumnStatsProbe.newInstance(Arrays.asList(equalExpr, inExpr1));
    // id = id3 and name = 'Julian', number of filtered file slices is 1.
    ColumnStatsProbe probe5 = ColumnStatsProbe.newInstance(Arrays.asList(equalExpr, equalExpr1));
    // id in (id1, id7) and name in ('Bob', 'Danny'), number of filtered file slices is 2.
    ColumnStatsProbe probe6 = ColumnStatsProbe.newInstance(Arrays.asList(inExpr, inExpr1));

    ColumnStatsProbe probeTinyInt = ColumnStatsProbe.newInstance(Collections.singletonList(equalTinyInt));
    ColumnStatsProbe probeSmallInt = ColumnStatsProbe.newInstance(Collections.singletonList(equalSmallInt));
    ColumnStatsProbe probeInt = ColumnStatsProbe.newInstance(Collections.singletonList(equalInt));
    ColumnStatsProbe probeBigInt = ColumnStatsProbe.newInstance(Collections.singletonList(equalBigInt));
    ColumnStatsProbe probeFloat = ColumnStatsProbe.newInstance(Collections.singletonList(equalFloat));
    ColumnStatsProbe probeDouble = ColumnStatsProbe.newInstance(Collections.singletonList(equalDouble));

    // TIMESTAMP data type tests - using the special data type config with f_timestamp as record key
    CallExpression equalExprTimestamp = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_timestamp", DataTypes.TIMESTAMP(3), 0, 0),
            new ValueLiteralExpression(LocalDateTime.ofInstant(Instant.ofEpochMilli(1), ZoneId.of("UTC")), DataTypes.TIMESTAMP(3).notNull())
        ),
        DataTypes.BOOLEAN());
    ColumnStatsProbe probeTimestamp = ColumnStatsProbe.newInstance(Collections.singletonList(equalExprTimestamp));

    // TIME data type tests - using the TIME config with appropriate record key
    CallExpression equalExprTime = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_time", DataTypes.TIME(), 0, 0),
            new ValueLiteralExpression(LocalTime.ofSecondOfDay(1), DataTypes.TIME().notNull())
        ),
        DataTypes.BOOLEAN());
    ColumnStatsProbe probeTime = ColumnStatsProbe.newInstance(Collections.singletonList(equalExprTime));

    // DATE data type tests - using the date config with appropriate record key
    CallExpression equalExprDate = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_date", DataTypes.DATE(), 0, 0),
            new ValueLiteralExpression(LocalDate.ofEpochDay(1), DataTypes.DATE().notNull())
        ),
        DataTypes.BOOLEAN());
    ColumnStatsProbe probeDate = ColumnStatsProbe.newInstance(Collections.singletonList(equalExprDate));

    // DECIMAL data type tests - using decimal ordering config
    CallExpression equalExprDecimal = CallExpression.permanent(
        BuiltInFunctionDefinitions.EQUALS,
        Arrays.asList(
            new FieldReferenceExpression("f_decimal", DataTypes.DECIMAL(38, 18), 2, 2),
            new ValueLiteralExpression(new BigDecimal("1.11"), DataTypes.DECIMAL(38, 18).notNull())
        ),
        DataTypes.BOOLEAN());
    ColumnStatsProbe probeDecimal = ColumnStatsProbe.newInstance(Collections.singletonList(equalExprDecimal));

    Object[][] data = new Object[][] {
        {"f_str", probe1, 8, 1},
        {"f_str", probe2, 8, 2},
        {"f_str", probe3, 8, 3},
        {"f_str,f_int", probe4, 8, 0},
        {"f_str,f_int", probe5, 8, 1},
        {"f_str,f_int", probe6, 8, 2},
        // the number of hoodie keys inferred from query predicate is 2, which exceed the configured max
        // number of hoodie keys for record index, thus fallback to not using record index.
        {"f_str,f_int", probe2, 1, 3},
        // key type is TINYINT
        {"f_tinyint", probeTinyInt, 8, 1},
        // key type is SMALLINT
        {"f_smallint", probeSmallInt, 8, 1},
        // key type is INT
        {"f_int", probeInt, 8, 1},
        // key type is BIGINT
        {"f_bigint", probeBigInt, 8, 1},
        // key type is FLOAT
        {"f_float", probeFloat, 8, 1},
        // key type is DOUBLE
        {"f_double", probeDouble, 8, 1},
        // key type is TIMESTAMP
        {"f_timestamp", probeTimestamp, 8, 1},
        // key type is TIME
        {"f_time", probeTime, 8, 1},
        // key type is DATE
        {"f_date", probeDate, 8, 1},
        // key type is DECIMAL
        {"f_decimal", probeDecimal, 8, 1},
    };
    return Stream.of(data).map(Arguments::of);
  }

  private List<FileSlice> getFilteredFileSlices(HoodieTableMetaClient metaClient, FileIndex fileIndex) {
    List<String> relPartitionPaths = fileIndex.getOrBuildPartitionPaths();
    List<StoragePathInfo> pathInfoList = fileIndex.getFilesInPartitions();
    HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline();
    List<FileSlice> allFileSlices;
    try (HoodieTableFileSystemView fsView = new HoodieTableFileSystemView(metaClient, timeline, pathInfoList)) {
      if (timeline.lastInstant().isPresent()) {
        allFileSlices = relPartitionPaths.stream()
            .flatMap(par -> fsView.getLatestMergedFileSlicesBeforeOrOn(par, timeline.lastInstant().get().requestedTime())).collect(Collectors.toList());
      } else {
        return Collections.emptyList();
      }
    }
    return fileIndex.filterFileSlices(allFileSlices);
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
