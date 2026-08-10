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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests LSM bulk-insert ordering without changing the configured built-in sort mode. */
public class TestLSMBulkInsertPartitioner extends HoodieSparkClientTestHarness {

  private HoodieTable lsmTable;

  private static final Comparator<Tuple2<String, String>> KEY_COMPARATOR = (left, right) -> {
    int partitionComparison = StringUtils.compareUtf8Bytes(left._1, right._1);
    return partitionComparison != 0
        ? partitionComparison
        : StringUtils.compareUtf8Bytes(left._2, right._2);
  };

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestLSMBulkInsertPartitioner");
    initPath();
    initHoodieStorage();

    lsmTable = mock(HoodieTable.class);
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(lsmTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isLSMTreeStorageLayout()).thenReturn(true);
    when(lsmTable.isPartitioned()).thenReturn(true);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @EnumSource(value = BulkInsertSortMode.class, names = {
      "GLOBAL_SORT", "PARTITION_SORT", "PARTITION_PATH_REPARTITION_AND_SORT"})
  void testHoodieRecordPartitionerSortsSupportedModes(BulkInsertSortMode sortMode) {
    JavaRDD<HoodieRecord<Object>> input = jsc.parallelize(createRecords(), 3);
    BulkInsertPartitioner<JavaRDD<HoodieRecord<Object>>> partitioner =
        BulkInsertInternalPartitionerFactory.get(
            lsmTable, createWriteConfig(sortMode, true));

    JavaRDD<HoodieRecord<Object>> actual = partitioner.repartitionRecords(input, 4);

    assertSortedSparkPartitions(actual.glom().collect(), record ->
        new Tuple2<>(record.getPartitionPath(), record.getRecordKey()));
    assertDistributionSemantics(sortMode, actual);
    assertTrue(partitioner.arePartitionRecordsSorted());
  }

  @ParameterizedTest
  @EnumSource(value = BulkInsertSortMode.class, names = {
      "GLOBAL_SORT", "PARTITION_SORT", "PARTITION_PATH_REPARTITION_AND_SORT"})
  void testRowPartitionerSortsSupportedModesWithoutChangingSchema(BulkInsertSortMode sortMode) {
    StructType schema = new StructType()
        .add(HoodieRecord.PARTITION_PATH_METADATA_FIELD, DataTypes.StringType, false)
        .add(HoodieRecord.RECORD_KEY_METADATA_FIELD, DataTypes.StringType, false)
        .add("value", DataTypes.IntegerType, false);
    Dataset<Row> input = sqlContext.createDataFrame(
        jsc.parallelize(createRows(), 3), schema);
    BulkInsertPartitioner<Dataset<Row>> partitioner =
        BulkInsertInternalPartitionerWithRowsFactory.get(
            lsmTable, createWriteConfig(sortMode, true), true);

    Dataset<Row> actual = partitioner.repartitionRecords(input, 4);

    assertEquals(schema, actual.schema(), "Sorting must not add temporary columns");
    assertSortedSparkPartitions(actual.javaRDD().glom().collect(), row -> new Tuple2<>(
        row.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        row.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD)));
    assertDistributionSemantics(sortMode, actual.javaRDD());
    assertTrue(partitioner.arePartitionRecordsSorted());
  }

  @ParameterizedTest
  @EnumSource(value = BulkInsertSortMode.class, names = {
      "GLOBAL_SORT", "PARTITION_SORT", "PARTITION_PATH_REPARTITION_AND_SORT"})
  void testPartitionersRequireMetaFields(BulkInsertSortMode sortMode) {
    HoodieWriteConfig config = createWriteConfig(sortMode, false);
    BulkInsertPartitioner<JavaRDD<HoodieRecord<Object>>> recordPartitioner =
        BulkInsertInternalPartitionerFactory.get(lsmTable, config);
    BulkInsertPartitioner<Dataset<Row>> rowPartitioner =
        BulkInsertInternalPartitionerWithRowsFactory.get(lsmTable, config, true);

    HoodieException recordException = assertThrows(HoodieException.class,
        () -> recordPartitioner.repartitionRecords(jsc.emptyRDD(), 1));
    HoodieException rowException = assertThrows(HoodieException.class,
        () -> rowPartitioner.repartitionRecords(sparkSession.emptyDataFrame(), 1));

    String expectedMessage = sortMode.name() + " mode requires meta-fields to be enabled";
    assertEquals(expectedMessage, recordException.getMessage());
    assertEquals(expectedMessage, rowException.getMessage());
  }

  @Test
  void testRowPartitionerSelectionForLsmModes() {
    assertRowPartitionerSelection(
        BulkInsertSortMode.GLOBAL_SORT, GlobalSortPartitionerWithRows.class);
    assertRowPartitionerSelection(
        BulkInsertSortMode.PARTITION_SORT, PartitionSortPartitionerWithRows.class);
    assertRowPartitionerSelection(
        BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT,
        LSMPartitionPathRepartitionAndSortPartitionerWithRows.class);
  }

  @Test
  void testHoodieRecordPartitionerSelectionForLsmModes() {
    assertHoodieRecordPartitionerSelection(
        BulkInsertSortMode.GLOBAL_SORT, LSMGlobalSortPartitioner.class);
    assertHoodieRecordPartitionerSelection(
        BulkInsertSortMode.PARTITION_SORT, LSMPartitionSortPartitioner.class);
    assertHoodieRecordPartitionerSelection(
        BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT,
        LSMPartitionPathRepartitionAndSortPartitioner.class);
  }

  @ParameterizedTest
  @EnumSource(value = BulkInsertSortMode.class, names = {"NONE", "PARTITION_PATH_REPARTITION"})
  void testNonSortingModesAreRejected(BulkInsertSortMode sortMode) {
    HoodieWriteConfig config = createWriteConfig(sortMode, true);
    String expectedMessage = "The bulk insert sort mode \"" + sortMode.name()
        + "\" does not guarantee record ordering and is not supported for LSM tables.";

    HoodieException recordException = assertThrows(HoodieException.class,
        () -> BulkInsertInternalPartitionerFactory.get(lsmTable, config));
    HoodieException rowException = assertThrows(HoodieException.class,
        () -> BulkInsertInternalPartitionerWithRowsFactory.get(lsmTable, config, true));

    assertEquals(expectedMessage, recordException.getMessage());
    assertEquals(expectedMessage, rowException.getMessage());
  }

  private BulkInsertPartitioner<Dataset<Row>> getRowPartitioner(BulkInsertSortMode sortMode) {
    return BulkInsertInternalPartitionerWithRowsFactory.get(
        lsmTable, createWriteConfig(sortMode, true), true);
  }

  private void assertRowPartitionerSelection(BulkInsertSortMode sortMode,
                                             Class<?> expectedPartitionerClass) {
    HoodieWriteConfig config = createWriteConfig(sortMode, true);
    assertEquals(expectedPartitionerClass,
        BulkInsertInternalPartitionerWithRowsFactory.get(lsmTable, config, true).getClass());
    assertEquals(expectedPartitionerClass,
        BulkInsertInternalPartitionerWithRowsFactory.get(lsmTable, config, true, true).getClass());
  }

  private BulkInsertPartitioner<JavaRDD<HoodieRecord<Object>>> getHoodieRecordPartitioner(
      BulkInsertSortMode sortMode) {
    return BulkInsertInternalPartitionerFactory.get(
        lsmTable, createWriteConfig(sortMode, true));
  }

  private void assertHoodieRecordPartitionerSelection(BulkInsertSortMode sortMode,
                                                      Class<?> expectedPartitionerClass) {
    HoodieWriteConfig config = createWriteConfig(sortMode, true);
    assertEquals(expectedPartitionerClass,
        BulkInsertInternalPartitionerFactory.get(lsmTable, config).getClass());
    assertEquals(expectedPartitionerClass,
        BulkInsertInternalPartitionerFactory.get(lsmTable, config, true).getClass());
  }

  private HoodieWriteConfig createWriteConfig(BulkInsertSortMode sortMode, boolean populateMetaFields) {
    return HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withBulkInsertSortMode(sortMode.name())
        .withPopulateMetaFields(populateMetaFields)
        .build();
  }

  private <T> void assertDistributionSemantics(BulkInsertSortMode sortMode,
                                               JavaRDD<T> actual) {
    if (sortMode == BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT) {
      assertEquals(4, actual.getNumPartitions());
      assertEachTablePartitionRoutesToOneSparkPartition(actual);
    }
  }

  private <T> void assertEachTablePartitionRoutesToOneSparkPartition(JavaRDD<T> records) {
    List<Tuple2<String, Integer>> partitionLocations = records.mapPartitionsWithIndex(
        (sparkPartition, iterator) -> {
          Set<String> tablePartitions = new HashSet<>();
          while (iterator.hasNext()) {
            Object record = iterator.next();
            tablePartitions.add(record instanceof HoodieRecord
                ? ((HoodieRecord<?>) record).getPartitionPath()
                : ((Row) record).getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
          }
          List<Tuple2<String, Integer>> locations = new ArrayList<>();
          tablePartitions.forEach(path -> locations.add(new Tuple2<>(path, sparkPartition)));
          return locations.iterator();
        }, true).collect();

    Map<String, Set<Integer>> sparkPartitionsByTablePartition = new HashMap<>();
    partitionLocations.forEach(location -> sparkPartitionsByTablePartition
        .computeIfAbsent(location._1, ignored -> new HashSet<>())
        .add(location._2));
    sparkPartitionsByTablePartition.values().forEach(
        sparkPartitions -> assertEquals(1, sparkPartitions.size()));
  }

  private <T> void assertSortedSparkPartitions(
      List<List<T>> sparkPartitions,
      java.util.function.Function<T, Tuple2<String, String>> keyExtractor) {
    for (List<T> sparkPartition : sparkPartitions) {
      Tuple2<String, String> previous = null;
      for (T record : sparkPartition) {
        Tuple2<String, String> current = keyExtractor.apply(record);
        assertTrue(previous == null || KEY_COMPARATOR.compare(previous, current) <= 0,
            "Spark partition is not UTF-8 sorted: " + previous + " > " + current);
        previous = current;
      }
    }
  }

  private List<HoodieRecord<Object>> createRecords() {
    List<HoodieRecord<Object>> records = new ArrayList<>();
    for (Tuple2<String, String> key : createKeys()) {
      records.add(new HoodieEmptyRecord<>(
          new HoodieKey(key._2, key._1), HoodieRecord.HoodieRecordType.AVRO));
    }
    return records;
  }

  private List<Row> createRows() {
    List<Row> rows = new ArrayList<>();
    int value = 0;
    for (Tuple2<String, String> key : createKeys()) {
      rows.add(RowFactory.create(key._1, key._2, value++));
    }
    return rows;
  }

  private List<Tuple2<String, String>> createKeys() {
    String bmpPrivateUse = new String(Character.toChars(0xE000));
    String supplementary = new String(Character.toChars(0x20000));
    return Arrays.asList(
        new Tuple2<>("p1", supplementary + "-a"),
        new Tuple2<>("p1", bmpPrivateUse + "-b"),
        new Tuple2<>("p2", supplementary + "-b"),
        new Tuple2<>("p2", bmpPrivateUse + "-a"),
        new Tuple2<>("p2", "ascii"),
        new Tuple2<>("p3", supplementary + "-c"),
        new Tuple2<>("p3", bmpPrivateUse + "-c"),
        new Tuple2<>("p1", "ascii"));
  }
}
