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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests {@link BulkInsertPartitioner}s with Rows.
 */
public class TestBulkInsertInternalPartitionerForRows extends HoodieSparkClientTestHarness {

  private static final Comparator<Row> DEFAULT_KEY_COMPARATOR =
      Comparator.comparing(o -> (o.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD) + "+" + o.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD)));

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestBulkInsertInternalPartitionerForRows");
    initPath();
    initHoodieStorage();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private static Stream<Arguments> configParams() {
    // Parameters:
    //   BulkInsertSortMode sortMode,
    //   boolean isTablePartitioned,
    //   boolean enforceNumOutputPartitions,
    //   boolean isGloballySorted,
    //   boolean isLocallySorted,
    //   boolean populateMetaFields
    Object[][] data = new Object[][] {
        {BulkInsertSortMode.GLOBAL_SORT, true, true, true, true, true},
        {BulkInsertSortMode.PARTITION_SORT, true, true, false, true, true},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION, true, true, false, false, true},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION, false, true, false, false, true},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT, true, true, false, false, true},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT, false, true, false, false, true},
        {BulkInsertSortMode.NONE, true, true, false, false, true},
        {BulkInsertSortMode.NONE, true, false, false, false, true},
        {BulkInsertSortMode.GLOBAL_SORT, true, true, true, true, false},
        {BulkInsertSortMode.PARTITION_SORT, true, true, false, true, false},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION, true, true, false, false, false},
        {BulkInsertSortMode.PARTITION_PATH_REPARTITION_AND_SORT, true, true, false, false, false, false},
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest(name = "[{index}] {0} isTablePartitioned={1} enforceNumOutputPartitions={2}")
  @MethodSource("configParams")
  public void testBulkInsertInternalPartitioner(BulkInsertSortMode sortMode,
                                                boolean isTablePartitioned,
                                                boolean enforceNumOutputPartitions,
                                                boolean isGloballySorted,
                                                boolean isLocallySorted,
                                                boolean populateMetaFields) {
    Dataset<Row> records = generateTestRecords();

    HoodieWriteConfig config = HoodieWriteConfig
        .newBuilder()
        .withPath("/")
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withBulkInsertSortMode(sortMode.name())
        .withPopulateMetaFields(populateMetaFields)
        .build();

    testBulkInsertInternalPartitioner(
        BulkInsertInternalPartitionerWithRowsFactory.get(config, isTablePartitioned, enforceNumOutputPartitions),
        records,
        enforceNumOutputPartitions,
        isGloballySorted,
        isLocallySorted,
        generateExpectedPartitionNumRecords(records),
        Option.empty(),
        populateMetaFields);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testCustomColumnSortPartitionerWithRows(boolean suffixRecordKey) {
    Dataset<Row> records = generateTestRecords();
    String sortColumnString = records.columns()[6];
    String[] sortColumns = sortColumnString.split(",");
    Comparator<Row> comparator = getCustomColumnComparator(sortColumns);

    TypedProperties properties = new TypedProperties();
    properties.setProperty(HoodieWriteConfig.BULKINSERT_SUFFIX_RECORD_KEY_SORT_COLUMNS.key(), String.valueOf(suffixRecordKey));
    HoodieWriteConfig config = HoodieWriteConfig
        .newBuilder()
        .withPath("/")
        .withUserDefinedBulkInsertPartitionerClass(RowCustomColumnsSortPartitioner.class.getName())
        .withUserDefinedBulkInsertPartitionerSortColumns(sortColumnString)
        .withProperties(properties)
        .build();

    testBulkInsertInternalPartitioner(new RowCustomColumnsSortPartitioner(sortColumns, config),
        records, true, true, true, generateExpectedPartitionNumRecords(records), Option.of(comparator), true);

    testBulkInsertInternalPartitioner(new RowCustomColumnsSortPartitioner(config),
        records, true, true, true, generateExpectedPartitionNumRecords(records), Option.of(comparator), true);
  }

  private void testBulkInsertInternalPartitioner(BulkInsertPartitioner partitioner,
                                                 Dataset<Row> rows,
                                                 boolean enforceNumOutputPartitions,
                                                 boolean isGloballySorted,
                                                 boolean isLocallySorted,
                                                 Map<String, Long> expectedPartitionNumRecords,
                                                 Option<Comparator<Row>> comparator,
                                                 boolean populateMetaFields) {
    int numPartitions = 2;
    if (!populateMetaFields) {
      assertThrows(HoodieException.class, () -> partitioner.repartitionRecords(rows, numPartitions));
      return;
    }
    Dataset<Row> actualRecords = (Dataset<Row>) partitioner.repartitionRecords(rows, numPartitions);
    if (isGloballySorted) {
      // For GLOBAL_SORT, `df.sort` may generate smaller number of partitions than the specified parallelism
      assertTrue(actualRecords.rdd().getNumPartitions() <= numPartitions);
    } else {
      assertEquals(
          enforceNumOutputPartitions ? numPartitions : rows.rdd().getNumPartitions(),
          actualRecords.rdd().getNumPartitions());
    }

    List<Row> collectedActualRecords = actualRecords.collectAsList();
    if (isGloballySorted) {
      // Verify global order
      verifyRowsAscendingOrder(collectedActualRecords, comparator);
    } else if (isLocallySorted) {
      // Verify local order
      actualRecords.mapPartitions((MapPartitionsFunction<Row, Object>) input -> {
        List<Row> partitionRows = new ArrayList<>();
        while (input.hasNext()) {
          partitionRows.add(input.next());
        }
        verifyRowsAscendingOrder(partitionRows, comparator);
        return Collections.emptyList().iterator();
      }, SparkDatasetTestUtils.ENCODER);
    }

    // Verify number of records per partition path
    Map<String, Long> actualPartitionNumRecords = new HashMap<>();
    for (Row record : collectedActualRecords) {
      String partitionPath = record.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
      actualPartitionNumRecords.put(partitionPath,
          actualPartitionNumRecords.getOrDefault(partitionPath, 0L) + 1);
    }
    assertEquals(expectedPartitionNumRecords, actualPartitionNumRecords);
  }

  public static Map<String, Long> generateExpectedPartitionNumRecords(Dataset<Row> rows) {
    Dataset<Row> toReturn = rows.groupBy(HoodieRecord.PARTITION_PATH_METADATA_FIELD).count();
    List<Row> result = toReturn.collectAsList();
    Map<String, Long> returnMap = new HashMap<>();
    for (Row row : result) {
      returnMap.put(row.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD), (Long) row.getAs("count"));
    }
    return returnMap;
  }

  public Dataset<Row> generateTestRecords() {
    Dataset<Row> rowsPart1 = SparkDatasetTestUtils.getRandomRows(sqlContext, 100, HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH, false);
    Dataset<Row> rowsPart2 = SparkDatasetTestUtils.getRandomRows(sqlContext, 150, HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH, false);
    Dataset<Row> rowsPart3 = SparkDatasetTestUtils.getRandomRows(sqlContext, 200, HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH, false);
    return rowsPart1.union(rowsPart2).union(rowsPart3);
  }

  private void verifyRowsAscendingOrder(List<Row> records, Option<Comparator<Row>> comparator) {
    List<Row> expectedRecords = new ArrayList<>(records);
    Collections.sort(expectedRecords, comparator.orElse(DEFAULT_KEY_COMPARATOR));
    assertEquals(expectedRecords, records);
  }

  private Comparator<Row> getCustomColumnComparator(String[] sortColumns) {
    Comparator<Row> comparator = Comparator.comparing(row -> {
      StringBuilder sb = new StringBuilder(row.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
      for (String col : sortColumns) {
        sb.append(row.getAs(col).toString());
      }
      return sb.toString();
    });
    return comparator;
  }
}
