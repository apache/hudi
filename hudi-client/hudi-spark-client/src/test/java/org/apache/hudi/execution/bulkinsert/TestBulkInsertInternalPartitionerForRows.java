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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.hudi.testutils.SparkDatasetTestUtils;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests {@link BulkInsertPartitioner}s with Rows.
 */
public class TestBulkInsertInternalPartitionerForRows extends HoodieClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initSparkContexts("TestBulkInsertInternalPartitionerForRows");
    initPath();
    initFileSystem();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private static Stream<Arguments> configParams() {
    Object[][] data = new Object[][] {
        {BulkInsertSortMode.GLOBAL_SORT, true, true},
        {BulkInsertSortMode.PARTITION_SORT, false, true},
        {BulkInsertSortMode.NONE, false, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("configParams")
  public void testBulkInsertInternalPartitioner(BulkInsertSortMode sortMode,
      boolean isGloballySorted, boolean isLocallySorted)
      throws Exception {
    Dataset<Row> records1 = generateTestRecords();
    Dataset<Row> records2 = generateTestRecords();
    testBulkInsertInternalPartitioner(BulkInsertInternalPartitionerWithRowsFactory.get(sortMode),
        records1, isGloballySorted, isLocallySorted, generateExpectedPartitionNumRecords(records1));
    testBulkInsertInternalPartitioner(BulkInsertInternalPartitionerWithRowsFactory.get(sortMode),
        records2, isGloballySorted, isLocallySorted, generateExpectedPartitionNumRecords(records2));
  }

  private void testBulkInsertInternalPartitioner(BulkInsertPartitioner partitioner,
      Dataset<Row> rows,
      boolean isGloballySorted, boolean isLocallySorted,
      Map<String, Long> expectedPartitionNumRecords) {
    int numPartitions = 2;
    Dataset<Row> actualRecords = (Dataset<Row>) partitioner.repartitionRecords(rows, numPartitions);
    List<Row> collectedActualRecords = actualRecords.collectAsList();
    if (isGloballySorted) {
      // Verify global order
      verifyRowsAscendingOrder(collectedActualRecords);
    } else if (isLocallySorted) {
      // Verify local order
      actualRecords.mapPartitions((MapPartitionsFunction<Row, Object>) input -> {
        List<Row> partitionRows = new ArrayList<>();
        while (input.hasNext()) {
          partitionRows.add(input.next());
        }
        verifyRowsAscendingOrder(partitionRows);
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
    return rowsPart1.union(rowsPart2);
  }

  private void verifyRowsAscendingOrder(List<Row> records) {
    List<Row> expectedRecords = new ArrayList<>(records);
    Collections.sort(expectedRecords, Comparator.comparing(o -> (o.getAs(HoodieRecord.PARTITION_PATH_METADATA_FIELD) + "+" + o.getAs(HoodieRecord.RECORD_KEY_METADATA_FIELD))));
    assertEquals(expectedRecords, records);
  }

}
