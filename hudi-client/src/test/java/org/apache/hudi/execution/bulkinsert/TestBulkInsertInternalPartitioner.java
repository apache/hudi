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
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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

public class TestBulkInsertInternalPartitioner extends HoodieClientTestBase {

  public static JavaRDD<HoodieRecord> generateTestRecordsForBulkInsert(JavaSparkContext jsc) {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    // RDD partition 1
    List<HoodieRecord> records1 = dataGenerator.generateInserts("0", 100);
    // RDD partition 2
    List<HoodieRecord> records2 = dataGenerator.generateInserts("0", 150);
    return jsc.parallelize(records1, 1).union(jsc.parallelize(records2, 1));
  }

  public static Map<String, Long> generateExpectedPartitionNumRecords(JavaRDD<HoodieRecord> records) {
    return records.map(record -> record.getPartitionPath()).countByValue();
  }

  private static JavaRDD<HoodieRecord> generateTripleTestRecordsForBulkInsert(JavaSparkContext jsc)
      throws Exception {
    return generateTestRecordsForBulkInsert(jsc).union(generateTestRecordsForBulkInsert(jsc))
        .union(generateTestRecordsForBulkInsert(jsc));
  }

  private static Stream<Arguments> configParams() {
    Object[][] data = new Object[][] {
        {BulkInsertSortMode.GLOBAL_SORT, true, true},
        {BulkInsertSortMode.PARTITION_SORT, false, true},
        {BulkInsertSortMode.NONE, false, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  private void verifyRecordAscendingOrder(List<HoodieRecord> records) {
    List<HoodieRecord> expectedRecords = new ArrayList<>(records);
    Collections.sort(expectedRecords, Comparator.comparing(o -> (o.getPartitionPath() + "+" + o.getRecordKey())));
    assertEquals(expectedRecords, records);
  }

  private void testBulkInsertInternalPartitioner(BulkInsertPartitioner partitioner,
                                                 JavaRDD<HoodieRecord> records,
                                                 boolean isGloballySorted, boolean isLocallySorted,
                                                 Map<String, Long> expectedPartitionNumRecords) {
    int numPartitions = 2;
    JavaRDD<HoodieRecord> actualRecords = partitioner.repartitionRecords(records, numPartitions);
    assertEquals(numPartitions, actualRecords.getNumPartitions());
    List<HoodieRecord> collectedActualRecords = actualRecords.collect();
    if (isGloballySorted) {
      // Verify global order
      verifyRecordAscendingOrder(collectedActualRecords);
    } else if (isLocallySorted) {
      // Verify local order
      actualRecords.mapPartitions(partition -> {
        List<HoodieRecord> partitionRecords = new ArrayList<>();
        partition.forEachRemaining(partitionRecords::add);
        verifyRecordAscendingOrder(partitionRecords);
        return Collections.emptyList().iterator();
      }).collect();
    }

    // Verify number of records per partition path
    Map<String, Long> actualPartitionNumRecords = new HashMap<>();
    for (HoodieRecord record : collectedActualRecords) {
      String partitionPath = record.getPartitionPath();
      actualPartitionNumRecords.put(partitionPath,
          actualPartitionNumRecords.getOrDefault(partitionPath, 0L) + 1);
    }
    assertEquals(expectedPartitionNumRecords, actualPartitionNumRecords);
  }

  @ParameterizedTest(name = "[{index}] {0}")
  @MethodSource("configParams")
  public void testBulkInsertInternalPartitioner(BulkInsertSortMode sortMode,
                                                boolean isGloballySorted, boolean isLocallySorted)
      throws Exception {
    JavaRDD<HoodieRecord> records1 = generateTestRecordsForBulkInsert(jsc);
    JavaRDD<HoodieRecord> records2 = generateTripleTestRecordsForBulkInsert(jsc);
    testBulkInsertInternalPartitioner(BulkInsertInternalPartitionerFactory.get(sortMode),
        records1, isGloballySorted, isLocallySorted, generateExpectedPartitionNumRecords(records1));
    testBulkInsertInternalPartitioner(BulkInsertInternalPartitionerFactory.get(sortMode),
        records2, isGloballySorted, isLocallySorted, generateExpectedPartitionNumRecords(records2));
  }
}
