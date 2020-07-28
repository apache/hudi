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
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.HoodieTestDataGenerator.newHoodieRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBulkInsertInternalPartitioner extends HoodieClientTestBase {

  public static JavaRDD<HoodieRecord> generateTestRecordsForBulkInsert(JavaSparkContext jsc)
      throws Exception {
    // RDD partition 1
    List<HoodieRecord> records1 = newHoodieRecords(3, "2020-07-31T03:16:41.415Z");
    records1.addAll(newHoodieRecords(2, "2020-08-01T03:16:41.415Z"));
    records1.addAll(newHoodieRecords(5, "2020-07-31T03:16:41.415Z"));
    // RDD partition 2
    List<HoodieRecord> records2 = newHoodieRecords(4, "2020-08-02T03:16:22.415Z");
    records2.addAll(newHoodieRecords(1, "2020-07-31T03:16:41.415Z"));
    records2.addAll(newHoodieRecords(5, "2020-08-01T06:16:41.415Z"));
    return jsc.parallelize(records1, 1).union(jsc.parallelize(records2, 1));
  }

  public static Map<String, Long> generateExpectedPartitionNumRecords() {
    Map<String, Long> expectedPartitionNumRecords = new HashMap<>();
    expectedPartitionNumRecords.put("2020/07/31", 9L);
    expectedPartitionNumRecords.put("2020/08/01", 7L);
    expectedPartitionNumRecords.put("2020/08/02", 4L);
    return expectedPartitionNumRecords;
  }

  private static JavaRDD<HoodieRecord> generateTripleTestRecordsForBulkInsert(JavaSparkContext jsc)
      throws Exception {
    return generateTestRecordsForBulkInsert(jsc).union(generateTestRecordsForBulkInsert(jsc))
        .union(generateTestRecordsForBulkInsert(jsc));
  }

  public static Map<String, Long> generateExpectedPartitionNumRecordsTriple() {
    Map<String, Long> expectedPartitionNumRecords = generateExpectedPartitionNumRecords();
    for (String partitionPath : expectedPartitionNumRecords.keySet()) {
      expectedPartitionNumRecords.put(partitionPath,
          expectedPartitionNumRecords.get(partitionPath) * 3);
    }
    return expectedPartitionNumRecords;
  }

  private static Stream<Arguments> configParams() {
    Object[][] data = new Object[][] {
        {BulkInsertInternalPartitioner.BulkInsertSortMode.GLOBAL_SORT, true, true},
        {BulkInsertInternalPartitioner.BulkInsertSortMode.PARTITION_SORT, false, true},
        {BulkInsertInternalPartitioner.BulkInsertSortMode.NONE, false, false}
    };
    return Stream.of(data).map(Arguments::of);
  }

  private void verifyRecordAscendingOrder(Iterator<HoodieRecord> records) {
    HoodieRecord prevRecord = null;

    for (Iterator<HoodieRecord> it = records; it.hasNext(); ) {
      HoodieRecord record = it.next();
      if (prevRecord != null) {
        assertTrue(record.getPartitionPath().compareTo(prevRecord.getPartitionPath()) > 0
            || (record.getPartitionPath().equals(prevRecord.getPartitionPath())
            && record.getRecordKey().compareTo(prevRecord.getRecordKey()) >= 0));
      }
      prevRecord = record;
    }
  }

  private void testBulkInsertInternalPartitioner(BulkInsertInternalPartitioner partitioner,
                                                 JavaRDD<HoodieRecord> records,
                                                 boolean isGloballySorted, boolean isLocallySorted,
                                                 Map<String, Long> expectedPartitionNumRecords) {
    int numPartitions = 2;
    JavaRDD<HoodieRecord> actualRecords = partitioner.repartitionRecords(records, numPartitions);
    assertEquals(numPartitions, actualRecords.getNumPartitions());
    List<HoodieRecord> collectedActualRecords = actualRecords.collect();
    if (isGloballySorted) {
      // Verify global order
      verifyRecordAscendingOrder(collectedActualRecords.iterator());
    } else if (isLocallySorted) {
      // Verify local order
      actualRecords.mapPartitions(partition -> {
        verifyRecordAscendingOrder(partition);
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
  public void testBulkInsertInternalPartitioner(BulkInsertInternalPartitioner.BulkInsertSortMode sortMode,
                                                boolean isGloballySorted, boolean isLocallySorted)
      throws Exception {
    testBulkInsertInternalPartitioner(BulkInsertInternalPartitioner.get(sortMode),
        generateTestRecordsForBulkInsert(jsc), isGloballySorted, isLocallySorted,
        generateExpectedPartitionNumRecords());
    testBulkInsertInternalPartitioner(BulkInsertInternalPartitioner.get(sortMode),
        generateTripleTestRecordsForBulkInsert(jsc), isGloballySorted, isLocallySorted,
        generateExpectedPartitionNumRecordsTriple());
  }
}
