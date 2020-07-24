package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.testutils.HoodieTestDataGenerator.newHoodieRecords;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBulkInsertInternalPartitioner extends HoodieClientTestBase {

  public static JavaRDD<HoodieRecord> generateTestRecordsForBulkInsert(JavaSparkContext jsc)
      throws Exception {
    List<HoodieRecord> records1 = newHoodieRecords(2, "2020-08-01T03:16:41.415Z");
    records1.addAll(newHoodieRecords(8, "2020-07-31T03:16:41.415Z"));
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
      JavaRDD<HoodieRecord> records, boolean isGloballySorted, boolean isLocallySorted,
      Map<String, Long> expectedPartitionNumRecords) {
    int numPartitions = 2;
    JavaRDD<HoodieRecord> actualRecords = partitioner.repartitionRecords(records, numPartitions);
    assertEquals(numPartitions, actualRecords.getNumPartitions());
    List<HoodieRecord> collectedActualRecords = actualRecords.collect();
    if (isGloballySorted) {
      verifyRecordAscendingOrder(collectedActualRecords.iterator());
    } else if (isLocallySorted) {
      actualRecords.mapPartitions(partition -> {
        verifyRecordAscendingOrder(partition);
        return Collections.emptyList().iterator();
      }).collect();
    }

    Map<String, Long> actualPartitionNumRecords = new HashMap<>();
    for (HoodieRecord record : collectedActualRecords) {
      String partitionPath = record.getPartitionPath();
      actualPartitionNumRecords.put(partitionPath,
          actualPartitionNumRecords.getOrDefault(partitionPath, 0L) + 1);
    }
    assertEquals(expectedPartitionNumRecords, actualPartitionNumRecords);
  }

  @Test
  public void testGlobalSortPartitioner() throws Exception {
    testBulkInsertInternalPartitioner(new GlobalSortPartitioner(),
        generateTestRecordsForBulkInsert(jsc), true, true,
        generateExpectedPartitionNumRecords());
    testBulkInsertInternalPartitioner(new GlobalSortPartitioner(),
        generateTripleTestRecordsForBulkInsert(jsc), true, true,
        generateExpectedPartitionNumRecordsTriple());
  }

  @Test
  public void testRDDPartitionSortPartitioner() throws Exception {
    testBulkInsertInternalPartitioner(new RDDPartitionSortPartitioner(),
        generateTestRecordsForBulkInsert(jsc), false, true,
        generateExpectedPartitionNumRecords());
    testBulkInsertInternalPartitioner(new RDDPartitionSortPartitioner(),
        generateTripleTestRecordsForBulkInsert(jsc), false, true,
        generateExpectedPartitionNumRecordsTriple());
  }

  @Test
  public void testNonSortPartitioner() throws Exception {
    testBulkInsertInternalPartitioner(new NonSortPartitioner(),
        generateTestRecordsForBulkInsert(jsc), false, false,
        generateExpectedPartitionNumRecords());
    testBulkInsertInternalPartitioner(new NonSortPartitioner(),
        generateTripleTestRecordsForBulkInsert(jsc), false, false,
        generateExpectedPartitionNumRecordsTriple());
  }
}
