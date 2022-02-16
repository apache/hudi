/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.testutils.HoodieJavaClientTestBase;

import org.apache.avro.Schema;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJavaBulkInsertInternalPartitioner extends HoodieJavaClientTestBase {
  private static final Comparator<HoodieRecord> KEY_COMPARATOR =
      Comparator.comparing(o -> (o.getPartitionPath() + "+" + o.getRecordKey()));

  public static List<HoodieRecord> generateTestRecordsForBulkInsert(int numRecords) {
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGenerator.generateInserts("0", numRecords);
    return records;
  }

  public static Map<String, Long> generatePartitionNumRecords(List<HoodieRecord> records) {
    return records.stream().map(record -> record.getPartitionPath())
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
  }

  @ParameterizedTest
  @ValueSource(strings = {"rider", "rider,driver"})
  public void testCustomColumnSortPartitioner(String sortColumnString) throws Exception {
    String[] sortColumns = sortColumnString.split(",");
    Comparator<HoodieRecord> columnComparator =
        getCustomColumnComparator(HoodieTestDataGenerator.AVRO_SCHEMA, sortColumns);

    List<HoodieRecord> records = generateTestRecordsForBulkInsert(1000);
    testBulkInsertInternalPartitioner(
        new JavaCustomColumnsSortPartitioner(sortColumns, HoodieTestDataGenerator.AVRO_SCHEMA, false),
        records, true, generatePartitionNumRecords(records), Option.of(columnComparator));
  }

  private Comparator<HoodieRecord> getCustomColumnComparator(Schema schema, String[] sortColumns) {
    return Comparator.comparing(
        record -> HoodieAvroUtils.getRecordColumnValues(record, sortColumns, schema, false).toString());
  }

  private void verifyRecordAscendingOrder(List<HoodieRecord> records,
                                          Option<Comparator<HoodieRecord>> comparator) {
    List<HoodieRecord> expectedRecords = new ArrayList<>(records);
    Collections.sort(expectedRecords, comparator.orElse(KEY_COMPARATOR));
    assertEquals(expectedRecords, records);
  }

  private void testBulkInsertInternalPartitioner(BulkInsertPartitioner partitioner,
                                                 List<HoodieRecord> records,
                                                 boolean isSorted,
                                                 Map<String, Long> expectedPartitionNumRecords,
                                                 Option<Comparator<HoodieRecord>> comparator) {
    List<HoodieRecord> actualRecords =
        (List<HoodieRecord>) partitioner.repartitionRecords(records, 1);
    if (isSorted) {
      // Verify global order
      verifyRecordAscendingOrder(actualRecords, comparator);
    }

    // Verify number of records per partition path
    assertEquals(expectedPartitionNumRecords, generatePartitionNumRecords(actualRecords));
  }
}
