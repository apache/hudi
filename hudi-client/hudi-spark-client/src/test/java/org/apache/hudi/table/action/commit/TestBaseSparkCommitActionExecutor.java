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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.config.HoodieWriteConfig.INSERT_SORT_MODE;
import static org.apache.hudi.config.HoodieWriteConfig.INSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBaseSparkCommitActionExecutor extends HoodieClientTestBase implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(TestBaseSparkCommitActionExecutor.class);

  @ParameterizedTest
  @ValueSource(strings = {"", "rider", "driver,rider"})
  public void testSortingInputRecordsForInsertOperation(String sortColumns) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    // Build the config
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(TRIP_EXAMPLE_SCHEMA).build();
    config.setValue(INSERT_SORT_MODE, "GLOBAL_SORT");
    config.setValue(INSERT_USER_DEFINED_PARTITIONER_SORT_COLUMNS, sortColumns);

    // Generate test data
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    List<HoodieRecord<RawTripTestPayload>> records = dataGenerator.generateInsertsWithHoodieAvroPayload("001", 500)
        .stream()
        .map(r -> (HoodieRecord<RawTripTestPayload>) r)
        .collect(Collectors.toList());

    // get the executor
    HoodieSparkCopyOnWriteTable table = (HoodieSparkCopyOnWriteTable) HoodieSparkTable.create(config, context, metaClient);
    BaseSparkCommitActionExecutor executor = new SparkInsertCommitActionExecutor<>(
        context, config, table, "001", HoodieJavaRDD.of(jsc.parallelize(records)));

    // Invoke private method to sort and index the input records
    Method method = BaseSparkCommitActionExecutor.class.getDeclaredMethod("getSortedIndexedRecords", HoodieData.class);
    method.setAccessible(true);
    JavaPairRDD<Tuple2<HoodieKey, Long>, HoodieRecord<RawTripTestPayload>> resultRdd =
        (JavaPairRDD<Tuple2<HoodieKey, Long>, HoodieRecord<RawTripTestPayload>>) method.invoke(
            executor, HoodieJavaRDD.of(jsc.parallelize(records)));
    List<Tuple2<Tuple2<HoodieKey, Long>, HoodieRecord<RawTripTestPayload>>> resultList = resultRdd.collect();

    String[] customSortColumns;
    if (!sortColumns.isEmpty()) {
      // Extract user specified sort-column fields as an array
      customSortColumns = Arrays.stream(sortColumns.split(","))
          .map(String::trim).toArray(String[]::new);
    } else {
      customSortColumns = null;
    }

    // Comparator to sort by either partition-path+record-key OR partition-path+custom-sort-col
    Comparator<HoodieRecord> c = (Comparator<HoodieRecord> & Serializable) (r1, r2) -> {
      String sortColString1;
      if (customSortColumns == null || customSortColumns.length == 0) {
        sortColString1 = r1.getRecordKey();
      } else {
        Object[] columnValues = r1.getColumnValues(new Schema.Parser().parse(config.getSchema()), customSortColumns, false);
        sortColString1 = Arrays.stream(columnValues).map(Object::toString).collect(Collectors.joining());
      }
      String k1 = new StringBuilder()
          .append(r1.getPartitionPath())
          .append("+")
          .append(sortColString1)
          .toString();

      String sortColString2;
      if (customSortColumns == null || customSortColumns.length == 0) {
        sortColString2 = r2.getRecordKey();
      } else {
        Object[] columnValues = r2.getColumnValues(new Schema.Parser().parse(config.getSchema()), customSortColumns, false);
        sortColString2 = Arrays.stream(columnValues).map(Object::toString).collect(Collectors.joining());
      }
      String k2 = new StringBuilder()
          .append(r2.getPartitionPath())
          .append("+")
          .append(sortColString2)
          .toString();
      return k1.compareTo(k2);
    };

    // Sort the input records
    records.sort(c);

    // Result size should match the input records size
    assertEquals(records.size(), resultList.size());

    // Assert that the result-set is sorted and sequentially indexed
    long prevIdx = -1;
    for (int i = 0; i < resultList.size(); i++) {
      // Result set is sequentially indexed
      long recordIdx = resultList.get(i)._1._2;
      assertEquals(prevIdx + 1, recordIdx);
      prevIdx++;

      // Result set is sorted and hence should match the sorted input record
      HoodieKey key = resultList.get(i)._1._1;
      String recordKey = key.getRecordKey();
      String partitionPath = key.getPartitionPath();
      assertEquals(records.get(i).getRecordKey(), recordKey);
      assertEquals(records.get(i).getPartitionPath(), partitionPath);
    }
  }
}
