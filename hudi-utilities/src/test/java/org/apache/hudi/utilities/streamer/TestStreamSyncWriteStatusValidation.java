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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link StreamSync#sumRecordAndErrorCounts(JavaRDD)}, the single-pass fold that replaces two
 * separate {@code mapToDouble(...).sum()} actions in error-table commit validation.
 */
class TestStreamSyncWriteStatusValidation extends SparkClientFunctionalTestHarness {

  @Test
  void sumsTotalAndErroredRecordsAcrossPartitions() {
    List<WriteStatus> writeStatuses = new ArrayList<>();
    writeStatuses.add(writeStatus(10L, 3L));
    writeStatuses.add(writeStatus(20L, 5L));
    writeStatuses.add(writeStatus(7L, 0L));
    // More partitions than elements forces an empty partition, exercising the aggregate fold.
    JavaRDD<WriteStatus> writeStatusRDD = jsc().parallelize(writeStatuses, 4);

    Tuple2<Long, Long> counts = StreamSync.sumRecordAndErrorCounts(writeStatusRDD);

    assertEquals(37L, counts._1, "total records summed across all partitions");
    assertEquals(8L, counts._2, "total errored records summed across all partitions");
  }

  @Test
  void sumsToZeroWhenNoWriteStatusesPresent() {
    JavaRDD<WriteStatus> emptyRDD = jsc().parallelize(new ArrayList<WriteStatus>(), 2);

    Tuple2<Long, Long> counts = StreamSync.sumRecordAndErrorCounts(emptyRDD);

    assertEquals(0L, counts._1);
    assertEquals(0L, counts._2);
  }

  @Test
  void sumsToZeroForZeroPartitionRDD() {
    // BaseErrorTableWriter.upsert can return sc.emptyRDD() on an empty commit; JavaRDD.reduce throws
    // UnsupportedOperationException on a 0-partition RDD, whereas aggregate returns the zero value.
    JavaRDD<WriteStatus> emptyRDD = jsc().emptyRDD();

    Tuple2<Long, Long> counts = StreamSync.sumRecordAndErrorCounts(emptyRDD);

    assertEquals(0L, counts._1);
    assertEquals(0L, counts._2);
  }

  private static WriteStatus writeStatus(long totalRecords, long totalErrorRecords) {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.setTotalRecords(totalRecords);
    writeStatus.setTotalErrorRecords(totalErrorRecords);
    return writeStatus;
  }
}
