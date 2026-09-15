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

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.SerializationUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link SparkSampleWritesUtils#takeBoundedSample(Iterator, int, long)}, which caps
 * the sample used for record-size estimation by both record count and total serialized size so the
 * single-task sample write cannot exceed {@code spark.rpc.message.maxSize}.
 */
public class TestSparkSampleWritesBounding {

  private static final String COMMIT_TIME = "001";

  private static List<HoodieRecord> drain(Iterator<HoodieRecord> iterator) {
    List<HoodieRecord> records = new ArrayList<>();
    iterator.forEachRemaining(records::add);
    return records;
  }

  /** Total serialized size the bounding logic accumulates for the first {@code count} records. */
  private static long serializedSizeOfFirst(List<HoodieRecord> records, int count) throws Exception {
    long bytes = 0L;
    for (int i = 0; i < count; i++) {
      HoodieRecord source = records.get(i);
      HoodieRecord sample = source.newInstance(new HoodieKey(source.getRecordKey(), ""));
      bytes += SerializationUtils.serialize(sample).length;
    }
    return bytes;
  }

  @Test
  public void boundsSampleByRecordCountAndSerializedSize() throws Exception {
    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED)) {
      List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, 50);

      // Record-count cap: a generous byte budget lets the count limit decide.
      List<HoodieRecord> byCount = drain(
          SparkSampleWritesUtils.takeBoundedSample(records.iterator(), 10, Long.MAX_VALUE));
      assertEquals(10, byCount.size(), "Record count limit should cap the sample size.");
      byCount.forEach(record ->
          assertEquals("", record.getPartitionPath(), "Sampled records should have an empty partition path."));

      // Byte-budget cap: a budget that fits exactly the first three records; the fourth pushes over.
      long budgetForThree = serializedSizeOfFirst(records, 3);
      List<HoodieRecord> byBytes = drain(
          SparkSampleWritesUtils.takeBoundedSample(records.iterator(), Integer.MAX_VALUE, budgetForThree));
      assertEquals(3, byBytes.size(), "Byte budget should stop sampling once the total serialized size is reached.");
    }
  }

  @Test
  public void atLeastOneRecordIsAlwaysRetained() throws Exception {
    try (HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(0xDEED)) {
      List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, 50);

      // A budget smaller than any single record still yields one record so an estimate is possible.
      List<HoodieRecord> sampled = drain(
          SparkSampleWritesUtils.takeBoundedSample(records.iterator(), Integer.MAX_VALUE, 1L));

      assertEquals(1, sampled.size(), "At least one record must be retained even under a tiny budget.");
    }
  }
}
