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

package org.apache.hudi.util;

import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for mini-batch write stat merging.
 */
class TestWriteStatMerger {

  @ParameterizedTest
  @ValueSource(longs = {110, 90, 0})
  void testReplacementFileSize(long latestSize) {
    HoodieWriteStat first = new HoodieWriteStat();
    first.setPath("partition/first.parquet");
    first.setFileSizeInBytes(100);
    first.setTotalWriteBytes(100);
    HoodieWriteStat second = new HoodieWriteStat();
    second.setPath("partition/second.parquet");
    second.setFileSizeInBytes(latestSize);
    second.setTotalWriteBytes(latestSize);

    HoodieWriteStat merged = WriteStatMerger.merge(first, second);
    assertEquals(second.getPath(), merged.getPath());
    assertEquals(latestSize, merged.getFileSizeInBytes());
    assertEquals(100 + latestSize, merged.getTotalWriteBytes());

    HoodieWriteStat third = new HoodieWriteStat();
    third.setPath("partition/third.parquet");
    third.setFileSizeInBytes(80);
    third.setTotalWriteBytes(80);
    merged = WriteStatMerger.merge(merged, third);
    assertEquals(third.getPath(), merged.getPath());
    assertEquals(80, merged.getFileSizeInBytes());
    assertEquals(180 + latestSize, merged.getTotalWriteBytes());
  }

  @Test
  void testDeltaWriteSizeAggregationIsPreserved() {
    HoodieDeltaWriteStat first = new HoodieDeltaWriteStat();
    first.setPath("partition/first.log");
    first.addLogFiles("first.log");
    first.setFileSizeInBytes(100);
    first.setTotalWriteBytes(100);
    HoodieDeltaWriteStat second = new HoodieDeltaWriteStat();
    second.setPath("partition/second.log");
    second.addLogFiles("second.log");
    second.setFileSizeInBytes(110);
    second.setTotalWriteBytes(110);

    HoodieDeltaWriteStat merged = (HoodieDeltaWriteStat) WriteStatMerger.merge(first, second);
    assertEquals(second.getPath(), merged.getPath());
    assertEquals(210, merged.getFileSizeInBytes());
    assertEquals(210, merged.getTotalWriteBytes());
    assertEquals(2, merged.getLogFiles().size());
  }
}
