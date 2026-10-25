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

package org.apache.hudi.hive;

import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEFAULT_PARTITION_PATH;
import static org.apache.hudi.common.util.PartitionPathEncodeUtils.DEPRECATED_DEFAULT_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestPartitionValueExtractor {
  @Test
  public void testHourPartition() {
    SlashEncodedHourPartitionValueExtractor hourPartition = new SlashEncodedHourPartitionValueExtractor();
    List<String> list = new ArrayList<>();
    list.add("2020-12-20-01");
    assertEquals(hourPartition.extractPartitionValuesInPath("2020/12/20/01"), list);
    assertThrows(IllegalArgumentException.class, () -> hourPartition.extractPartitionValuesInPath("2020/12/20"));
    assertEquals(hourPartition.extractPartitionValuesInPath("update_time=2020/12/20/01"), list);
    // a null partition value is written to the single-segment default-partition directory rather
    // than the yyyy/mm/dd/HH layout, and has to survive the extractor rather than blow it up
    assertEquals(
        Collections.singletonList(DEFAULT_PARTITION_PATH),
        hourPartition.extractPartitionValuesInPath(DEFAULT_PARTITION_PATH));
    assertEquals(
        Collections.singletonList(DEFAULT_PARTITION_PATH),
        hourPartition.extractPartitionValuesInPath("update_time=" + DEFAULT_PARTITION_PATH));
    // the pre-0.12 marker lands in a "default" directory and maps to the same Hive null marker
    assertEquals(
        Collections.singletonList(DEFAULT_PARTITION_PATH),
        hourPartition.extractPartitionValuesInPath(DEPRECATED_DEFAULT_PARTITION_PATH));
    assertEquals(
        Collections.singletonList(DEFAULT_PARTITION_PATH),
        hourPartition.extractPartitionValuesInPath("update_time=" + DEPRECATED_DEFAULT_PARTITION_PATH));
  }

  @Test
  public void testDayPartition() {
    SlashEncodedDayPartitionValueExtractor dayPartition = new SlashEncodedDayPartitionValueExtractor();
    assertEquals(
        Collections.singletonList("2026-01-05"),
        dayPartition.extractPartitionValuesInPath("2026/01/05"));
    assertEquals(
        Collections.singletonList("2026-01-05"),
        dayPartition.extractPartitionValuesInPath("datestr=2026/01/05"));
    assertThrows(IllegalArgumentException.class, () -> dayPartition.extractPartitionValuesInPath("2026/01"));
    // a null partition value is written to the single-segment default-partition directory rather
    // than the yyyy/mm/dd layout, and has to survive the extractor rather than blow it up
    assertEquals(
        Collections.singletonList(DEFAULT_PARTITION_PATH),
        dayPartition.extractPartitionValuesInPath(DEFAULT_PARTITION_PATH));
    assertEquals(
        Collections.singletonList(DEFAULT_PARTITION_PATH),
        dayPartition.extractPartitionValuesInPath("datestr=" + DEFAULT_PARTITION_PATH));
    // the pre-0.12 marker lands in a "default" directory and maps to the same Hive null marker
    assertEquals(
        Collections.singletonList(DEFAULT_PARTITION_PATH),
        dayPartition.extractPartitionValuesInPath(DEPRECATED_DEFAULT_PARTITION_PATH));
    assertEquals(
        Collections.singletonList(DEFAULT_PARTITION_PATH),
        dayPartition.extractPartitionValuesInPath("datestr=" + DEPRECATED_DEFAULT_PARTITION_PATH));
  }

  @Test
  public void testHiveStylePartition() {
    HiveStylePartitionValueExtractor hiveStylePartition = new HiveStylePartitionValueExtractor();
    List<String> list = new ArrayList<>();
    list.add("2021-04-02");
    assertEquals(hiveStylePartition.extractPartitionValuesInPath("datestr=2021-04-02"), list);
    assertThrows(
        IllegalArgumentException.class,
        () -> hiveStylePartition.extractPartitionValuesInPath("2021/04/02"));
    // Only the first '=' is the separator, so a value containing '=' is preserved.
    assertEquals(
        Collections.singletonList("a=b=c"),
        hiveStylePartition.extractPartitionValuesInPath("k=a=b=c"));
    // base64-encoded value with '=' padding must not be truncated
    assertEquals(
        Collections.singletonList("YWJjZA=="),
        hiveStylePartition.extractPartitionValuesInPath("col=YWJjZA=="));
  }

  @Test
  public void testSinglePartPartition() {
    PartitionValueExtractor extractor = new SinglePartPartitionValueExtractor();
    assertEquals(
        Collections.singletonList("202210-01-20"),
        extractor.extractPartitionValuesInPath("202210/01/20"));
  }
}
