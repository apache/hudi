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

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;

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
  }
}
