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

package org.apache.hudi.common.model;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestHoodieConsistentHashingMetadata {

  @Test
  public void testGetTimestamp() {
    Assertions.assertTrue(HoodieConsistentHashingMetadata.getTimestampFromFile("0000.hashing_metadata").equals("0000"));
    Assertions.assertTrue(HoodieConsistentHashingMetadata.getTimestampFromFile("1234.hashing_metadata").equals("1234"));
  }

  @Test
  public void testPartitionPathToUuidPostfix() {
    Assertions.assertEquals("0000-0000-0000-000000000000", HoodieConsistentHashingMetadata.partitionPathToUuidPostfix(""));
    Assertions.assertEquals("0000-0000-0000-0000000part1", HoodieConsistentHashingMetadata.partitionPathToUuidPostfix("part1"));
    Assertions.assertEquals("0000-0000-0000-00part1part2", HoodieConsistentHashingMetadata.partitionPathToUuidPostfix("part1/part2"));
    Assertions.assertEquals("0000-0000-0par-t1part2part3", HoodieConsistentHashingMetadata.partitionPathToUuidPostfix("part1/part2/part3"));
    Assertions.assertEquals("0yea-r=20-22mo-nth=12day=31",
        HoodieConsistentHashingMetadata.partitionPathToUuidPostfix("year=2022/month=12/day=31"));
    Assertions.assertEquals("xyea-r=20-22mo-nth=12day=31",
        HoodieConsistentHashingMetadata.partitionPathToUuidPostfix("region=xx/year=2022/month=12/day=31"));
  }

  @Test
  public void testConstructDefaultHashingNodes() {
    List<ConsistentHashingNode> nodes = HoodieConsistentHashingMetadata.constructDefaultHashingNodes("part1", 8);
    Assertions.assertEquals("00000000-0000-0000-0000-0000000part1", nodes.get(0).getFileIdPrefix());
    Assertions.assertEquals("00000001-0000-0000-0000-0000000part1", nodes.get(1).getFileIdPrefix());
    Assertions.assertEquals("00000007-0000-0000-0000-0000000part1", nodes.get(7).getFileIdPrefix());
  }
}
