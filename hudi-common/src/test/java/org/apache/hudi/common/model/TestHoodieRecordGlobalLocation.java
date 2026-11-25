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

package org.apache.hudi.common.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieRecordGlobalLocation {
  @Test
  void testEquals() {
    String partitionPath = "any_partition";
    String instantTime = "any_instant_time";
    String fileId = "any_file_id";
    HoodieRecordGlobalLocation thisLocation =
        new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId, 0L);
    HoodieRecordGlobalLocation thatLocation =
        new HoodieRecordGlobalLocation(partitionPath, instantTime, fileId, 1L);
    assertTrue(thisLocation.equals(thatLocation));
  }

  @Test
  void testFromLocalKeepsPosition() {
    HoodieRecordGlobalLocation location =
        HoodieRecordGlobalLocation.fromLocal("any_partition", new HoodieRecordLocation("any_instant_time", "any_file_id", 7L));
    assertEquals("any_partition", location.getPartitionPath());
    assertEquals("any_instant_time", location.getInstantTime());
    assertEquals("any_file_id", location.getFileId());
    assertEquals(7L, location.getPosition());
  }
}
