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
 * KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.read;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestHoodieReadStats {

  @Test
  void testAdditionalLogScanMetricsAreTracked() {
    HoodieReadStats readStats = new HoodieReadStats();

    readStats.setTotalLogBlocks(3);
    readStats.setTotalValidLogBlocks(2);
    readStats.setTotalLogBlocksSize(1024L);
    readStats.setTotalLogBlocksScanTimeMs(25L);
    readStats.setTotalLogSizeCompacted(512L);

    assertEquals(3, readStats.getTotalLogBlocks());
    assertEquals(2, readStats.getTotalValidLogBlocks());
    assertEquals(1024L, readStats.getTotalLogBlocksSize());
    assertEquals(25L, readStats.getTotalLogBlocksScanTimeMs());
    assertEquals(512L, readStats.getTotalLogSizeCompacted());
  }
}
