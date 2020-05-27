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

import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.FileIOUtils;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie commit metadata {@link HoodieCommitMetadata}.
 */
public class TestHoodieCommitMetadata {

  @Test
  public void testPerfStatPresenceInHoodieMetadata() throws Exception {

    List<HoodieWriteStat> fakeHoodieWriteStats = HoodieTestUtils.generateFakeHoodieWriteStat(100);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    fakeHoodieWriteStats.forEach(stat -> commitMetadata.addWriteStat(stat.getPartitionPath(), stat));
    assertTrue(commitMetadata.getTotalCreateTime() > 0);
    assertTrue(commitMetadata.getTotalUpsertTime() > 0);
    assertTrue(commitMetadata.getTotalScanTime() > 0);
    assertTrue(commitMetadata.getTotalLogFilesCompacted() > 0);

    String serializedCommitMetadata = commitMetadata.toJsonString();
    HoodieCommitMetadata metadata =
        HoodieCommitMetadata.fromJsonString(serializedCommitMetadata, HoodieCommitMetadata.class);
    // Make sure timing metrics are not written to instant file
    assertEquals(0, (long) metadata.getTotalScanTime());
    assertTrue(metadata.getTotalLogFilesCompacted() > 0);
  }

  @Test
  public void testCompatibilityWithoutOperationType() throws Exception {
    // test compatibility of old version file
    String serializedCommitMetadata =
        FileIOUtils.readAsUTFString(TestHoodieCommitMetadata.class.getResourceAsStream("/old-version.commit"));
    HoodieCommitMetadata metadata =
        HoodieCommitMetadata.fromJsonString(serializedCommitMetadata, HoodieCommitMetadata.class);
    assertSame(metadata.getOperationType(), WriteOperationType.UNKNOWN);

    // test operate type
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    assertSame(commitMetadata.getOperationType(), WriteOperationType.INSERT);

    // test serialized
    serializedCommitMetadata = commitMetadata.toJsonString();
    metadata =
        HoodieCommitMetadata.fromJsonString(serializedCommitMetadata, HoodieCommitMetadata.class);
    assertSame(metadata.getOperationType(), WriteOperationType.INSERT);
  }
}
