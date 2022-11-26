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

package org.apache.hudi.client;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieKeyWithLocation;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestWriteStatus {
  @Test
  public void testFailureFraction() {
    WriteStatus status = new WriteStatus(true, 0.1);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markFailure(new HoodieKeyWithLocation(new HoodieKey(UUID.randomUUID().toString(), "path1"), null, Option.empty()), t, null);
    }
    assertTrue(status.getFailedRecords().size() > 0);
    assertTrue(status.getFailedRecords().size() < 150); // 150 instead of 100, to prevent flaky test
    assertTrue(status.hasErrors());
  }

  @Test
  public void testSuccessRecordTracking() {
    WriteStatus status = new WriteStatus(false, 1.0);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markSuccess(new HoodieKeyWithLocation(new HoodieKey(UUID.randomUUID().toString(), "path1"), null, Option.empty()), Option.empty());
      status.markFailure(new HoodieKeyWithLocation(new HoodieKey(UUID.randomUUID().toString(), "path1"), null, Option.empty()), t, Option.empty());
    }
    assertEquals(1000, status.getFailedRecords().size());
    assertTrue(status.hasErrors());
    assertTrue(status.getWrittenRecords().isEmpty());
    assertEquals(2000, status.getTotalRecords());
  }

  @Test
  public void testFailureFractionExtended() {
    WriteStatus status = new WriteStatus(true, 0.1);
    String fileId = UUID.randomUUID().toString();
    String partitionPath = UUID.randomUUID().toString();
    status.setFileId(fileId);
    status.setPartitionPath(partitionPath);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markFailure(new HoodieKeyWithLocation(new HoodieKey(UUID.randomUUID().toString(), partitionPath), null, Option.empty()), t, Option.empty());
    }
    // verification
    assertEquals(fileId, status.getFileId());
    assertEquals(partitionPath, status.getPartitionPath());
    assertTrue(status.getFailedRecords().size() > 0);
    assertTrue(status.getFailedRecords().size() < 150); // 150 instead of 100, to prevent flaky test
    assertTrue(status.hasErrors());
  }

  @Test
  public void testSuccessRecordTrackingExtended() {
    boolean[] vals = {true, false};
    for (boolean trackSuccess : vals) {
      WriteStatus status = new WriteStatus(trackSuccess, 1.0);
      String fileId = UUID.randomUUID().toString();
      status.setFileId(fileId);
      String partitionPath = UUID.randomUUID().toString();
      status.setPartitionPath(partitionPath);
      Throwable t = new Exception("some error in writing");
      for (int i = 0; i < 1000; i++) {
        status.markSuccess(new HoodieKeyWithLocation(new HoodieKey(UUID.randomUUID().toString(), partitionPath), null, Option.empty()), Option.empty());
        status.markFailure(new HoodieKeyWithLocation(new HoodieKey(UUID.randomUUID().toString(), partitionPath), null, Option.empty()), t, Option.empty());
      }
      // verification
      assertEquals(fileId, status.getFileId());
      assertEquals(partitionPath, status.getPartitionPath());
      assertEquals(1000, status.getFailedRecords().size());
      assertTrue(status.hasErrors());
      if (trackSuccess) {
        assertEquals(1000, status.getWrittenRecords().size());
      } else {
        assertTrue(status.getWrittenRecords().isEmpty());
      }
      assertEquals(2000, status.getTotalRecords());
    }
  }

  @Test
  public void testGlobalError() {
    WriteStatus status = new WriteStatus(true, 0.1);
    Throwable t = new Exception("some error in writing");
    status.setGlobalError(t);
    assertEquals(t, status.getGlobalError());
  }
}
