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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieRecord;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestWriteStatus {
  @Test
  public void testFailureFraction() {
    WriteStatus status = new WriteStatus(true, 0.1);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markFailure(Mockito.mock(HoodieRecord.class), t, null);
    }
    assertTrue(status.getFailedRecords().size() > 0);
    assertTrue(status.getFailedRecords().size() < 150); // 150 instead of 100, to prevent flaky test
    assertTrue(status.hasErrors());

    assertFalse(status.hasGlobalError());
    status.setGlobalError(t);
    assertTrue(status.hasGlobalError());
    assertEquals(status.getGlobalError(), t);

    status.setTotalErrorRecords(1000);
    assertEquals(status.getTotalErrorRecords(), 1000);
  }

  @Test
  public void testSuccessRecordTracking() {
    WriteStatus status = new WriteStatus(false, 1.0);
    Throwable t = new Exception("some error in writing");
    for (int i = 0; i < 1000; i++) {
      status.markSuccess(Mockito.mock(HoodieRecord.class), null);
      status.markFailure(Mockito.mock(HoodieRecord.class), t, null);
    }
    assertEquals(1000, status.getFailedRecords().size());
    assertTrue(status.hasErrors());
    assertEquals(status.getErrors().size(), 1);
    assertTrue(status.getWrittenRecords().isEmpty());
    assertEquals(2000, status.getTotalRecords());

    status.setTotalRecords(1000);
    assertEquals(status.getTotalRecords(), 1000);
  }

  @Test
  public void testAdditional() {
    WriteStatus status = new WriteStatus(false, 1.0);
    // format is not enforced so we dont need to test it
    status.toString();
  }
}
