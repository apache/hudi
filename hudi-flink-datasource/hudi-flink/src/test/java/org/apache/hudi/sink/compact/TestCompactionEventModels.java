/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.compact;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.CompactionOperation;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestCompactionEventModels {

  @Test
  void testPlanEventConstructorsAndAccessors() {
    CompactionOperation operation = mock(CompactionOperation.class);
    CompactionPlanEvent event = new CompactionPlanEvent("001", operation, 3, true, true);

    assertEquals("001", event.getCompactionInstantTime());
    assertSame(operation, event.getOperation());
    assertEquals(3, event.getIndex());
    assertTrue(event.isMetadataTable());
    assertTrue(event.isLogCompaction());
  }

  @Test
  void testCommitEventDistinguishesSuccessAndFailure() {
    CompactionCommitEvent failed = new CompactionCommitEvent("001", "file-1", 2, false, true);
    assertTrue(failed.isFailed());
    assertTrue(failed.isLogCompaction());

    WriteStatus status = mock(WriteStatus.class);
    CompactionCommitEvent success = new CompactionCommitEvent(
        "002", "file-2", Collections.singletonList(status), 4, true, false);
    assertFalse(success.isFailed());
    assertEquals("002", success.getInstant());
    assertEquals("file-2", success.getFileId());
    assertSame(status, success.getWriteStatuses().get(0));
    assertEquals(4, success.getTaskID());
    assertTrue(success.isMetadataTable());
  }
}
