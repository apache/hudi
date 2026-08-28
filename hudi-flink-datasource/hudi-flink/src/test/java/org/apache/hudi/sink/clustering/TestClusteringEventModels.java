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

package org.apache.hudi.sink.clustering;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.ClusteringGroupInfo;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class TestClusteringEventModels {

  @Test
  void testPlanEventAccessors() {
    ClusteringGroupInfo group = mock(ClusteringGroupInfo.class);
    Map<String, String> params = Collections.singletonMap("strategy", "sort");
    ClusteringPlanEvent event = new ClusteringPlanEvent("001", group, params);
    event.setIndex(7);

    assertEquals("001", event.getClusteringInstantTime());
    assertSame(group, event.getClusteringGroupInfo());
    assertSame(params, event.getStrategyParams());
    assertEquals(7, event.getIndex());
  }

  @Test
  void testCommitEventDistinguishesSuccessAndFailure() {
    assertTrue(new ClusteringCommitEvent("001", "file-1", 1).isFailed());

    WriteStatus status = mock(WriteStatus.class);
    ClusteringCommitEvent success = new ClusteringCommitEvent(
        "002", "file-2", Collections.singletonList(status), 2);
    assertFalse(success.isFailed());
    assertEquals("002", success.getInstant());
    assertEquals("file-2", success.getFileIds());
    assertSame(status, success.getWriteStatuses().get(0));
    assertEquals(2, success.getTaskID());
  }
}
