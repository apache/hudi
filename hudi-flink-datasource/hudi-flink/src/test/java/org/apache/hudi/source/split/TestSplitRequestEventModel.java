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

package org.apache.hudi.source.split;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSplitRequestEventModel {

  @Test
  void testAllConstructorsAndAccessors() {
    SplitRequestEvent empty = new SplitRequestEvent();
    assertTrue(empty.finishedSplitIds().isEmpty());
    assertNull(empty.requesterHostname());

    SplitRequestEvent withoutHost = new SplitRequestEvent(Collections.singletonList("split-1"));
    assertEquals(Collections.singletonList("split-1"), withoutHost.finishedSplitIds());
    assertNull(withoutHost.requesterHostname());

    SplitRequestEvent withHost = new SplitRequestEvent(
        Arrays.asList("split-1", "split-2"), "worker.example");
    assertEquals(Arrays.asList("split-1", "split-2"), withHost.finishedSplitIds());
    assertEquals("worker.example", withHost.requesterHostname());
  }
}
