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

package org.apache.hudi.common.data;

import org.apache.hudi.common.engine.HoodieEngineContext;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestHoodieBroadcastScope {

  @Test
  void testBroadcastsEachInstanceOnceAndReleasesAllOnClose() {
    HoodieEngineContext engineContext = mock(HoodieEngineContext.class);
    List<HoodieBroadcast<?>> created = new ArrayList<>();
    when(engineContext.broadcast(any())).thenAnswer(invocation -> {
      HoodieBroadcast<?> broadcast = mock(HoodieBroadcast.class);
      created.add(broadcast);
      return broadcast;
    });
    List<String> value = new ArrayList<>();
    List<String> equalValue = new ArrayList<>();

    HoodieBroadcastScope scope = new HoodieBroadcastScope(engineContext);
    HoodieBroadcast<List<String>> first = scope.broadcast(value);
    assertSame(first, scope.broadcast(value));
    assertNotSame(first, scope.broadcast(equalValue));
    assertEquals(2, created.size());
    created.forEach(broadcast -> verify(broadcast, never()).destroy());

    scope.close();
    created.forEach(broadcast -> verify(broadcast, times(1)).destroy());
    scope.close();
    created.forEach(broadcast -> verify(broadcast, times(1)).destroy());
  }
}
