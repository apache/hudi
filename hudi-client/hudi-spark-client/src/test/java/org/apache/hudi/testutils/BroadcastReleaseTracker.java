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

package org.apache.hudi.testutils;

import org.apache.hudi.common.data.HoodieBroadcast;
import org.apache.hudi.common.engine.HoodieEngineContext;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Tracks the broadcasts created through a spied engine context and whether their callers release them. A released
 * handle stays usable, so a test can still serialize the task functions that captured it.
 */
public class BroadcastReleaseTracker {

  private final List<TrackedBroadcast<?>> broadcasts = new CopyOnWriteArrayList<>();

  /**
   * Makes {@code spyContext} return tracked handles from {@link HoodieEngineContext#broadcast}.
   */
  public static BroadcastReleaseTracker install(HoodieEngineContext spyContext) {
    BroadcastReleaseTracker tracker = new BroadcastReleaseTracker();
    doAnswer(invocation -> tracker.track((HoodieBroadcast<?>) invocation.callRealMethod())).when(spyContext).broadcast(any());
    return tracker;
  }

  public int created() {
    return broadcasts.size();
  }

  public void assertAllReleased() {
    assertTrue(broadcasts.stream().allMatch(broadcast -> broadcast.released),
        "Released " + broadcasts.stream().filter(broadcast -> broadcast.released).count() + " of " + broadcasts.size() + " broadcasts");
  }

  private <T> HoodieBroadcast<T> track(HoodieBroadcast<T> broadcast) {
    TrackedBroadcast<T> tracked = new TrackedBroadcast<>(broadcast);
    broadcasts.add(tracked);
    return tracked;
  }

  private static class TrackedBroadcast<T> implements HoodieBroadcast<T> {
    private static final long serialVersionUID = 1L;

    private final HoodieBroadcast<T> delegate;
    private transient volatile boolean released;

    TrackedBroadcast(HoodieBroadcast<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public T value() {
      return delegate.value();
    }

    @Override
    public void destroy() {
      released = true;
    }
  }
}
