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

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * The broadcasts of one unit of work, e.g. the metadata table update of one commit, which {@link #close} releases
 * together once no task that reads them can still run. A value is broadcast once per scope, however often it is asked
 * for, so the steps of the work share one copy of it on each worker.
 */
public class HoodieBroadcastScope implements AutoCloseable {

  private final HoodieEngineContext engineContext;
  private final Map<Object, HoodieBroadcast<?>> broadcasts = new IdentityHashMap<>();

  public HoodieBroadcastScope(HoodieEngineContext engineContext) {
    this.engineContext = engineContext;
  }

  /**
   * Broadcasts the value, or returns the broadcast of the same instance made earlier in this scope.
   */
  @SuppressWarnings("unchecked")
  public synchronized <T> HoodieBroadcast<T> broadcast(T value) {
    return (HoodieBroadcast<T>) broadcasts.computeIfAbsent(value, engineContext::broadcast);
  }

  /**
   * Releases every broadcast of this scope.
   */
  @Override
  public synchronized void close() {
    List<HoodieBroadcast<?>> toRelease = new ArrayList<>(broadcasts.values());
    broadcasts.clear();
    toRelease.forEach(HoodieBroadcast::destroy);
  }
}
