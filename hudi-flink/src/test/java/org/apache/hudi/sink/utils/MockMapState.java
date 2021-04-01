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

package org.apache.hudi.sink.utils;

import org.apache.flink.api.common.state.MapState;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Mock map state for testing.
 *
 * @param <K> Type of state key
 * @param <V> Type of state value
 */
public class MockMapState<K, V> implements MapState<K, V> {
  private final Map<K, V> backingMap = new HashMap<>();

  @Override
  public V get(K uk) {
    return backingMap.get(uk);
  }

  @Override
  public void put(K uk, V uv) {
    backingMap.put(uk, uv);
  }

  @Override
  public void putAll(Map<K, V> map) {
    backingMap.putAll(map);
  }

  @Override
  public void remove(K uk) {
    backingMap.remove(uk);
  }

  @Override
  public boolean contains(K uk) {
    return backingMap.containsKey(uk);
  }

  @Override
  public Iterable<Map.Entry<K, V>> entries() {
    return backingMap.entrySet();
  }

  @Override
  public Iterable<K> keys() {
    return backingMap.keySet();
  }

  @Override
  public Iterable<V> values() {
    return backingMap.values();
  }

  @Override
  public Iterator<Map.Entry<K, V>> iterator() {
    return backingMap.entrySet().iterator();
  }

  @Override
  public boolean isEmpty() {
    return backingMap.isEmpty();
  }

  @Override
  public void clear() {
    backingMap.clear();
  }
}
