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

package org.apache.hudi.sink.utils.keyedstate;

import org.apache.flink.api.common.state.MapState;
import org.apache.hudi.sink.utils.MockMapState;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Mock keyed map state for testing.
 * @param <K> Type of state key
 * @param <V> Type of state value
 */
public class MockKeyedMapState<K, V> implements MapState<K, V>, Cloneable {
  private MockKeyContext keyContext;
  private Map<Object, MockMapState<K, V>> mockMapStateMap;

  public MockKeyedMapState(MockKeyContext keyContext) {
    Objects.requireNonNull(keyContext, "keyContext is null");
    this.keyContext = keyContext;
  }

  @Override
  public V get(K key) throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    return mapState.get(key);
  }

  @Override
  public void put(K key, V value) throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    mapState.put(key, value);
  }

  @Override
  public void putAll(Map<K, V> map) throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    mapState.putAll(map);
  }

  @Override
  public void remove(K key) throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    mapState.remove(key);
  }

  @Override
  public boolean contains(K key) throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    return mapState.contains(key);
  }

  @Override
  public Iterable<Map.Entry<K, V>> entries() throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    return mapState.entries();
  }

  @Override
  public Iterable<K> keys() throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    return mapState.keys();
  }

  @Override
  public Iterable<V> values() throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    return mapState.values();
  }

  @Override
  public Iterator<Map.Entry<K, V>> iterator() throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    return mapState.iterator();
  }

  @Override
  public boolean isEmpty() throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    return mapState.isEmpty();
  }

  @Override
  public void clear() {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockMapState<K, V> mapState = mockMapStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockMapState<K, V>());
    mapState.clear();
  }

  @Override
  public Object clone() {
    try {
      MockKeyedMapState<K, V> copy = (MockKeyedMapState<K, V>) super.clone();

      // deep copy keyContext
      MockKeyContext newKeyContext = new MockKeyContext();
      newKeyContext.setCurrentKey(keyContext.getCurrentKey());
      copy.keyContext = newKeyContext;

      // deep copy mockMapStateMap
      copy.mockMapStateMap = mockMapStateMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
        MockMapState<K, V> oldMapState = entry.getValue();
        MockMapState<K, V> newMapState = new MockMapState();
        oldMapState.entries().forEach(v -> newMapState.put(v.getKey(), v.getValue()));
        return newMapState;
      }));
      return copy;
    } catch (CloneNotSupportedException ex) {
      throw new RuntimeException(ex.getMessage());
    }
  }
}
