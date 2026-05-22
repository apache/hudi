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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.streaming.api.functions.sink.filesystem.TestUtils;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Mock keyed list state for testing.
 * @param <T> Type of state value
 */
public class MockKeyedListState<T> implements ListState<T>, Cloneable  {
  private MockKeyContext keyContext;
  private Map<Object, TestUtils.MockListState<T>> mockListStateMap;

  public MockKeyedListState(MockKeyContext keyContext) {
    Objects.requireNonNull(keyContext, "keyContext is null");
    this.keyContext = keyContext;
  }

  @Override
  public void update(List<T> values) throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    TestUtils.MockListState<T> listState = mockListStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new TestUtils.MockListState<>());
    listState.update(values);
  }

  @Override
  public void addAll(List<T> values) throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    TestUtils.MockListState<T> listState = mockListStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new TestUtils.MockListState<>());
    listState.addAll(values);
  }

  @Override
  public Iterable<T> get() throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    TestUtils.MockListState<T> listState = mockListStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new TestUtils.MockListState<>());
    return listState.get();
  }

  @Override
  public void add(T value) throws Exception {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    TestUtils.MockListState<T> listState = mockListStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new TestUtils.MockListState<>());
    listState.add(value);
  }

  @Override
  public void clear() {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    TestUtils.MockListState<T> listState = mockListStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new TestUtils.MockListState<>());
    listState.clear();
  }

  @Override
  public Object clone() {
    try {
      MockKeyedListState<T> copy = (MockKeyedListState<T>) super.clone();

      // deep copy keyContext
      MockKeyContext newKeyContext = new MockKeyContext();
      newKeyContext.setCurrentKey(keyContext.getCurrentKey());
      copy.keyContext = newKeyContext;

      // deep copy mockListStateMap
      copy.mockListStateMap = mockListStateMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
        TestUtils.MockListState<T> oldListState = entry.getValue();
        TestUtils.MockListState<T> newListState = new TestUtils.MockListState<T>();
        newListState.addAll(oldListState.getBackingList());
        return newListState;
      }));
      return copy;
    } catch (CloneNotSupportedException ex) {
      throw new RuntimeException(ex.getMessage());
    }
  }
}
