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

import org.apache.flink.api.common.state.ValueState;
import org.apache.hudi.sink.utils.MockValueState;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Mock keyed list state for testing.
 * @param <V> Type of state value
 */
public class MockKeyedValueState<V> implements ValueState<V>, Cloneable {
  private MockKeyContext keyContext;
  private Map<Object, MockValueState<V>> keyValueStateMap;

  public MockKeyedValueState(MockKeyContext keyContext) {
    Objects.requireNonNull(keyContext, "keyContext is null");
    this.keyContext = keyContext;
    this.keyValueStateMap = new HashMap<>();
  }

  @Override
  public V value() throws IOException {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockValueState<V> valueState = keyValueStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockValueState<>());
    return valueState.value();
  }

  @Override
  public void update(V value) throws IOException {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    MockValueState<V> valueState = keyValueStateMap.computeIfAbsent(keyContext.getCurrentKey(), k -> new MockValueState<>());
    valueState.update(value);
  }

  @Override
  public void clear() {
    Objects.requireNonNull(keyContext.getCurrentKey(), "currentKey is null");
    keyValueStateMap.remove(keyContext.getCurrentKey());
  }

  @Override
  public Object clone() {
    try {
      MockKeyedValueState<V> copy = (MockKeyedValueState<V>) super.clone();

      // deep copy keyContext
      MockKeyContext newKeyContext = new MockKeyContext();
      newKeyContext.setCurrentKey(keyContext.getCurrentKey());
      copy.keyContext = newKeyContext;

      // deep copy keyValueStateMap
      copy.keyValueStateMap = keyValueStateMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> {
        MockValueState<V> oldValueState = entry.getValue();
        MockValueState<V> newValueState = new MockValueState<>();
        newValueState.update(oldValueState.value());
        return newValueState;
      }));
      return copy;
    } catch (CloneNotSupportedException ex) {
      throw new RuntimeException(ex.getMessage());
    }
  }
}
