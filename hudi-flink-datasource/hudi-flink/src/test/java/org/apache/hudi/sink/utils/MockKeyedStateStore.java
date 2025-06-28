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

import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.hudi.adapter.KeyedStateStoreAdapter;
import org.apache.hudi.sink.utils.keyedstate.MockKeyContext;
import org.apache.hudi.sink.utils.keyedstate.MockKeyedListState;
import org.apache.hudi.sink.utils.keyedstate.MockKeyedMapState;
import org.apache.hudi.sink.utils.keyedstate.MockKeyedValueState;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * This class simulates a keyed state store for testing purposes.
 * It provides methods to manage different types of state (ValueState, ListState, MapState)
 * and supports checkpointing and rollback functionalities.
 */
@SuppressWarnings("rawtypes")
public class MockKeyedStateStore implements KeyedStateStoreAdapter {
  private final MockKeyContext keyContext;

  private Map<String, MockKeyedValueState> keyedValueStateMap;
  private Map<String, MockKeyedListState> keyedListStateMap;
  private Map<String, MockKeyedMapState> keyedMapStateMap;

  private final Map<Long, Map<String, MockKeyedValueState>> historyValueStateMap;
  private final Map<Long, Map<String, MockKeyedListState>> historyListStateMap;
  private final Map<Long, Map<String, MockKeyedMapState>> historyMapStateMap;

  private Map<String, MockKeyedValueState> lastSuccessValueStateMap;
  private Map<String, MockKeyedListState> lastSuccessListStateMap;
  private Map<String, MockKeyedMapState> lastSuccessMapStateMap;

  public MockKeyedStateStore() {
    this.keyContext = new MockKeyContext();

    this.keyedValueStateMap = new HashMap<>();
    this.keyedListStateMap = new HashMap<>();
    this.keyedMapStateMap = new HashMap<>();

    this.historyValueStateMap = new HashMap<>();
    this.historyListStateMap = new HashMap<>();
    this.historyMapStateMap = new HashMap<>();

    this.lastSuccessValueStateMap = new HashMap<>();
    this.lastSuccessListStateMap = new HashMap<>();
    this.lastSuccessMapStateMap = new HashMap<>();
  }

  @Override
  public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
    String name = stateProperties.getName();
    keyedValueStateMap.putIfAbsent(name, new MockKeyedValueState<T>(keyContext));
    return keyedValueStateMap.get(name);
  }

  @Override
  public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
    String name = stateProperties.getName();
    keyedListStateMap.putIfAbsent(name, new MockKeyedListState<T>(keyContext));
    return keyedListStateMap.get(name);
  }

  @Override
  public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
    throw new UnsupportedOperationException("getReducingState is not supported yet");
  }

  @Override
  public <I, A, O> AggregatingState<I, O> getAggregatingState(AggregatingStateDescriptor<I, A, O> stateProperties) {
    throw new UnsupportedOperationException("getAggregatingState is not supported yet");
  }

  @Override
  public <K, V> MapState<K, V> getMapState(MapStateDescriptor<K, V> stateProperties) {
    String name = stateProperties.getName();
    keyedMapStateMap.putIfAbsent(name, new MockKeyedMapState(keyContext));
    return keyedMapStateMap.get(name);
  }

  public void setCurrentKey(Object currentKey) {
    Objects.requireNonNull(currentKey, "currentKey is null");
    keyContext.setCurrentKey(currentKey);
  }

  public void checkpointBegin(long checkpointId) throws Exception {
    historyValueStateMap.put(checkpointId, copyKeyedValueStates(keyedValueStateMap));

    historyListStateMap.put(checkpointId, copyKeyedListStates(keyedListStateMap));

    historyMapStateMap.put(checkpointId, copyKeyedMapStates(keyedMapStateMap));
  }

  public void checkpointSuccess(long checkpointId) {
    lastSuccessValueStateMap = historyValueStateMap.get(checkpointId);
    lastSuccessListStateMap = historyListStateMap.get(checkpointId);
    lastSuccessMapStateMap = historyMapStateMap.get(checkpointId);
  }

  public void rollBackToLastSuccessCheckpoint() {
    this.keyedValueStateMap = copyKeyedValueStates(lastSuccessValueStateMap);
    this.keyedListStateMap = copyKeyedListStates(lastSuccessListStateMap);
    this.keyedMapStateMap = copyKeyedMapStates(lastSuccessMapStateMap);
  }

  @SuppressWarnings("unchecked")
  private Map<String, MockKeyedValueState> copyKeyedValueStates(Map<String, MockKeyedValueState> keyedValueStateMap) {
    return Collections.unmodifiableMap(keyedValueStateMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> (MockKeyedValueState) entry.getValue().clone())));
  }

  @SuppressWarnings("unchecked")
  private Map<String, MockKeyedListState> copyKeyedListStates(Map<String, MockKeyedListState> keyedListStateMap) {
    return Collections.unmodifiableMap(keyedListStateMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> (MockKeyedListState) entry.getValue().clone())));
  }

  @SuppressWarnings("unchecked")
  private Map<String, MockKeyedMapState> copyKeyedMapStates(Map<String, MockKeyedMapState> keyedMapStateMap) {
    return Collections.unmodifiableMap(keyedMapStateMap.entrySet().stream().collect(
            Collectors.toMap(Map.Entry::getKey, entry -> (MockKeyedMapState) entry.getValue().clone())));
  }
}
