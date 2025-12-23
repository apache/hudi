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

package org.apache.hudi.adapter;

import org.apache.flink.api.common.state.KeyedStateStore;

import javax.annotation.Nonnull;

public interface KeyedStateStoreAdapter extends KeyedStateStore {

  default <T> org.apache.flink.api.common.state.v2.ValueState<T> getValueState(@Nonnull org.apache.flink.api.common.state.v2.ValueStateDescriptor<T> valueStateDescriptor) {
    throw new UnsupportedOperationException("getValueState() v2 is not supported.");
  }

  default <T> org.apache.flink.api.common.state.v2.ListState<T> getListState(@Nonnull org.apache.flink.api.common.state.v2.ListStateDescriptor<T> listStateDescriptor) {
    throw new UnsupportedOperationException("getListState() v2 is not supported.");
  }

  default <K, V> org.apache.flink.api.common.state.v2.MapState<K, V> getMapState(@Nonnull org.apache.flink.api.common.state.v2.MapStateDescriptor<K, V> mapStateDescriptor) {
    throw new UnsupportedOperationException("getMapState() v2 is not supported.");
  }

  default <T> org.apache.flink.api.common.state.v2.ReducingState<T> getReducingState(@Nonnull org.apache.flink.api.common.state.v2.ReducingStateDescriptor<T> reducingStateDescriptor) {
    throw new UnsupportedOperationException("getReducingState() v2 is not supported.");
  }

  default <I, A, O> org.apache.flink.api.common.state.v2.AggregatingState<I, O> getAggregatingState(
      @Nonnull org.apache.flink.api.common.state.v2.AggregatingStateDescriptor<I, A, O> aggregatingStateDescriptor) {
    throw new UnsupportedOperationException("getAggregatingState() v2 is not supported.");
  }

}
