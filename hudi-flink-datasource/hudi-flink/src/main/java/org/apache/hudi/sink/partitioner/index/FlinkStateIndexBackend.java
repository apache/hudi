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

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import org.apache.flink.api.common.state.ValueState;

import java.io.IOException;

/**
 * An implementation of {@link IndexBackend} based on flink keyed value state.
 */
public class FlinkStateIndexBackend implements IndexBackend {

  private final ValueState<HoodieRecordGlobalLocation> indexState;

  public FlinkStateIndexBackend(ValueState<HoodieRecordGlobalLocation> indexState) {
    this.indexState = indexState;
  }

  @Override
  public HoodieRecordGlobalLocation get(String recordKey) throws IOException {
    return indexState.value();
  }

  @Override
  public void update(String recordKey, HoodieRecordGlobalLocation recordGlobalLocation) throws IOException {
    this.indexState.update(recordGlobalLocation);
  }

  @Override
  public void close() throws IOException {
    // do nothing.
  }
}
