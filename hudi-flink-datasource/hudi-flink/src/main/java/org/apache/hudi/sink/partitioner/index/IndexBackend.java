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
import org.apache.hudi.sink.event.Correspondent;

import java.io.Closeable;
import java.io.IOException;

/**
 * An interface that provides an abstraction for managing record location mappings in the index system.
 * It serves as the backend for storing and retrieving the location of records identified by their unique keys.
 */
public interface IndexBackend extends Closeable {

  /**
   * Retrieves the global location of a record based on its key.
   *
   * @param recordKey the unique key identifying the record
   * @return the global location of the record, or null if the record is not found in the index
   */
  HoodieRecordGlobalLocation get(String recordKey) throws IOException;

  /**
   * Updates the global location of a record in the index.
   *
   * @param recordKey the unique key identifying the record
   * @param recordGlobalLocation the new global location of the record
   */
  void update(String recordKey, HoodieRecordGlobalLocation recordGlobalLocation) throws IOException;

  /**
   * Listener method called when the bucket assign operator finishes the checkpoint with {@code checkpointId}.
   *
   * @param checkpointId checkpoint id.
   */
  default void onCheckpoint(long checkpointId) {
    // do nothing.
  }

  /**
   * Listener method called when the bucket assign operator receives a notify checkpoint complete event.
   *
   * @param correspondent The Correspondent used to get inflight instants from the coordinator.
   */
  default void onCheckpointComplete(Correspondent correspondent) {
    // do nothing.
  }
}
