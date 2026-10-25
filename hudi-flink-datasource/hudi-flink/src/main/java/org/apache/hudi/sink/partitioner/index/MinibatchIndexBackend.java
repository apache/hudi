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
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Index delegator which supports mini-batch index operations.
 */
public interface MinibatchIndexBackend extends GlobalIndexBackend {

  /**
   * Retrieves locations for a batch of record keys.
   *
   * <p>The returned map should contain one entry for each requested key and preserve the request
   * order when the implementation can do so. Missing keys should map to {@code null}.
   *
   * @param recordKeys record keys to look up
   * @return record-key to global-location mapping
   */
  Map<String, HoodieRecordGlobalLocation> get(List<String> recordKeys) throws IOException;

  /**
   * Updates locations for a batch of record keys.
   *
   * @param recordKeysAndLocations record-key and location pairs to write into the backend
   */
  void update(List<Pair<String, HoodieRecordGlobalLocation>> recordKeysAndLocations) throws IOException;
}
