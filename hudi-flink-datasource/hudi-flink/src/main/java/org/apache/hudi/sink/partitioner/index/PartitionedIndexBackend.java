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

/**
 * Index backend for partition-scoped record-to-file-group mappings.
 *
 * <p>Unlike {@link GlobalIndexBackend}, this backend is addressed by both data partition and record
 * key. It is used by the partitioned RLI based dynamic bucket assignment path, where the persisted
 * index value is the file group id selected by {@code BucketAssigner}.
 */
public interface PartitionedIndexBackend extends IndexBackend {

  /**
   * Retrieves the file group id for a record in the given partition.
   *
   * @param partitionPath the partition path of the record
   * @param recordKey the unique key identifying the record
   * @return the file group id, or null if the record is not found in the index
   */
  String get(String partitionPath, String recordKey);

  /**
   * Updates the file group id for a record in the given partition.
   *
   * @param partitionPath the partition path of the record
   * @param recordKey the unique key identifying the record
   * @param fileId the file group id, usually the file id prefix produced by bucket assignment
   */
  void update(String partitionPath, String recordKey, String fileId);
}
