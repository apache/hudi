/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;

public interface ExtensibleBucketInsertPartitioner {

  /**
   * Add pending extensible bucket metadata.
   * When call this method, the bulk insert will directly use the pending metadata as the bucket metadata for writing data to after-resizing buckets.
   * Used in the case of executing bulk insert.
   * NOTE: This method should be called before the bulk insert operation, and will skip building identifiers from records, just use the pending metadata.
   * For which not calling this method, the bulk insert will use the committed metadata as the bucket metadata and disallow writing data to the pending-resizing buckets.
   * @param metadata pending bucket-resizing metadata
   */
  void addPendingExtensibleBucketMetadata(HoodieExtensibleBucketMetadata metadata);
}
