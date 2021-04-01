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

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.sink.partitioner.delta.DeltaBucketAssigner;

/**
 * Utilities for {@code BucketAssigner}.
 */
public abstract class BucketAssigners {

  private BucketAssigners() {}

  /**
   * Creates a {@code BucketAssigner}.
   *
   * @param tableType The table type
   * @param context   The engine context
   * @param config    The configuration
   * @return the bucket assigner instance
   */
  public static BucketAssigner create(
      HoodieTableType tableType,
      HoodieFlinkEngineContext context,
      HoodieWriteConfig config) {
    switch (tableType) {
      case COPY_ON_WRITE:
        return new BucketAssigner(context, config);
      case MERGE_ON_READ:
        return new DeltaBucketAssigner(context, config);
      default:
        throw new AssertionError();
    }
  }
}
