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

package org.apache.hudi.table.storage;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;

/**
 * A factory to generate layout.
 */
public final class HoodieLayoutFactory {
  public static HoodieStorageLayout createLayout(HoodieWriteConfig config) {
    switch (config.getLayoutType()) {
      case DEFAULT:
        return new HoodieDefaultLayout(config);
      case BUCKET:
        switch (config.getBucketIndexEngineType()) {
          case RANGE_BUCKET:
          case SIMPLE:
            return new HoodieSimpleBucketLayout(config);
          case CONSISTENT_HASHING:
            return new HoodieConsistentBucketLayout(config);
          default:
            throw new HoodieNotSupportedException("Unknown bucket index engine type: " + config.getBucketIndexEngineType());
        }
      default:
        throw new HoodieNotSupportedException("Unknown layout type, set " + config.getLayoutType());
    }
  }
}
