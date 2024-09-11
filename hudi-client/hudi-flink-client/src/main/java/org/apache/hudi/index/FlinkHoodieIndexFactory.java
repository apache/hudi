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

package org.apache.hudi.index;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.HoodieGlobalBloomIndex;
import org.apache.hudi.index.bloom.ListBasedHoodieBloomIndexHelper;
import org.apache.hudi.index.bucket.HoodieConsistentBucketIndex;
import org.apache.hudi.index.bucket.HoodieSimpleBucketIndex;
import org.apache.hudi.index.simple.HoodieGlobalSimpleIndex;
import org.apache.hudi.index.simple.HoodieSimpleIndex;
import org.apache.hudi.index.state.FlinkInMemoryStateIndex;

/**
 * A factory to generate Flink {@link HoodieIndex}.
 */
public final class FlinkHoodieIndexFactory {
  public static HoodieIndex createIndex(HoodieFlinkEngineContext context, HoodieWriteConfig config) {
    // first use index class config to create index.
    if (!StringUtils.isNullOrEmpty(config.getIndexClass())) {
      return HoodieIndexUtils.createUserDefinedIndex(config);
    }

    switch (config.getIndexType()) {
      case FLINK_STATE:
        // Flink state index stores the index mappings with a state-backend,
        // instantiates an in-memory HoodieIndex component as a placeholder.
      case INMEMORY:
        return new FlinkInMemoryStateIndex(context, config);
      case BLOOM:
        return new HoodieBloomIndex(config, ListBasedHoodieBloomIndexHelper.getInstance());
      case GLOBAL_BLOOM:
        return new HoodieGlobalBloomIndex(config, ListBasedHoodieBloomIndexHelper.getInstance());
      case SIMPLE:
        return new HoodieSimpleIndex(config, Option.empty());
      case GLOBAL_SIMPLE:
        return new HoodieGlobalSimpleIndex(config, Option.empty());
      case BUCKET:
        switch (config.getBucketIndexEngineType()) {
          case SIMPLE:
            return new HoodieSimpleBucketIndex(config);
          case CONSISTENT_HASHING:
            return new HoodieConsistentBucketIndex(config);
          default:
            throw new HoodieIndexException("Unknown bucket index engine type: " + config.getBucketIndexEngineType());
        }
      default:
        throw new HoodieIndexException("Unsupported index type " + config.getIndexType());
    }
  }
}
