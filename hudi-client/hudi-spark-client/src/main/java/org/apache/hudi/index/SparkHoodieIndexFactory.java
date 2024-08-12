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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.HoodieGlobalBloomIndex;
import org.apache.hudi.index.bloom.SparkHoodieBloomIndexHelper;
import org.apache.hudi.index.bucket.HoodieSimpleBucketIndex;
import org.apache.hudi.index.bucket.HoodieSparkConsistentBucketIndex;
import org.apache.hudi.index.hbase.SparkHoodieHBaseIndex;
import org.apache.hudi.index.inmemory.HoodieInMemoryHashIndex;
import org.apache.hudi.index.simple.HoodieGlobalSimpleIndex;
import org.apache.hudi.index.simple.HoodieSimpleIndex;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;

/**
 * A factory to generate Spark {@link HoodieIndex}.
 */
public final class SparkHoodieIndexFactory {
  public static HoodieIndex createIndex(HoodieWriteConfig config) {
    boolean sqlMergeIntoPrepped = config.getProps().getBoolean(HoodieWriteConfig.SPARK_SQL_MERGE_INTO_PREPPED_KEY, false);
    if (sqlMergeIntoPrepped) {
      return new HoodieInternalProxyIndex(config);
    }
    // first use index class config to create index.
    if (!StringUtils.isNullOrEmpty(config.getIndexClass())) {
      return HoodieIndexUtils.createUserDefinedIndex(config);
    }

    switch (config.getIndexType()) {
      case HBASE:
        return new SparkHoodieHBaseIndex(config);
      case INMEMORY:
        return new HoodieInMemoryHashIndex(config);
      case BLOOM:
        return new HoodieBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
      case GLOBAL_BLOOM:
        return new HoodieGlobalBloomIndex(config, SparkHoodieBloomIndexHelper.getInstance());
      case SIMPLE:
        return new HoodieSimpleIndex(config, HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(config));
      case GLOBAL_SIMPLE:
        return new HoodieGlobalSimpleIndex(config, HoodieSparkKeyGeneratorFactory.createBaseKeyGenerator(config));
      case BUCKET:
        switch (config.getBucketIndexEngineType()) {
          case SIMPLE:
            return new HoodieSimpleBucketIndex(config);
          case CONSISTENT_HASHING:
            return new HoodieSparkConsistentBucketIndex(config);
          default:
            throw new HoodieIndexException("Unknown bucket index engine type: " + config.getBucketIndexEngineType());
        }
      case RECORD_INDEX:
        return new SparkMetadataTableRecordIndex(config);
      default:
        throw new HoodieIndexException("Index type unspecified, set " + config.getIndexType());
    }
  }

  /**
   * Whether index is global or not.
   * @param config HoodieWriteConfig to use.
   * @return {@code true} if index is a global one. else {@code false}.
   */
  public static boolean isGlobalIndex(HoodieWriteConfig config) {
    switch (config.getIndexType()) {
      case HBASE:
        return true;
      case INMEMORY:
        return true;
      case BLOOM:
        return false;
      case GLOBAL_BLOOM:
        return true;
      case SIMPLE:
        return false;
      case GLOBAL_SIMPLE:
        return true;
      case BUCKET:
        return false;
      case RECORD_INDEX:
        return true;
      default:
        return createIndex(config).isGlobal();
    }
  }
}
