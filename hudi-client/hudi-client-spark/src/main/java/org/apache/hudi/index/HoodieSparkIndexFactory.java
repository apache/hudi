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

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.bloom.HoodieSparkBloomIndex;
import org.apache.hudi.index.bloom.HoodieSparkGlobalBloomIndex;
import org.apache.hudi.index.hbase.SparkHBaseIndex;
import org.apache.hudi.index.simple.HoodieSparkGlobalSimpleIndex;
import org.apache.hudi.index.simple.HoodieSparkSimpleIndex;

/**
 * A factory to create different type index from config.
 */
public class HoodieSparkIndexFactory  {
  public static HoodieIndex createIndex(HoodieWriteConfig config) {
    // first use index class config to create index.
    if (!StringUtils.isNullOrEmpty(config.getIndexClass())) {
      Object instance = ReflectionUtils.loadClass(config.getIndexClass(), config);
      if (!(instance instanceof HoodieIndex)) {
        throw new HoodieIndexException(config.getIndexClass() + " is not a subclass of HoodieIndex");
      }
      return (HoodieIndex) instance;
    }
    switch (config.getIndexType()) {
      case HBASE:
        return new SparkHBaseIndex<>(config);
      case INMEMORY:
        return new SparkInMemoryHashIndex(config);
      case BLOOM:
        return new HoodieSparkBloomIndex(config);
      case GLOBAL_BLOOM:
        return new HoodieSparkGlobalBloomIndex(config);
      case SIMPLE:
        return new HoodieSparkSimpleIndex(config);
      case GLOBAL_SIMPLE:
        return new HoodieSparkGlobalSimpleIndex(config);
      default:
        throw new HoodieIndexException("Index type unspecified, set " + config.getIndexType());
    }
  }

}
