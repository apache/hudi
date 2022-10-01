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
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.bloom.HoodieBloomIndex;
import org.apache.hudi.index.bloom.ListBasedHoodieBloomIndexHelper;
import org.apache.hudi.index.inmemory.HoodieInMemoryHashIndex;

/**
 * A factory to generate Java {@link HoodieIndex}.
 */
public final class JavaHoodieIndexFactory {

  public static HoodieIndex createIndex(HoodieWriteConfig config) {
    // first use index class config to create index.
    if (!StringUtils.isNullOrEmpty(config.getString(HoodieIndexConfig.INDEX_CLASS_NAME))) {
      Object instance = ReflectionUtils.loadClass(config.getString(HoodieIndexConfig.INDEX_CLASS_NAME), config);
      if (!(instance instanceof HoodieIndex)) {
        throw new HoodieIndexException(config.getString(HoodieIndexConfig.INDEX_CLASS_NAME) + " is not a subclass of HoodieIndex");
      }
      return (HoodieIndex) instance;
    }

    // TODO more indexes to be added
    switch (HoodieIndex.IndexType.valueOf(config.getString(HoodieIndexConfig.INDEX_TYPE))) {
      case INMEMORY:
        return new HoodieInMemoryHashIndex(config);
      case BLOOM:
        return new HoodieBloomIndex(config, ListBasedHoodieBloomIndexHelper.getInstance());
      default:
        throw new HoodieIndexException("Unsupported index type " + HoodieIndex.IndexType.valueOf(config.getString(HoodieIndexConfig.INDEX_TYPE)));
    }
  }
}
