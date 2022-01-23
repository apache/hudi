/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hbase.io.hfile;

import org.apache.hudi.hbase.io.HeapSize;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * In-memory BlockCache that may be backed by secondary layer(s).
 */
@InterfaceAudience.Private
public interface FirstLevelBlockCache extends ResizableBlockCache, HeapSize {

  /**
   * Whether the cache contains the block with specified cacheKey
   *
   * @param cacheKey cache key for the block
   * @return true if it contains the block
   */
  boolean containsBlock(BlockCacheKey cacheKey);

  /**
   * Specifies the secondary cache. An entry that is evicted from this cache due to a size
   * constraint will be inserted into the victim cache.
   *
   * @param victimCache the second level cache
   * @throws IllegalArgumentException if the victim cache had already been set
   */
  void setVictimCache(BlockCache victimCache);
}
