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

package org.apache.hudi.hbase;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Enum describing all possible memory compaction policies
 */
@InterfaceAudience.Public
public enum MemoryCompactionPolicy {
  /**
   * No memory compaction, when size threshold is exceeded data is flushed to disk
   */
  NONE,
  /**
   * Basic policy applies optimizations which modify the index to a more compacted representation.
   * This is beneficial in all access patterns. The smaller the cells are the greater the
   * benefit of this policy.
   * This is the default policy.
   */
  BASIC,
  /**
   * In addition to compacting the index representation as the basic policy, eager policy
   * eliminates duplication while the data is still in memory (much like the
   * on-disk compaction does after the data is flushed to disk). This policy is most useful for
   * applications with high data churn or small working sets.
   */
  EAGER,
  /**
   * Adaptive compaction adapts to the workload. It applies either index compaction or data
   * compaction based on the ratio of duplicate cells in the data.
   */
  ADAPTIVE

}
