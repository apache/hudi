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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableFunctionUnchecked;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

/**
 * Helper class to perform delete keys on hoodie table.
 *
 * @param <T>
 */
public abstract class BaseDeleteHelper<T, I, K, O, R> extends ParallelismHelper<I> {

  protected BaseDeleteHelper(SerializableFunctionUnchecked<I, Integer> partitionNumberExtractor) {
    super(partitionNumberExtractor);
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param keys HoodieKeys to deduplicate
   * @param table target Hoodie table for deduplicating
   * @param parallelism parallelism or partitions to be used while reducing/deduplicating
   * @return HoodieKey already be deduplicated
   */
  public abstract K deduplicateKeys(K keys, HoodieTable<T, I, K, O> table, int parallelism);

  public abstract HoodieWriteMetadata<O> execute(String instantTime,
                                                 K keys, HoodieEngineContext context,
                                                 HoodieWriteConfig config, HoodieTable<T, I, K, O> table,
                                                 BaseCommitActionExecutor<T, I, K, O, R> deleteExecutor);
}
