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

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BaseHoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

/**
 * Base class helps to perform delete keys on hoodie table.
 *
 * @param <T> Type of payload in {@link org.apache.hudi.common.model.HoodieRecord}
 * @param <I> Type of inputs
 * @param <K> Type of keys
 * @param <O> Type of outputs
 * @param <P> Type of record position [Key, Option[partitionPath, fileID]] in hoodie table
 */
public abstract class BaseDeleteHelper<T extends HoodieRecordPayload, I, K, O, P> {


  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param keys RDD of HoodieKey to deduplicate
   * @return RDD of HoodieKey already be deduplicated
   */
  public abstract K deduplicateKeys(K keys, BaseHoodieTable<T, I, K, O, P> table);

  public abstract HoodieWriteMetadata<O> execute(String instantTime,
                                                 K keys,
                                                 HoodieEngineContext context,
                                                 HoodieWriteConfig config,
                                                 BaseHoodieTable<T, I, K, O, P> table,
                                                 BaseCommitActionExecutor<T, I, K, O, P> deleteExecutor);

}
