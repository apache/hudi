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

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BaseHoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;

/**
 * Base class helps to perform bulk insert.
 *
 * @param <T> Type of payload in {@link org.apache.hudi.common.model.HoodieRecord}
 * @param <I> Type of inputs
 * @param <K> Type of keys
 * @param <O> Type of outputs
 * @param <P> Type of record position [Key, Option[partitionPath, fileID]] in hoodie table
 */
public abstract class BaseBulkInsertHelper<T extends HoodieRecordPayload, I, K, O, P> {
  public abstract HoodieWriteMetadata<O> bulkInsert(
      I inputRecords,
      String instantTime,
      BaseHoodieTable<T, I, K, O, P> table,
      HoodieWriteConfig config,
      BaseCommitActionExecutor<T, I, K, O, P> executor,
      boolean performDedupe,
      Option<UserDefinedBulkInsertPartitioner<I>> bulkInsertPartitioner);

}
