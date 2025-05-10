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

package org.apache.hudi.commit;

import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Executor to be used by stream sync. Directly invokes HoodieDatasetBulkInsertHelper.bulkInsert so that WriteStatus is
 * properly returned. Additionally, we do not want to commit the write in this code because it happens in StreamSync.
 */
public class HoodieStreamerDatasetBulkInsertCommitActionExecutor extends BaseDatasetBulkInsertCommitActionExecutor {

  public HoodieStreamerDatasetBulkInsertCommitActionExecutor(HoodieWriteConfig config, SparkRDDWriteClient writeClient) {
    super(config, writeClient);
  }

  @Override
  protected Option<HoodieData<WriteStatus>> doExecute(Dataset<Row> records, boolean arePartitionRecordsSorted) {
    table.getActiveTimeline().transitionRequestedToInflight(table.getMetaClient().createNewInstant(HoodieInstant.State.REQUESTED,
            getCommitActionType(), instantTime), Option.empty());
    return Option.of(HoodieDatasetBulkInsertHelper
        .bulkInsert(records, instantTime, table, writeConfig, arePartitionRecordsSorted, false, getWriteOperationType()));
  }

  @Override
  public WriteOperationType getWriteOperationType() {
    return WriteOperationType.BULK_INSERT;
  }
}
