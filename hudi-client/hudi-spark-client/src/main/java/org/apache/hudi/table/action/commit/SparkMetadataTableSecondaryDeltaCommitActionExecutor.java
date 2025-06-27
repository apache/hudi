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

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.deltacommit.SparkUpsertPreppedDeltaCommitActionExecutor;

/**
 * Upsert delta commit action executor for metadata table.
 *
 * <p>This action executor is expected to be used during second write to metadata table
 * when streaming writes are enabled to avoid adding inflight commit files twice.
 *
 * @param <T>
 */
public class SparkMetadataTableSecondaryDeltaCommitActionExecutor<T> extends SparkUpsertPreppedDeltaCommitActionExecutor<T> {
  private final boolean initialCall;

  public SparkMetadataTableSecondaryDeltaCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config,
                                                              HoodieTable table, String instantTime,
                                                              HoodieData<HoodieRecord<T>> preppedRecords,
                                                              boolean initialCall) {
    super(context, config, table, instantTime, preppedRecords);
    this.initialCall = initialCall;
  }

  @Override
  protected void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, String instantTime)
      throws HoodieCommitException {
    // with streaming writes support, we might write to metadata table multiple times for the same instant times.
    // ie. writeClient.startCommit(t1), writeClient.upsert(batch1, t1), writeClient.upsert(batch2, t1), writeClient.commit(t1, ...)
    // So, here we are generating inflight file only in the last known writes, which we know will only have FILES partition.
    if (initialCall) {
      super.saveWorkloadProfileMetadataToInflight(profile, instantTime);
    }
  }
}
