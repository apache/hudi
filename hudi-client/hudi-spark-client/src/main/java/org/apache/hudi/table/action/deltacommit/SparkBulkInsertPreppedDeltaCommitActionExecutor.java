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

package org.apache.hudi.table.action.deltacommit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;

public class SparkBulkInsertPreppedDeltaCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseSparkDeltaCommitActionExecutor<T> {

  private final HoodieData<HoodieRecord<T>> preppedInputRecordRdd;
  private final Option<BulkInsertPartitioner> bulkInsertPartitioner;

  public SparkBulkInsertPreppedDeltaCommitActionExecutor(HoodieSparkEngineContext context,
                                                         HoodieWriteConfig config, HoodieTable table,
                                                         String instantTime, HoodieData<HoodieRecord<T>> preppedInputRecordRdd,
                                                         Option<BulkInsertPartitioner> bulkInsertPartitioner) {
    super(context, config, table, instantTime, WriteOperationType.BULK_INSERT);
    this.preppedInputRecordRdd = preppedInputRecordRdd;
    this.bulkInsertPartitioner = bulkInsertPartitioner;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    try {
      return SparkBulkInsertHelper.newInstance().bulkInsert(preppedInputRecordRdd, instantTime, table, config,
          this, false, bulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert for commit time " + instantTime, e);
    }
  }

}
