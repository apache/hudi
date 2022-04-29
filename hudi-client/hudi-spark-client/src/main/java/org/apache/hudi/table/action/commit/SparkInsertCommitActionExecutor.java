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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

public class SparkInsertCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseSparkCommitActionExecutor<T> {

  private final HoodieData<HoodieRecord<T>> inputRecordsRDD;

  public SparkInsertCommitActionExecutor(HoodieSparkEngineContext context,
                                         HoodieWriteConfig config, HoodieTable table,
                                         String instantTime, HoodieData<HoodieRecord<T>> inputRecordsRDD) {
    super(context, config, table, instantTime, WriteOperationType.INSERT);
    this.inputRecordsRDD = inputRecordsRDD;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    return HoodieWriteHelper.newInstance().write(instantTime, inputRecordsRDD, context, table,
        config.shouldCombineBeforeInsert(), config.getInsertShuffleParallelism(), this, operationType);
  }
}
