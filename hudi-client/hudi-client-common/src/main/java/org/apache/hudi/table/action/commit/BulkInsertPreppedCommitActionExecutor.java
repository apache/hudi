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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.Map;

public class BulkInsertPreppedCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseCommitActionExecutor<T, HoodieWriteMetadata> {

  private final BaseCommitHelper<T> commitHelper;
  private final BaseBulkInsertHelper<T> bulkInsertHelper;
  private final HoodieData<HoodieRecord<T>> preppedInputRecords;
  private final Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> bulkInsertPartitioner;

  public BulkInsertPreppedCommitActionExecutor(
      HoodieEngineContext context, HoodieWriteConfig config,
      HoodieTable table,
      String instantTime, HoodieData<HoodieRecord<T>> preppedInputRecords,
      Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> bulkInsertPartitioner,
      BaseCommitHelper<T> commitHelper, BaseBulkInsertHelper<T> bulkInsertHelper) {
    this(context, config, table, instantTime, preppedInputRecords, bulkInsertPartitioner,
        Option.empty(), commitHelper, bulkInsertHelper);
  }

  public BulkInsertPreppedCommitActionExecutor(
      HoodieEngineContext context, HoodieWriteConfig config,
      HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
      String instantTime, HoodieData<HoodieRecord<T>> preppedInputRecords,
      Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> bulkInsertPartitioner,
      Option<Map<String, String>> extraMetadata, BaseCommitHelper<T> commitHelper,
      BaseBulkInsertHelper<T> bulkInsertHelper) {
    super(context, config, table, instantTime, WriteOperationType.BULK_INSERT_PREPPED,
        extraMetadata, commitHelper);
    this.preppedInputRecords = preppedInputRecords;
    this.bulkInsertPartitioner = bulkInsertPartitioner;
    this.commitHelper = commitHelper;
    this.bulkInsertHelper = bulkInsertHelper;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
    try {
      return bulkInsertHelper.bulkInsert(preppedInputRecords, instantTime, table, config,
          false, bulkInsertPartitioner, commitHelper);
    } catch (HoodieInsertException ie) {
      throw ie;
    } catch (Throwable e) {
      throw new HoodieInsertException("Failed to bulk insert for commit time " + instantTime, e);
    }
  }
}
