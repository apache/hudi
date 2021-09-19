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

package org.apache.hudi.table;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieListData;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BulkInsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.commit.JavaBulkInsertHelper;
import org.apache.hudi.table.action.commit.JavaCommitHelper;
import org.apache.hudi.table.action.commit.UpsertPreppedCommitActionExecutor;
import org.apache.hudi.table.action.deltacommit.JavaUpsertPreppedDeltaCommitHelper;

import java.util.List;

public class HoodieJavaMergeOnReadTable<T extends HoodieRecordPayload<T>> extends HoodieJavaCopyOnWriteTable<T> {
  protected HoodieJavaMergeOnReadTable(HoodieWriteConfig config, HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    super(config, context, metaClient);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> upsertPrepped(HoodieEngineContext context,
                                                              String instantTime,
                                                              List<HoodieRecord<T>> preppedRecords) {
    return convertMetadata(new UpsertPreppedCommitActionExecutor<>(
        context, config, this, instantTime, HoodieListData.of(preppedRecords),
        new JavaUpsertPreppedDeltaCommitHelper<>(context, config, this, instantTime)).execute());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsertPrepped(HoodieEngineContext context,
                                                                  String instantTime,
                                                                  List<HoodieRecord<T>> preppedRecords,
                                                                  Option<BulkInsertPartitioner<List<HoodieRecord<T>>>> bulkInsertPartitioner) {
    return convertMetadata(new BulkInsertPreppedCommitActionExecutor<T>(
        context, config, this, instantTime, HoodieListData.of(preppedRecords),
        convertBulkInsertPartitioner(bulkInsertPartitioner),
        new JavaCommitHelper<>(context, config, this, instantTime, WriteOperationType.BULK_INSERT_PREPPED),
        JavaBulkInsertHelper.newInstance()).execute());
  }
}
