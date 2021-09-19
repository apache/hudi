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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class BaseCommitActionExecutor<T extends HoodieRecordPayload<T>, R>
    extends BaseActionExecutor<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> {

  protected final BaseCommitHelper<T> commitHelper;
  protected final Option<Map<String, String>> extraMetadata;
  protected final WriteOperationType operationType;
  protected final TaskContextSupplier taskContextSupplier;

  public BaseCommitActionExecutor(
      HoodieEngineContext context, HoodieWriteConfig config,
      HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
      String instantTime, WriteOperationType operationType,
      BaseCommitHelper<T> commitHelper) {
    this(context, config, table, instantTime, operationType, Option.empty(), commitHelper);
  }

  public BaseCommitActionExecutor(
      HoodieEngineContext context, HoodieWriteConfig config,
      HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
      String instantTime, WriteOperationType operationType,
      Option<Map<String, String>> extraMetadata,
      BaseCommitHelper<T> commitHelper) {
    super(context, config, table, instantTime);
    this.commitHelper = commitHelper;
    this.operationType = operationType;
    this.extraMetadata = extraMetadata;
    this.taskContextSupplier = context.getTaskContextSupplier();
    commitHelper.init(config);
  }

  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(
      HoodieData<HoodieRecord<T>> inputRecords) {
    return commitHelper.execute(inputRecords);
  }

  public Configuration getHadoopConf() {
    return hadoopConf;
  }

  protected String getCommitActionType() {
    return table.getMetaClient().getCommitActionType();
  }

  protected void commit(
      Option<Map<String, String>> extraMetadata,
      HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    commitHelper.commit(extraMetadata, result);
  }

  protected Iterator<List<WriteStatus>> handleInsert(
      String idPfx, Iterator<HoodieRecord<T>> recordItr) throws Exception {
    return commitHelper.handleInsert(idPfx, recordItr);
  }

  protected Iterator<List<WriteStatus>> handleUpdate(
      String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException {
    return commitHelper.handleUpdate(partitionPath, fileId, recordItr);
  }
}
