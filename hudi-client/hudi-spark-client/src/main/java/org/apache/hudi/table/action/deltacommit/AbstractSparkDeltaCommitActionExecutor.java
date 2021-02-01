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

import java.util.Map;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.SparkLazyInsertIterable;
import org.apache.hudi.io.AppendHandleFactory;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.commit.BaseSparkCommitActionExecutor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public abstract class AbstractSparkDeltaCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseSparkCommitActionExecutor<T> {
  private static final Logger LOG = LogManager.getLogger(AbstractSparkDeltaCommitActionExecutor.class);

  // UpsertPartitioner for MergeOnRead table type
  private SparkUpsertDeltaCommitPartitioner mergeOnReadUpsertPartitioner;

  public AbstractSparkDeltaCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                String instantTime, WriteOperationType operationType) {
    this(context, config, table, instantTime, operationType, Option.empty());
  }

  public AbstractSparkDeltaCommitActionExecutor(HoodieSparkEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                String instantTime, WriteOperationType operationType,
                                                Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, operationType, extraMetadata);
  }

  @Override
  public Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    mergeOnReadUpsertPartitioner = new SparkUpsertDeltaCommitPartitioner(profile, (HoodieSparkEngineContext) context, table, config);
    return mergeOnReadUpsertPartitioner;
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException {
    LOG.info("Merging updates for commit " + instantTime + " for file " + fileId);

    if (!table.getIndex().canIndexLogFiles() && mergeOnReadUpsertPartitioner.getSmallFileIds().contains(fileId)) {
      LOG.info("Small file corrections for updates for commit " + instantTime + " for file " + fileId);
      return super.handleUpdate(partitionPath, fileId, recordItr);
    } else {
      HoodieAppendHandle<?,?,?,?> appendHandle = new HoodieAppendHandle<>(config, instantTime, table,
          partitionPath, fileId, recordItr, taskContextSupplier);
      appendHandle.doAppend();
      return Collections.singletonList(appendHandle.close()).iterator();
    }
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    // If canIndexLogFiles, write inserts to log files else write inserts to base files
    if (table.getIndex().canIndexLogFiles()) {
      return new SparkLazyInsertIterable<>(recordItr, true, config, instantTime, table,
          idPfx, taskContextSupplier, new AppendHandleFactory<>());
    } else {
      return super.handleInsert(idPfx, recordItr);
    }
  }

}
