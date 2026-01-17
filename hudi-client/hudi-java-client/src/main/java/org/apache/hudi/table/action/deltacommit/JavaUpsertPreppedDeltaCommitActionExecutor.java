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

package org.apache.hudi.table.action.deltacommit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.io.HoodieAppendHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.JavaBulkInsertHelper;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class JavaUpsertPreppedDeltaCommitActionExecutor<T> extends BaseJavaDeltaCommitActionExecutor<T> {

  private final List<HoodieRecord<T>> preppedInputRecords;

  public JavaUpsertPreppedDeltaCommitActionExecutor(HoodieJavaEngineContext context, HoodieWriteConfig config, HoodieTable table,
                                                    String instantTime, List<HoodieRecord<T>> preppedInputRecords) {
    super(context, config, table, instantTime, WriteOperationType.UPSERT_PREPPED);
    this.preppedInputRecords = preppedInputRecords;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    HoodieWriteMetadata<List<WriteStatus>> result = new HoodieWriteMetadata<>();
    // First group by target file id.
    HashMap<Pair<String, String>, List<HoodieRecord<T>>> recordsByFileId = new HashMap<>();
    List<HoodieRecord<T>> insertedRecords = new LinkedList<>();

    // Split records into inserts and updates.
    for (HoodieRecord<T> record : preppedInputRecords) {
      if (!record.isCurrentLocationKnown()) {
        insertedRecords.add(record);
      } else {
        Pair<String, String> fileIdPartitionPath = Pair.of(record.getCurrentLocation().getFileId(), record.getPartitionPath());
        if (!recordsByFileId.containsKey(fileIdPartitionPath)) {
          recordsByFileId.put(fileIdPartitionPath, new LinkedList<>());
        }
        recordsByFileId.get(fileIdPartitionPath).add(record);
      }
    }
    log.info(String.format("Total update fileIDs %s, total inserts %s for commit %s",
        recordsByFileId.size(), insertedRecords.size(), instantTime));

    List<WriteStatus> allWriteStatuses = new ArrayList<>();
    try {
      recordsByFileId.forEach((k, v) -> {
        HoodieAppendHandle<?, ?, ?, ?> appendHandle = new HoodieAppendHandle(config, instantTime, table,
            k.getRight(), k.getLeft(), v.iterator(), taskContextSupplier);
        appendHandle.doAppend();
        allWriteStatuses.addAll(appendHandle.close());
      });

      if (insertedRecords.size() > 0) {
        HoodieWriteMetadata<List<WriteStatus>> insertResult = JavaBulkInsertHelper.newInstance()
            .bulkInsert(insertedRecords, instantTime, table, config, this, false, Option.empty());
        allWriteStatuses.addAll(insertResult.getWriteStatuses());
      }
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }

    updateIndexAndMaybeRunPreCommitValidations(allWriteStatuses, result);
    return result;
  }
}
