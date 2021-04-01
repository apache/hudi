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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FlinkInsertOverwriteCommitActionExecutor<T extends HoodieRecordPayload<T>> extends BaseFlinkCommitActionExecutor<T> {

  private final List<HoodieRecord<T>> inputRecords;

  public FlinkInsertOverwriteCommitActionExecutor(HoodieEngineContext context,
                                                  HoodieWriteHandle<?, ?, ?, ?> writeHandle,
                                                  HoodieWriteConfig config,
                                                  HoodieTable table,
                                                  String instantTime,
                                                  List<HoodieRecord<T>> inputRecords) {
    this(context, writeHandle, config, table, instantTime, inputRecords, WriteOperationType.INSERT_OVERWRITE);
  }

  public FlinkInsertOverwriteCommitActionExecutor(HoodieEngineContext context,
                                                  HoodieWriteHandle<?, ?, ?, ?> writeHandle,
                                                  HoodieWriteConfig config,
                                                  HoodieTable table,
                                                  String instantTime,
                                                  List<HoodieRecord<T>> inputRecords,
                                                  WriteOperationType writeOperationType) {
    super(context, writeHandle, config, table, instantTime, writeOperationType);
    this.inputRecords = inputRecords;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute() {
    return FlinkWriteHelper.newInstance().write(instantTime, inputRecords, context, table,
        config.shouldCombineBeforeInsert(), config.getInsertShuffleParallelism(), this, false);
  }

  @Override
  protected String getCommitActionType() {
    return HoodieTimeline.REPLACE_COMMIT_ACTION;
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(List<WriteStatus> writeStatuses) {
    return writeStatuses.stream().map(status -> status.getStat().getPartitionPath()).distinct()
            .map(partitionPath -> new Tuple2<>(partitionPath, getAllExistingFileIds(partitionPath)))
            .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
  }

  protected List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(FileSlice::getFileId).distinct().collect(Collectors.toList());
  }
}
