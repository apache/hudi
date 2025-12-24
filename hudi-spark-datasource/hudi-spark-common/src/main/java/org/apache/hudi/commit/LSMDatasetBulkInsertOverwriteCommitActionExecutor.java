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
import org.apache.hudi.client.HoodieTimelineSkipInstanceClient;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LSMDatasetBulkInsertOverwriteCommitActionExecutor extends LSMDatasetBulkInsertCommitActionExecutor {

  public LSMDatasetBulkInsertOverwriteCommitActionExecutor(HoodieWriteConfig config,
                                                           SparkRDDWriteClient writeClient,
                                                           String instantTime) {
    super(config, writeClient, instantTime);
  }

  @Override
  protected void preExecute() {
    HoodieTimelineSkipInstanceClient.storeBacktrackInstantToTmpForInsertOverwrite(
        table.getMetaClient().getFs(), table.getMetaClient().getBacktrackingPath(), instantTime);

    table.validateInsertSchema();
    writeClient.startCommitWithTime(instantTime, getCommitActionType());
    writeClient.preWrite(instantTime, getWriteOperationType(), table.getMetaClient());
  }

  @Override
  protected Option<HoodieData<WriteStatus>> doExecute(Dataset<Row> records, boolean arePartitionRecordsSorted) {
    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED, getCommitActionType(), instantTime), Option.empty());
    return Option.of(HoodieDatasetBulkInsertHelper
        .bulkInsert(records, instantTime, table, writeConfig, arePartitionRecordsSorted, false, getWriteOperationType()));
  }

  @Override
  protected void afterExecute(HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {
    writeClient.postWrite(result, instantTime, table);
    HoodieTimelineSkipInstanceClient.deleteBacktrackInstantFromTmpForInsertOverwrite(table.getMetaClient().getFs(), table.getMetaClient().getBacktrackingTmpPath(), instantTime);
  }

  @Override
  public WriteOperationType getWriteOperationType() {
    return WriteOperationType.INSERT_OVERWRITE;
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieData<WriteStatus> writeStatuses) {
    if (!writeConfig.getReplacePartitionRecords()) {
      return Collections.emptyMap();
    }

    Map<String, List<String>> partitionAndReplaceFiledIds;
    if (canTakeFirstPartition(writeConfig, writeStatuses)) {
      String partition = writeStatuses.first().getStat().getPartitionPath();
      partitionAndReplaceFiledIds = Collections.singletonMap(partition, getAllExistingFileIds(partition));
    } else {
      partitionAndReplaceFiledIds = HoodieJavaPairRDD.getJavaPairRDD(writeStatuses.map(status -> status.getStat().getPartitionPath()).distinct().mapToPair(partitionPath ->
          Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
    }

    // TODO 是否需要兼容backtrack逻辑
    Boolean needCreateBacktrackingInstance = true;
    for (Map.Entry<String, List<String>> partitionLevelFileIds : partitionAndReplaceFiledIds.entrySet()) {
      if (partitionLevelFileIds.getValue() != null && partitionLevelFileIds.getValue().size() > 0) {
        needCreateBacktrackingInstance = false;
        break;
      }
    }
    if (needCreateBacktrackingInstance) {
      HoodieTimelineSkipInstanceClient.storeBacktrackInstanceForInsertOverwrite(table.getMetaClient().getFs(), table.getMetaClient().getBacktrackingPath(), instantTime);
    }
    return partitionAndReplaceFiledIds;
  }

  protected List<String> getAllExistingFileIds(String partitionPath) {
    return table.getSliceView().getLatestFileSlices(partitionPath)
        .flatMap(fileSlice -> fileSlice.getLogFiles().map(HoodieLogFile::getFileName)).distinct().collect(Collectors.toList());
  }
}
