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
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieInternalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DatasetBulkInsertOverwriteCommitActionExecutor extends BaseDatasetBulkInsertCommitActionExecutor {

  public DatasetBulkInsertOverwriteCommitActionExecutor(HoodieWriteConfig config,
                                                        SparkRDDWriteClient writeClient, String instantTime) {
    super(config, writeClient, instantTime);
  }

  @Override
  protected Option<HoodieData<WriteStatus>> doExecute(Dataset<Row> records, boolean arePartitionRecordsSorted) {
    table.getActiveTimeline().transitionRequestedToInflight(table.getMetaClient().createNewInstant(HoodieInstant.State.REQUESTED,
        getCommitActionType(), instantTime), Option.empty());
    return Option.of(HoodieDatasetBulkInsertHelper
        .bulkInsert(records, instantTime, table, writeConfig, arePartitionRecordsSorted, false));
  }

  @Override
  public WriteOperationType getWriteOperationType() {
    return WriteOperationType.INSERT_OVERWRITE;
  }

  @Override
  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieData<WriteStatus> writeStatuses) {
    String staticOverwritePartition = writeConfig.getStringOrDefault(HoodieInternalConfig.STATIC_OVERWRITE_PARTITION_PATHS);
    if (StringUtils.nonEmpty(staticOverwritePartition)) {
      // static insert overwrite partitions
      List<String> partitionPaths = Arrays.asList(staticOverwritePartition.split(","));
      table.getContext().setJobStatus(this.getClass().getSimpleName(), "Getting ExistingFileIds of matching static partitions");
      return HoodieJavaPairRDD.getJavaPairRDD(table.getContext().parallelize(partitionPaths, partitionPaths.size()).mapToPair(
          partitionPath -> Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
    } else {
      // dynamic insert overwrite partitions
      return HoodieJavaPairRDD.getJavaPairRDD(writeStatuses.map(status -> status.getStat().getPartitionPath()).distinct().mapToPair(partitionPath ->
          Pair.of(partitionPath, getAllExistingFileIds(partitionPath)))).collectAsMap();
    }
  }

  protected List<String> getAllExistingFileIds(String partitionPath) {
    // because new commit is not complete. it is safe to mark all existing file Ids as old files
    return table.getSliceView().getLatestFileSlices(partitionPath).map(FileSlice::getFileId).distinct().collect(Collectors.toList());
  }
}
