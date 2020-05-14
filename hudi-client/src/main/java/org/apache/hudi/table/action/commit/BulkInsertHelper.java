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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.BulkInsertMapFunction;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;

import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class BulkInsertHelper<T extends HoodieRecordPayload<T>> {

  public static <T extends HoodieRecordPayload<T>> HoodieWriteMetadata bulkInsert(
      JavaRDD<HoodieRecord<T>> inputRecords, String instantTime,
      HoodieTable<T> table, HoodieWriteConfig config,
      CommitActionExecutor<T> executor, boolean performDedupe,
      Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    // De-dupe/merge if needed
    JavaRDD<HoodieRecord<T>> dedupedRecords = inputRecords;

    if (performDedupe) {
      dedupedRecords = WriteHelper.combineOnCondition(config.shouldCombineBeforeInsert(), inputRecords,
          config.getInsertShuffleParallelism(), ((HoodieTable<T>)table));
    }

    final JavaRDD<HoodieRecord<T>> repartitionedRecords;
    final int parallelism = config.getBulkInsertShuffleParallelism();
    if (bulkInsertPartitioner.isPresent()) {
      repartitionedRecords = bulkInsertPartitioner.get().repartitionRecords(dedupedRecords, parallelism);
    } else {
      // Now, sort the records and line them up nicely for loading.
      repartitionedRecords = dedupedRecords.sortBy(record -> {
        // Let's use "partitionPath + key" as the sort key. Spark, will ensure
        // the records split evenly across RDD partitions, such that small partitions fit
        // into 1 RDD partition, while big ones spread evenly across multiple RDD partitions
        return String.format("%s+%s", record.getPartitionPath(), record.getRecordKey());
      }, true, parallelism);
    }

    // generate new file ID prefixes for each output partition
    final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
        table.getMetaClient().getCommitActionType(), instantTime), Option.empty());

    JavaRDD<WriteStatus> writeStatusRDD = repartitionedRecords
        .mapPartitionsWithIndex(new BulkInsertMapFunction<T>(instantTime, config, table, fileIDPrefixes), true)
        .flatMap(List::iterator);

    executor.updateIndexAndCommitIfNeeded(writeStatusRDD, result);
    return result;
  }
}
