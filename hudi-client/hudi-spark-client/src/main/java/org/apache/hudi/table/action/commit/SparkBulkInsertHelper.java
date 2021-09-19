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

import org.apache.hudi.SparkHoodieRDDData;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.BulkInsertMapFunction;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.table.HoodieSparkTable.convertBulkInsertPartitioner;
import static org.apache.hudi.table.action.commit.SparkCommitHelper.getRdd;

/**
 * A spark implementation of {@link BaseBulkInsertHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class SparkBulkInsertHelper<T extends HoodieRecordPayload<T>> extends BaseBulkInsertHelper<T> {

  private SparkBulkInsertHelper() {
  }

  private static class BulkInsertHelperHolder {
    private static final SparkBulkInsertHelper SPARK_BULK_INSERT_HELPER = new SparkBulkInsertHelper();
  }

  public static SparkBulkInsertHelper newInstance() {
    return BulkInsertHelperHolder.SPARK_BULK_INSERT_HELPER;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsert(
      final HoodieData<HoodieRecord<T>> inputRecords, final String instantTime, final HoodieTable table,
      final HoodieWriteConfig config, final boolean performDedupe,
      final Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner,
      final BaseCommitHelper<T> commitHelper) {
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();

    //transition bulk_insert state to inflight
    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
            table.getMetaClient().getCommitActionType(), instantTime), Option.empty(),
        config.shouldAllowMultiWriteOnSameInstant());
    // write new files
    HoodieData<WriteStatus> writeStatuses =
        bulkInsert(inputRecords, instantTime, table, config, performDedupe,
            userDefinedBulkInsertPartitioner, false, config.getBulkInsertShuffleParallelism(), false);
    //update index
    ((SparkCommitHelper<T>) commitHelper).updateIndexAndCommitIfNeeded(getRdd(writeStatuses), result);
    return result;
  }

  @Override
  public HoodieData<WriteStatus> bulkInsert(
      HoodieData<HoodieRecord<T>> inputRecords, String instantTime, HoodieTable table,
      HoodieWriteConfig config, boolean performDedupe,
      Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner,
      boolean useWriterSchema, int parallelism, boolean preserveMetadata) {

    // De-dupe/merge if needed
    HoodieData<HoodieRecord<T>> dedupedRecords = inputRecords;

    if (performDedupe) {
      dedupedRecords = SparkWriteHelper.newInstance().combineOnCondition(
          config.shouldCombineBeforeInsert(), inputRecords, parallelism, table);
    }

    final JavaRDD<HoodieRecord<T>> repartitionedRecords;
    BulkInsertPartitioner<HoodieData<HoodieRecord<T>>> partitioner =
        userDefinedBulkInsertPartitioner.isPresent()
            ? userDefinedBulkInsertPartitioner.get()
            : convertBulkInsertPartitioner(
            BulkInsertInternalPartitionerFactory.get(config.getBulkInsertSortMode()));
    repartitionedRecords = getRdd(partitioner.repartitionRecords(dedupedRecords, parallelism));

    // generate new file ID prefixes for each output partition
    final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());

    JavaRDD<WriteStatus> writeStatusRDD = repartitionedRecords
        .mapPartitionsWithIndex(new BulkInsertMapFunction<>(
            instantTime, partitioner.arePartitionRecordsSorted(), config, table, fileIDPrefixes,
            useWriterSchema, preserveMetadata), true)
        .flatMap(List::iterator);

    return SparkHoodieRDDData.of(writeStatusRDD);
  }
}
