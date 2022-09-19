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
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.execution.bulkinsert.BulkInsertInternalPartitionerFactory;
import org.apache.hudi.execution.bulkinsert.BulkInsertMapFunction;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * A spark implementation of {@link BaseBulkInsertHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class SparkBulkInsertHelper<T extends HoodieRecordPayload, R> extends BaseBulkInsertHelper<T, HoodieData<HoodieRecord<T>>,
    HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> {

  private SparkBulkInsertHelper() {
  }

  private static class BulkInsertHelperHolder {
    private static final SparkBulkInsertHelper HOODIE_BULK_INSERT_HELPER = new SparkBulkInsertHelper<>();
  }

  public static SparkBulkInsertHelper newInstance() {
    return BulkInsertHelperHolder.HOODIE_BULK_INSERT_HELPER;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsert(final HoodieData<HoodieRecord<T>> inputRecords,
                                                                 final String instantTime,
                                                                 final HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
                                                                 final HoodieWriteConfig config,
                                                                 final BaseCommitActionExecutor<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>, R> executor,
                                                                 final boolean performDedupe,
                                                                 final Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    //transition bulk_insert state to inflight
    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
            executor.getCommitActionType(), instantTime), Option.empty(),
        config.shouldAllowMultiWriteOnSameInstant());

    BulkInsertPartitioner partitioner = userDefinedBulkInsertPartitioner.orElse(BulkInsertInternalPartitionerFactory.get(table, config));

    // write new files
    HoodieData<WriteStatus> writeStatuses = bulkInsert(inputRecords, instantTime, table, config, performDedupe, partitioner, false,
        config.getBulkInsertShuffleParallelism(), new CreateHandleFactory(false));
    //update index
    ((BaseSparkCommitActionExecutor) executor).updateIndexAndCommitIfNeeded(writeStatuses, result);
    return result;
  }

  /**
   * Do bulk insert using WriteHandleFactory from the partitioner (i.e., partitioner.getWriteHandleFactory)
   */
  public HoodieData<WriteStatus> bulkInsert(HoodieData<HoodieRecord<T>> inputRecords,
                                            String instantTime,
                                            HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
                                            HoodieWriteConfig config,
                                            boolean performDedupe,
                                            BulkInsertPartitioner partitioner,
                                            boolean useWriterSchema,
                                            int parallelism) {
    return bulkInsert(inputRecords, instantTime, table, config, performDedupe, partitioner, useWriterSchema, parallelism, null);
  }

  @Override
  public HoodieData<WriteStatus> bulkInsert(HoodieData<HoodieRecord<T>> inputRecords,
                                            String instantTime,
                                            HoodieTable<T, HoodieData<HoodieRecord<T>>, HoodieData<HoodieKey>, HoodieData<WriteStatus>> table,
                                            HoodieWriteConfig config,
                                            boolean performDedupe,
                                            BulkInsertPartitioner partitioner,
                                            boolean useWriterSchema,
                                            int parallelism,
                                            WriteHandleFactory writeHandleFactory) {

    // De-dupe/merge if needed
    HoodieData<HoodieRecord<T>> dedupedRecords = inputRecords;

    if (performDedupe) {
      dedupedRecords = (HoodieData<HoodieRecord<T>>) HoodieWriteHelper.newInstance().combineOnCondition(config.shouldCombineBeforeInsert(), inputRecords,
          parallelism, table);
    }

    // only JavaRDD is supported for Spark partitioner, but it is not enforced by BulkInsertPartitioner API. To improve this, TODO HUDI-3463
    final HoodieData<HoodieRecord<T>> repartitionedRecords = HoodieJavaRDD.of((JavaRDD<HoodieRecord<T>>) partitioner.repartitionRecords(HoodieJavaRDD.getJavaRDD(dedupedRecords), parallelism));

    JavaRDD<WriteStatus> writeStatusRDD = HoodieJavaRDD.getJavaRDD(repartitionedRecords)
        .mapPartitionsWithIndex(new BulkInsertMapFunction<>(instantTime,
            partitioner.arePartitionRecordsSorted(), config, table, useWriterSchema, partitioner, writeHandleFactory), true)
        .flatMap(List::iterator);

    return HoodieJavaRDD.of(writeStatusRDD);
  }
}
