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
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.JavaLazyInsertIterable;
import org.apache.hudi.execution.bulkinsert.JavaBulkInsertInternalPartitionerFactory;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.FileIdPrefixProvider;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.ArrayList;
import java.util.List;

/**
 * A java implementation of {@link BaseBulkInsertHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class JavaBulkInsertHelper<T, R> extends BaseBulkInsertHelper<T, List<HoodieRecord<T>>,
    List<HoodieKey>, List<WriteStatus>, R> {

  private JavaBulkInsertHelper() {
  }

  private static class BulkInsertHelperHolder {
    private static final JavaBulkInsertHelper JAVA_BULK_INSERT_HELPER = new JavaBulkInsertHelper();
  }

  public static JavaBulkInsertHelper newInstance() {
    return BulkInsertHelperHolder.JAVA_BULK_INSERT_HELPER;
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> bulkInsert(final List<HoodieRecord<T>> inputRecords,
                                                           final String instantTime,
                                                           final HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                                                           final HoodieWriteConfig config,
                                                           final BaseCommitActionExecutor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>, R> executor,
                                                           final boolean performDedupe,
                                                           final Option<BulkInsertPartitioner> userDefinedBulkInsertPartitioner) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    // It's possible the transition to inflight could have already happened.
    if (!table.getActiveTimeline().filterInflights().containsInstant(instantTime)) {
      table.getActiveTimeline().transitionRequestedToInflight(
          new HoodieInstant(HoodieInstant.State.REQUESTED, table.getMetaClient().getCommitActionType(), instantTime),
          Option.empty(),
          config.shouldAllowMultiWriteOnSameInstant());
    }

    BulkInsertPartitioner partitioner = userDefinedBulkInsertPartitioner.orElse(JavaBulkInsertInternalPartitionerFactory.get(config.getBulkInsertSortMode()));

    // write new files
    List<WriteStatus> writeStatuses = bulkInsert(inputRecords, instantTime, table, config, performDedupe, partitioner, false,
        config.getBulkInsertShuffleParallelism(), new CreateHandleFactory(false));
    //update index
    ((BaseJavaCommitActionExecutor) executor).updateIndexAndCommitIfNeeded(writeStatuses, result);
    return result;
  }

  @Override
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> inputRecords,
                                      String instantTime,
                                      HoodieTable<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>> table,
                                      HoodieWriteConfig config,
                                      boolean performDedupe,
                                      BulkInsertPartitioner partitioner,
                                      boolean useWriterSchema,
                                      int parallelism,
                                      WriteHandleFactory writeHandleFactory) {

    // De-dupe/merge if needed
    List<HoodieRecord<T>> dedupedRecords = inputRecords;

    if (performDedupe) {
      dedupedRecords = (List<HoodieRecord<T>>) JavaWriteHelper.newInstance().combineOnCondition(config.shouldCombineBeforeInsert(), inputRecords,
          parallelism, table);
    }

    final List<HoodieRecord<T>> repartitionedRecords = (List<HoodieRecord<T>>) partitioner.repartitionRecords(dedupedRecords, parallelism);

    FileIdPrefixProvider fileIdPrefixProvider = (FileIdPrefixProvider) ReflectionUtils.loadClass(
        config.getFileIdPrefixProviderClassName(),
        new TypedProperties(config.getProps()));

    List<WriteStatus> writeStatuses = new ArrayList<>();

    new JavaLazyInsertIterable<>(repartitionedRecords.iterator(), true,
        config, instantTime, table,
        fileIdPrefixProvider.createFilePrefix(""), table.getTaskContextSupplier(),
        // Always get the first WriteHandleFactory, as there is only a single data partition for hudi java engine.
        (WriteHandleFactory) partitioner.getWriteHandleFactory(0).orElse(writeHandleFactory)).forEachRemaining(writeStatuses::addAll);

    return writeStatuses;
  }
}
