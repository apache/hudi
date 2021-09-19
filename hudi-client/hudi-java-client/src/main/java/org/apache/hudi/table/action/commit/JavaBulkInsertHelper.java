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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.data.HoodieListData;
import org.apache.hudi.execution.JavaLazyInsertIterable;
import org.apache.hudi.execution.bulkinsert.JavaBulkInsertInternalPartitionerFactory;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.FileIdPrefixProvider;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.table.HoodieJavaTable.convertBulkInsertPartitioner;
import static org.apache.hudi.table.action.commit.JavaCommitHelper.getList;

/**
 * A java implementation of {@link BaseBulkInsertHelper}.
 *
 * @param <T>
 */
@SuppressWarnings("checkstyle:LineLength")
public class JavaBulkInsertHelper<T extends HoodieRecordPayload<T>> extends BaseBulkInsertHelper<T> {

  private JavaBulkInsertHelper() {
  }

  private static class BulkInsertHelperHolder {
    private static final JavaBulkInsertHelper JAVA_BULK_INSERT_HELPER = new JavaBulkInsertHelper();
  }

  public static JavaBulkInsertHelper newInstance() {
    return BulkInsertHelperHolder.JAVA_BULK_INSERT_HELPER;
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> bulkInsert(
      final HoodieData<HoodieRecord<T>> inputRecords, final String instantTime, final HoodieTable table,
      final HoodieWriteConfig config, final boolean performDedupe,
      final Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner,
      final BaseCommitHelper<T> commitHelper) {
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();

    // It's possible the transition to inflight could have already happened.
    if (!table.getActiveTimeline().filterInflights().containsInstant(instantTime)) {
      table.getActiveTimeline().transitionRequestedToInflight(
          new HoodieInstant(HoodieInstant.State.REQUESTED, table.getMetaClient().getCommitActionType(), instantTime),
          Option.empty(),
          config.shouldAllowMultiWriteOnSameInstant());
    }

    // write new files
    HoodieData<WriteStatus> writeStatuses = bulkInsert(
        inputRecords, instantTime, table, config, performDedupe, userDefinedBulkInsertPartitioner,
        false, config.getBulkInsertShuffleParallelism(), false);
    //update index
    ((JavaCommitHelper<T>) commitHelper).updateIndexAndCommitIfNeeded(getList(writeStatuses), result);
    return result;
  }

  @Override
  public HoodieData<WriteStatus> bulkInsert(
      HoodieData<HoodieRecord<T>> inputRecords, String instantTime, HoodieTable table,
      HoodieWriteConfig config, boolean performDedupe,
      Option<BulkInsertPartitioner<HoodieData<HoodieRecord<T>>>> userDefinedBulkInsertPartitioner,
      boolean useWriterSchema, int parallelism, boolean preserveHoodieMetadata) {

    // De-dupe/merge if needed
    HoodieData<HoodieRecord<T>> dedupedRecords = inputRecords;

    if (performDedupe) {
      dedupedRecords = (HoodieData<HoodieRecord<T>>) JavaWriteHelper.newInstance().combineOnCondition(
          config.shouldCombineBeforeInsert(), inputRecords, parallelism, table);
    }

    final List<HoodieRecord<T>> repartitionedRecords;
    BulkInsertPartitioner<HoodieData<HoodieRecord<T>>> partitioner =
        userDefinedBulkInsertPartitioner.isPresent()
            ? userDefinedBulkInsertPartitioner.get()
            : convertBulkInsertPartitioner(
            JavaBulkInsertInternalPartitionerFactory.get(config.getBulkInsertSortMode()));
    repartitionedRecords = getList(partitioner.repartitionRecords(dedupedRecords, parallelism));

    FileIdPrefixProvider fileIdPrefixProvider = (FileIdPrefixProvider) ReflectionUtils.loadClass(
        config.getFileIdPrefixProviderClassName(),
        config.getProps());

    List<WriteStatus> writeStatuses = new ArrayList<>();

    new JavaLazyInsertIterable<>(repartitionedRecords.iterator(), true,
        config, instantTime, table,
        fileIdPrefixProvider.createFilePrefix(""), table.getTaskContextSupplier(),
        new CreateHandleFactory<>()).forEachRemaining(writeStatuses::addAll);

    return HoodieListData.of(writeStatuses);
  }
}
