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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieData;
import org.apache.hudi.data.HoodieListData;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.JavaLazyInsertIterable;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieSortedMergeHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JavaCommitHelper<T extends HoodieRecordPayload<T>> extends BaseCommitHelper<T> {

  private static final Logger LOG = LogManager.getLogger(JavaCommitHelper.class);

  public JavaCommitHelper(
      HoodieEngineContext context, HoodieWriteConfig config, HoodieTable table,
      String instantTime, WriteOperationType operationType) {
    this(context, config, table, instantTime, operationType, Option.empty());
  }

  public JavaCommitHelper(
      HoodieEngineContext context, HoodieWriteConfig config, HoodieTable table,
      String instantTime, WriteOperationType operationType,
      Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, operationType, extraMetadata);
  }

  public static <U> List<U> getList(HoodieData<U> hoodieData) {
    return ((HoodieListData<U>) hoodieData).get();
  }

  @Override
  public void init(HoodieWriteConfig config) {
    // No OP
  }

  @Override
  public HoodieWriteMetadata<HoodieData<WriteStatus>> execute(
      HoodieData<HoodieRecord<T>> inputRecords) {
    HoodieWriteMetadata<HoodieData<WriteStatus>> result = new HoodieWriteMetadata<>();

    WorkloadProfile profile = null;
    if (isWorkloadProfileNeeded()) {
      profile = new WorkloadProfile(buildProfile(getList(inputRecords)));
      LOG.info("Workload profile :" + profile);
      try {
        saveWorkloadProfileMetadataToInflight(profile, instantTime);
      } catch (Exception e) {
        HoodieTableMetaClient metaClient = table.getMetaClient();
        HoodieInstant inflightInstant = new HoodieInstant(HoodieInstant.State.INFLIGHT, metaClient.getCommitActionType(), instantTime);
        try {
          if (!metaClient.getFs().exists(new Path(metaClient.getMetaPath(), inflightInstant.getFileName()))) {
            throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", e);
          }
        } catch (IOException ex) {
          LOG.error("Check file exists failed");
          throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", ex);
        }
      }
    }

    final Partitioner partitioner = getPartitioner(profile);
    Map<Integer, List<HoodieRecord<T>>> partitionedRecords = partition(getList(inputRecords), partitioner);

    List<WriteStatus> writeStatuses = new LinkedList<>();
    partitionedRecords.forEach((partition, records) -> {
      if (WriteOperationType.isChangingRecords(operationType)) {
        handleUpsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatuses::addAll);
      } else {
        handleInsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatuses::addAll);
      }
    });
    updateIndex(writeStatuses, result);
    updateIndexAndCommitIfNeeded(writeStatuses, result);
    return result;
  }

  protected void updateIndex(
      List<WriteStatus> writeStatuses, HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    Instant indexStartTime = Instant.now();
    // Update the index back
    HoodieData<WriteStatus> statuses = HoodieListData.of((List<WriteStatus>)
        table.getIndex().updateLocation(writeStatuses, context, table));
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
  }

  private Partitioner getPartitioner(WorkloadProfile profile) {
    if (WriteOperationType.isChangingRecords(operationType)) {
      return getUpsertPartitioner(profile);
    } else {
      return getInsertPartitioner(profile);
    }
  }

  private Map<Integer, List<HoodieRecord<T>>> partition(
      List<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
    Map<Integer, List<Pair<Pair<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>>>> partitionedMidRecords =
        dedupedRecords.stream()
            .map(record -> Pair.of(Pair.of(
                record.getKey(), Option.ofNullable(record.getCurrentLocation())), record))
            .collect(Collectors.groupingBy(x -> partitioner.getPartition(x.getLeft())));
    Map<Integer, List<HoodieRecord<T>>> results = new LinkedHashMap<>();
    partitionedMidRecords.forEach((key, value) -> results.put(key, value.stream().map(x -> x.getRight()).collect(Collectors.toList())));
    return results;
  }

  protected Pair<HashMap<String, WorkloadStat>, WorkloadStat> buildProfile(
      List<HoodieRecord<T>> inputRecords) {
    HashMap<String, WorkloadStat> partitionPathStatMap = new HashMap<>();
    WorkloadStat globalStat = new WorkloadStat();

    Map<Pair<String, Option<HoodieRecordLocation>>, Long> partitionLocationCounts = inputRecords
        .stream()
        .map(record -> Pair.of(
            Pair.of(record.getPartitionPath(), Option.ofNullable(record.getCurrentLocation())), record))
        .collect(Collectors.groupingBy(Pair::getLeft, Collectors.counting()));

    for (Map.Entry<Pair<String, Option<HoodieRecordLocation>>, Long> e : partitionLocationCounts.entrySet()) {
      String partitionPath = e.getKey().getLeft();
      Long count = e.getValue();
      Option<HoodieRecordLocation> locOption = e.getKey().getRight();

      if (!partitionPathStatMap.containsKey(partitionPath)) {
        partitionPathStatMap.put(partitionPath, new WorkloadStat());
      }

      if (locOption.isPresent()) {
        // update
        partitionPathStatMap.get(partitionPath).addUpdates(locOption.get(), count);
        globalStat.addUpdates(locOption.get(), count);
      } else {
        // insert
        partitionPathStatMap.get(partitionPath).addInserts(count);
        globalStat.addInserts(count);
      }
    }
    return Pair.of(partitionPathStatMap, globalStat);
  }

  protected Map<String, List<String>> getPartitionToReplacedFileIds(
      HoodieWriteMetadata<HoodieData<WriteStatus>> writeMetadata) {
    return Collections.emptyMap();
  }

  protected Iterator<List<WriteStatus>> handleUpsertPartition(
      String instantTime, Integer partition, Iterator recordItr, Partitioner partitioner) {
    JavaUpsertPartitioner javaUpsertPartitioner = (JavaUpsertPartitioner) partitioner;
    BucketInfo binfo = javaUpsertPartitioner.getBucketInfo(partition);
    BucketType btype = binfo.bucketType;
    try {
      if (btype.equals(BucketType.INSERT)) {
        return handleInsert(binfo.fileIdPrefix, recordItr);
      } else if (btype.equals(BucketType.UPDATE)) {
        return handleUpdate(binfo.partitionPath, binfo.fileIdPrefix, recordItr);
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partition);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + btype + " for partition :" + partition;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  protected Iterator<List<WriteStatus>> handleInsertPartition(
      String instantTime, Integer partition, Iterator recordItr, Partitioner partitioner) {
    return handleUpsertPartition(instantTime, partition, recordItr, partitioner);
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(
      String idPfx, Iterator<HoodieRecord<T>> recordItr) {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new JavaLazyInsertIterable<>(recordItr, true, config, instantTime, table, idPfx,
        context.getTaskContextSupplier(), new CreateHandleFactory<>());
  }

  protected HoodieMergeHandle getUpdateHandle(
      String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    if (table.requireSortedRecords()) {
      return new HoodieSortedMergeHandle<>(config, instantTime, table, recordItr, partitionPath, fileId, context.getTaskContextSupplier(), Option.empty());
    } else {
      return new HoodieMergeHandle<>(config, instantTime, table, recordItr, partitionPath, fileId, context.getTaskContextSupplier(), Option.empty());
    }
  }

  @Override
  protected BaseMergeHelper getMergeHelper() {
    return JavaMergeHelper.newInstance();
  }

  protected HoodieMergeHandle getUpdateHandle(
      String partitionPath, String fileId, Map<String, HoodieRecord<T>> keyToNewRecords,
      HoodieBaseFile dataFileToBeMerged) {
    return new HoodieMergeHandle<>(config, instantTime, table, keyToNewRecords,
        partitionPath, fileId, dataFileToBeMerged, context.getTaskContextSupplier(), Option.empty());
  }

  /**
   * Provides a partitioner to perform the upsert operation, based on the workload profile.
   */
  public Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    return new JavaUpsertPartitioner(profile, context, table, config);
  }

  /**
   * Provides a partitioner to perform the insert operation, based on the workload profile.
   */
  public Partitioner getInsertPartitioner(WorkloadProfile profile) {
    return getUpsertPartitioner(profile);
  }

  public void updateIndexAndCommitIfNeeded(List<WriteStatus> writeStatuses, HoodieWriteMetadata<HoodieData<WriteStatus>> result) {
    Instant indexStartTime = Instant.now();
    // Update the index back
    HoodieData<WriteStatus> statuses = HoodieListData.of((List<WriteStatus>)
        table.getIndex().updateLocation(writeStatuses, context, table));
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
    result.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(result));
    commitOnAutoCommit(result);
  }

  @Override
  protected void syncTableMetadata() {

  }
}
