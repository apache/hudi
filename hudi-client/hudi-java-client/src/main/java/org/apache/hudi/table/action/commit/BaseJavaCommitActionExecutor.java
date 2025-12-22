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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.JavaLazyInsertIterable;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.io.HoodieMergeHandleFactory;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import lombok.extern.slf4j.Slf4j;

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

@Slf4j
public abstract class BaseJavaCommitActionExecutor<T> extends
    BaseCommitActionExecutor<T, List<HoodieRecord<T>>, List<HoodieKey>, List<WriteStatus>, HoodieWriteMetadata> {

  protected BaseJavaCommitActionExecutor(HoodieEngineContext context,
                                      HoodieWriteConfig config,
                                      HoodieTable table,
                                      String instantTime,
                                      WriteOperationType operationType) {
    super(context, config, table, instantTime, operationType, Option.empty());
  }

  protected BaseJavaCommitActionExecutor(HoodieEngineContext context,
                                      HoodieWriteConfig config,
                                      HoodieTable table,
                                      String instantTime,
                                      WriteOperationType operationType,
                                      Option<Map<String, String>> extraMetadata) {
    super(context, config, table, instantTime, operationType, extraMetadata);
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute(List<HoodieRecord<T>> inputRecords) {
    return execute(inputRecords, Option.empty());
  }

  @Override
  public HoodieWriteMetadata<List<WriteStatus>> execute(List<HoodieRecord<T>> inputRecords, Option<HoodieTimer> sourceReadAndIndexTimer) {
    HoodieWriteMetadata<List<WriteStatus>> result = new HoodieWriteMetadata<>();

    WorkloadProfile workloadProfile =
        new WorkloadProfile(buildProfile(inputRecords), table.getIndex().canIndexLogFiles());
    log.info("Input workload profile :" + workloadProfile);
    final Partitioner partitioner = getPartitioner(workloadProfile);
    try {
      saveWorkloadProfileMetadataToInflight(workloadProfile, instantTime);
    } catch (Exception e) {
      HoodieTableMetaClient metaClient = table.getMetaClient();
      HoodieInstant inflightInstant = instantGenerator.createNewInstant(HoodieInstant.State.INFLIGHT, metaClient.getCommitActionType(), instantTime);
      try {
        if (!metaClient.getStorage().exists(
            new StoragePath(metaClient.getTimelinePath(), instantFileNameGenerator.getFileName(inflightInstant)))) {
          throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", e);
        }
      } catch (IOException ex) {
        log.error("Check file exists failed");
        throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", ex);
      }
    }
    Map<Integer, List<HoodieRecord<T>>> partitionedRecords = partition(inputRecords, partitioner);

    List<WriteStatus> writeStatuses = new LinkedList<>();
    partitionedRecords.forEach((partition, records) -> {
      if (WriteOperationType.isChangingRecords(operationType)) {
        handleUpsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatuses::addAll);
      } else {
        handleInsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatuses::addAll);
      }
    });
    updateIndex(writeStatuses, result);
    updateIndexAndMaybeRunPreCommitValidations(writeStatuses, result);
    return result;
  }

  protected List<WriteStatus> updateIndex(List<WriteStatus> writeStatuses, HoodieWriteMetadata<List<WriteStatus>> result) {
    Instant indexStartTime = Instant.now();
    // Update the index back
    List<WriteStatus> statuses = table.getIndex().updateLocation(HoodieListData.eager(writeStatuses), context, table).collectAsList();
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
    return statuses;
  }

  @Override
  protected String getCommitActionType() {
    return table.getMetaClient().getCommitActionType();
  }

  private Partitioner getPartitioner(WorkloadProfile profile) {
    if (WriteOperationType.isChangingRecords(operationType)) {
      return getUpsertPartitioner(profile);
    } else {
      return getInsertPartitioner(profile);
    }
  }

  private Map<Integer, List<HoodieRecord<T>>> partition(List<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
    Map<Integer, List<Pair<Pair<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>>>> partitionedMidRecords = dedupedRecords
        .stream()
        .map(record -> Pair.of(Pair.of(record.getKey(), Option.ofNullable(record.getCurrentLocation())), record))
        .collect(Collectors.groupingBy(x -> partitioner.getPartition(x.getLeft())));
    Map<Integer, List<HoodieRecord<T>>> results = new LinkedHashMap<>();
    partitionedMidRecords.forEach((key, value) -> results.put(key, value.stream().map(x -> x.getRight()).collect(Collectors.toList())));
    return results;
  }

  protected Pair<Map<String, WorkloadStat>, WorkloadStat> buildProfile(List<HoodieRecord<T>> inputRecords) {
    Map<String, WorkloadStat> partitionPathStatMap = new HashMap<>();
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

  @Override
  protected void commit(HoodieWriteMetadata<List<WriteStatus>> result) {
    commit(result, result.getWriteStatuses().stream().map(WriteStatus::getStat).collect(Collectors.toList()));
  }

  protected void setCommitMetadata(HoodieWriteMetadata<List<WriteStatus>> result) {
    result.setCommitMetadata(Option.of(CommitUtils.buildMetadata(result.getWriteStatuses().stream().map(WriteStatus::getStat).collect(Collectors.toList()),
        result.getPartitionToReplaceFileIds(), extraMetadata, operationType, getSchemaToStoreInCommit(), getCommitActionType())));
  }

  protected Map<String, List<String>> getPartitionToReplacedFileIds(HoodieWriteMetadata<List<WriteStatus>> writeMetadata) {
    return Collections.emptyMap();
  }

  @SuppressWarnings("unchecked")
  protected Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                              Partitioner partitioner) {
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
      log.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  protected Iterator<List<WriteStatus>> handleInsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                              Partitioner partitioner) {
    return handleUpsertPartition(instantTime, partition, recordItr, partitioner);
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
                                                  Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      log.info("Empty partition with fileId => " + fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates
    HoodieMergeHandle<?, ?, ?, ?> mergeHandle = getUpdateHandle(partitionPath, fileId, recordItr);
    return IOUtils.runMerge(mergeHandle, instantTime, fileId);
  }

  protected HoodieMergeHandle<?, ?, ?, ?> getUpdateHandle(String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    Option<BaseKeyGenerator> keyGeneratorOpt = Option.empty();
    if (!config.populateMetaFields()) {
      try {
        keyGeneratorOpt = Option.of((BaseKeyGenerator) HoodieAvroKeyGeneratorFactory.createKeyGenerator(config.getProps()));
      } catch (IOException e) {
        throw new HoodieIOException("Only BaseKeyGenerator (or any key generator that extends from BaseKeyGenerator) are supported when meta "
            + "columns are disabled. Please choose the right key generator if you wish to disable meta fields.", e);
      }
    }
    return HoodieMergeHandleFactory.create(operationType, config, instantTime, table, recordItr, partitionPath, fileId,
        taskContextSupplier, keyGeneratorOpt);
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr) {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      log.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new JavaLazyInsertIterable<>(recordItr, true, config, instantTime, table, idPfx,
        taskContextSupplier, new CreateHandleFactory<>());
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

  public void updateIndexAndMaybeRunPreCommitValidations(List<WriteStatus> writeStatuses, HoodieWriteMetadata result) {
    Instant indexStartTime = Instant.now();
    // Update the index back
    List<WriteStatus> statuses = table.getIndex().updateLocation(HoodieListData.eager(writeStatuses), context, table).collectAsList();
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
    result.setPartitionToReplaceFileIds(getPartitionToReplacedFileIds(result));
    runPrecommitValidators(result);
  }

  @Override
  protected void updateColumnsToIndexForColumnStats(HoodieTableMetaClient metaClient, List<String> columnsToIndex) {
    // no op. HUDI-8801
  }
}
