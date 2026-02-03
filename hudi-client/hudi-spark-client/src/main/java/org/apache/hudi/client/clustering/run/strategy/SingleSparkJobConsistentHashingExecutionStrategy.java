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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.LazyConcatenatingIterator;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ClusteringGroupInfo;
import org.apache.hudi.common.model.ClusteringOperation;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.IOUtils;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.BaseConsistentHashingBucketClusteringPlanStrategy;
import org.apache.hudi.util.ExecutorFactory;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Execution strategy for table with {@link org.apache.hudi.index.HoodieIndex.BucketIndexEngineType#CONSISTENT_HASHING} to perform bucket resizing.
 * This strategy will split a clustering plan into multiple operations and perform the operations in a single spark job, each operation will be a single task.
 * <p>
 * There are two types of operations:
 * <li> <b>Split</b>: Split a bucket into multiple buckets
 * <li> <b>Merge</b>: Merge multiple buckets into a single bucket
 */
@Slf4j
public class SingleSparkJobConsistentHashingExecutionStrategy<T> extends SingleSparkJobExecutionStrategy<T> {

  private final String indexKeyFields;
  private final HoodieSchema readerSchema;

  public SingleSparkJobConsistentHashingExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext,
                                                          HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
    this.indexKeyFields = table.getConfig().getBucketIndexHashField();
    this.readerSchema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(writeConfig.getSchema()));
  }

  @Override
  protected List<WriteStatus> performClusteringForGroup(ReaderContextFactory<T> readerContextFactory, ClusteringGroupInfo clusteringGroup, Map<String, String> strategyParams,
                                                        boolean preserveHoodieMetadata, HoodieSchema schema, TaskContextSupplier taskContextSupplier, String instantTime) {
    // deal with split / merge operations
    ValidationUtils.checkArgument(clusteringGroup.getNumOutputGroups() >= 1, "Number of output groups should be at least 1");
    if (clusteringGroup.getNumOutputGroups() == 1) {
      // means that there is a merge operation
      return performBucketMergeForGroup(readerContextFactory, clusteringGroup, strategyParams, taskContextSupplier, instantTime);
    }
    // more than one output groups means split operation
    return performBucketSplitForGroup(readerContextFactory, clusteringGroup, strategyParams, taskContextSupplier, instantTime);
  }

  private List<ConsistentHashingNode> decodeConsistentHashingNodes(ClusteringGroupInfo clusteringGroupInfo) {
    Option<Map<String, String>> extraMetadata = clusteringGroupInfo.getExtraMetadata();
    ValidationUtils.checkArgument(extraMetadata.isPresent(), "Extra metadata should be present for consistent hashing operations");
    String json = extraMetadata.get().get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY);
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(json), "Child nodes should not be null or empty for consistent hashing operations");
    try {
      return ConsistentHashingNode.fromJsonString(json);
    } catch (Exception e) {
      throw new HoodieClusteringException("Failed to parse child nodes from metadata", e);
    }
  }

  private List<WriteStatus> performBucketMergeForGroup(ReaderContextFactory<T> readerContextFactory, ClusteringGroupInfo clusteringGroup, Map<String, String> strategyParams,
                                                       TaskContextSupplier taskContextSupplier, String instantTime) {
    long maxMemoryPerCompaction = IOUtils.getMaxMemoryPerCompaction(taskContextSupplier, writeConfig);
    log.info("MaxMemoryPerCompaction run as part of clustering => {}", maxMemoryPerCompaction);
    Option<Map<String, String>> extraMetadata = clusteringGroup.getExtraMetadata();
    ValidationUtils.checkArgument(extraMetadata.isPresent(), "Extra metadata should be present for consistent hashing operations");
    String partition = extraMetadata.get().get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_PARTITION_KEY);
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(partition), "Partition should not be null or empty");
    List<ConsistentHashingNode> nodes = decodeConsistentHashingNodes(clusteringGroup);
    Option<ConsistentHashingNode> newBucket = Option.fromJavaOptional(nodes.stream().filter(node -> node.getTag() == ConsistentHashingNode.NodeTag.REPLACE).findFirst());
    ValidationUtils.checkArgument(newBucket.isPresent(), "New bucket should be present for merge operation");
    ConsistentHashingNode newBucketNode = newBucket.get();
    List<Supplier<ClosableIterator<HoodieRecord<T>>>> readerSuppliers = new ArrayList<>(clusteringGroup.getOperations().size());
    clusteringGroup.getOperations().stream().forEach(op -> {
      Supplier<ClosableIterator<HoodieRecord<T>>> supplier = () -> getRecordIterator(readerContextFactory, op, instantTime, maxMemoryPerCompaction);
      readerSuppliers.add(supplier);
    });
    LazyConcatenatingIterator<HoodieRecord<T>> inputRecordsIter = new LazyConcatenatingIterator<>(readerSuppliers);

    HoodieConsumer<HoodieRecord<T>, List<WriteStatus>> insertHandler =
        new InsertHandler(writeConfig, instantTime, getHoodieTable(), taskContextSupplier, new FixedIdSuffixCreateHandleFactory(), true, record -> newBucketNode.getFileIdPrefix(), readerSchema);
    return ExecutorFactory.create(writeConfig, inputRecordsIter, insertHandler, op -> op, getHoodieTable().getPreExecuteRunnable()).execute();
  }

  static class FixedIdSuffixCreateHandleFactory extends CreateHandleFactory {
    @Override
    protected String getNextFileId(String idPfx) {
      return FSUtils.createNewFileId(idPfx, 0);
    }
  }

  static class InsertHandler<T> implements HoodieConsumer<HoodieRecord<T>, List<WriteStatus>> {

    private final HoodieWriteConfig config;
    private final String instantTime;
    private final HoodieTable hoodieTable;
    private final TaskContextSupplier taskContextSupplier;
    private final WriteHandleFactory writeHandleFactory;
    private final List<WriteStatus> statuses;
    private final boolean recordsSorted;
    private final Map<String/*fileIdPrefix*/, HoodieWriteHandle> writeHandles;
    private final Function<HoodieRecord, String> fileIdPrefixExtractor;
    private final HoodieSchema schema;

    public InsertHandler(HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable, TaskContextSupplier taskContextSupplier,
                         WriteHandleFactory writeHandleFactory, boolean recordsSorted, Function<HoodieRecord, String> fileIdPrefixExtractor, HoodieSchema schema) {
      this.config = config;
      this.instantTime = instantTime;
      this.hoodieTable = hoodieTable;
      this.taskContextSupplier = taskContextSupplier;
      this.writeHandleFactory = writeHandleFactory;
      this.statuses = new ArrayList<>();
      this.recordsSorted = recordsSorted;
      this.writeHandles = new HashMap<>();
      this.fileIdPrefixExtractor = fileIdPrefixExtractor;
      this.schema = schema;
    }

    @Override
    public void consume(HoodieRecord record) throws Exception {
      String fileIdPrefix = fileIdPrefixExtractor.apply(record);
      HoodieWriteHandle handle = writeHandles.get(fileIdPrefix);
      if (handle == null) {
        if (recordsSorted) {
          // records sorted, so we can close the previous handles
          closeOpenHandles();
        }
        handle = writeHandleFactory.create(config, instantTime, hoodieTable, record.getPartitionPath(), fileIdPrefix, taskContextSupplier);
        writeHandles.put(fileIdPrefix, handle);
      }
      handle.write(record, schema, config.getProps());
    }

    @Override
    public List<WriteStatus> finish() {
      closeOpenHandles();
      return statuses;
    }

    private void closeOpenHandles() {
      for (HoodieWriteHandle<?, ?, ?, ?> handle : writeHandles.values()) {
        statuses.addAll(handle.close());
      }
      writeHandles.clear();
    }
  }

  private List<WriteStatus> performBucketSplitForGroup(ReaderContextFactory<T> readerContextFactory, ClusteringGroupInfo clusteringGroup, Map<String, String> strategyParams,
                                                       TaskContextSupplier taskContextSupplier, String instantTime) {
    ValidationUtils.checkArgument(clusteringGroup.getOperations().size() == 1, "Split operation should have only one operation");
    Option<Map<String, String>> extraMetadata = clusteringGroup.getExtraMetadata();
    ValidationUtils.checkArgument(extraMetadata.isPresent(), "Extra metadata should be present for consistent hashing operations");
    String partition = extraMetadata.get().get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_PARTITION_KEY);
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(partition), "Partition should not be null or empty");
    List<ConsistentHashingNode> nodes = decodeConsistentHashingNodes(clusteringGroup);
    Integer seqNo = Integer.parseInt(extraMetadata.get().get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_SEQUENCE_NUMBER_KEY));
    HoodieConsistentHashingMetadata metadata = new HoodieConsistentHashingMetadata((short) 0, partition, instantTime, 0, seqNo + 1, Collections.emptyList());
    metadata.setChildrenNodes(nodes);
    ConsistentBucketIdentifier identifier = new ConsistentBucketIdentifier(metadata);
    ClusteringOperation operation = clusteringGroup.getOperations().get(0);
    ClosableIterator<HoodieRecord<T>> iterator = getRecordIterator(readerContextFactory, operation, instantTime, IOUtils.getMaxMemoryPerCompaction(new SparkTaskContextSupplier(), writeConfig));
    Function<HoodieRecord<T>, String> fileIdPrefixExtractor = record -> identifier.getBucket(record.getRecordKey(), this.indexKeyFields).getFileIdPrefix();

    HoodieConsumer<HoodieRecord<T>, List<WriteStatus>> insertHandler =
        new InsertHandler(writeConfig, instantTime, getHoodieTable(), taskContextSupplier, new FixedIdSuffixCreateHandleFactory(), false, fileIdPrefixExtractor, readerSchema);
    return ExecutorFactory.create(writeConfig, iterator, insertHandler, op -> op, getHoodieTable().getPreExecuteRunnable()).execute();
  }
}
