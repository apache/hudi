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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.HoodieSparkConsistentBucketIndex;
import org.apache.hudi.io.AppendHandleFactory;
import org.apache.hudi.io.SingleFileHandleCreateFactory;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * A partitioner for (consistent hashing) bucket index used in bulk_insert
 */
public class RDDConsistentBucketPartitioner<T> extends RDDBucketIndexPartitioner<T> {

  private static final Logger LOG = LogManager.getLogger(RDDConsistentBucketPartitioner.class);

  private final HoodieTable table;
  private final List<String> indexKeyFields;
  private final Map<String, List<ConsistentHashingNode>> hashingChildrenNodes;
  private final String[] sortColumnNames;
  private final boolean preserveHoodieMetadata;
  private final boolean consistentLogicalTimestampEnabled;

  private List<Boolean> doAppend;
  private List<String> fileIdPfxList;

  public RDDConsistentBucketPartitioner(HoodieTable table, Map<String, String> strategyParams, boolean preserveHoodieMetadata) {
    this.table = table;
    this.indexKeyFields = Arrays.asList(table.getConfig().getBucketIndexHashField().split(","));
    this.hashingChildrenNodes = new HashMap<>();
    this.consistentLogicalTimestampEnabled = table.getConfig().isConsistentLogicalTimestampEnabled();
    this.preserveHoodieMetadata = preserveHoodieMetadata;

    if (strategyParams.containsKey(PLAN_STRATEGY_SORT_COLUMNS.key())) {
      sortColumnNames = strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()).split(",");
    } else {
      sortColumnNames = null;
    }
  }

  public RDDConsistentBucketPartitioner(HoodieTable table) {
    this(table, Collections.emptyMap(), false);
    ValidationUtils.checkArgument(table.getIndex() instanceof HoodieSparkConsistentBucketIndex,
        "RDDConsistentBucketPartitioner can only be used together with consistent hashing bucket index");
    ValidationUtils.checkArgument(table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ),
        "CoW table with bucket index doesn't support bulk_insert");
  }

  /**
   * Repartition the records to conform the bucket index storage layout constraints.
   * Specifically, partition the records based on consistent bucket index, which is computed
   * using hashing metadata and records' key.
   *
   * @param records               Input Hoodie records
   * @param outputSparkPartitions Not used, the actual parallelism is determined by the bucket number
   * @return partitioned records, each partition of data corresponds to a bucket (i.e., file group)
   */
  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
    Map<String, ConsistentBucketIdentifier> partitionToIdentifier = initializeBucketIdentifier(records);
    Map<String, Map<String, Integer>> partitionToFileIdPfxIdxMap = generateFileIdPfx(partitionToIdentifier);
    return doPartition(records, new Partitioner() {
      @Override
      public int numPartitions() {
        return fileIdPfxList.size();
      }

      @Override
      public int getPartition(Object key) {
        HoodieKey hoodieKey = (HoodieKey) key;
        String partition = hoodieKey.getPartitionPath();
        ConsistentHashingNode node = partitionToIdentifier.get(partition).getBucket(hoodieKey, indexKeyFields);
        return partitionToFileIdPfxIdxMap.get(partition).get(node.getFileIdPrefix());
      }
    });
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return (sortColumnNames != null && sortColumnNames.length > 0)
        || table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE;
  }

  @Override
  public Option<WriteHandleFactory> getWriteHandleFactory(int idx) {
    return doAppend.get(idx) ? Option.of(new AppendHandleFactory()) :
        Option.of(new SingleFileHandleCreateFactory(FSUtils.createNewFileId(getFileIdPfx(idx), 0), this.preserveHoodieMetadata));
  }

  @Override
  public String getFileIdPfx(int partitionId) {
    return fileIdPfxList.get(partitionId);
  }

  public void addHashingChildrenNodes(String partition, List<ConsistentHashingNode> nodes) {
    ValidationUtils.checkState(nodes.stream().noneMatch(n -> n.getTag() == ConsistentHashingNode.NodeTag.NORMAL), "children nodes should not be tagged as NORMAL");
    hashingChildrenNodes.put(partition, nodes);
  }

  /**
   * Get (construct) the bucket identifier of the given partition
   */
  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    HoodieSparkConsistentBucketIndex index = (HoodieSparkConsistentBucketIndex) table.getIndex();
    HoodieConsistentHashingMetadata metadata = index.loadOrCreateMetadata(this.table, partition);
    if (hashingChildrenNodes.containsKey(partition)) {
      metadata.setChildrenNodes(hashingChildrenNodes.get(partition));
    }
    return new ConsistentBucketIdentifier(metadata);
  }

  /**
   * Initialize hashing metadata of input records. The metadata of all related partitions will be loaded, and
   * the mapping from partition to its bucket identifier is constructed.
   */
  private Map<String, ConsistentBucketIdentifier> initializeBucketIdentifier(JavaRDD<HoodieRecord<T>> records) {
    return records.map(HoodieRecord::getPartitionPath).distinct().collect().stream()
        .collect(Collectors.toMap(p -> p, p -> getBucketIdentifier(p)));
  }

  /**
   * Initialize fileIdPfx for each data partition. Specifically, the following fields is constructed:
   * - fileIdPfxList: the Nth element corresponds to the Nth data partition, indicating its fileIdPfx
   * - doAppend: represents if the Nth data partition should use AppendHandler
   * - partitionToFileIdPfxIdxMap (return value): (table partition) -> (fileIdPfx -> idx) mapping
   *
   * @param partitionToIdentifier Mapping from table partition to bucket identifier
   */
  private Map<String, Map<String, Integer>> generateFileIdPfx(Map<String, ConsistentBucketIdentifier> partitionToIdentifier) {
    Map<String, Map<String, Integer>> partitionToFileIdPfxIdxMap = new HashMap(partitionToIdentifier.size() * 2);
    doAppend = new ArrayList<>();
    fileIdPfxList = new ArrayList<>();
    int count = 0;
    for (ConsistentBucketIdentifier identifier : partitionToIdentifier.values()) {
      Map<String, Integer> fileIdPfxToIdx = new HashMap();
      for (ConsistentHashingNode node : identifier.getNodes()) {
        fileIdPfxToIdx.put(node.getFileIdPrefix(), count++);
      }
      fileIdPfxList.addAll(identifier.getNodes().stream().map(ConsistentHashingNode::getFileIdPrefix).collect(Collectors.toList()));
      if (identifier.getMetadata().isFirstCreated()) {
        // Create new file group when the hashing metadata is new (i.e., first write to the partition)
        doAppend.addAll(Collections.nCopies(identifier.getNodes().size(), false));
      } else {
        // Child node requires generating a fresh new base file, rather than log file
        doAppend.addAll(identifier.getNodes().stream().map(n -> n.getTag() == ConsistentHashingNode.NodeTag.NORMAL).collect(Collectors.toList()));
      }
      partitionToFileIdPfxIdxMap.put(identifier.getMetadata().getPartitionPath(), fileIdPfxToIdx);
    }

    ValidationUtils.checkState(fileIdPfxList.size() == partitionToIdentifier.values().stream().mapToInt(ConsistentBucketIdentifier::getNumBuckets).sum(),
        "Error state after constructing fileId & idx mapping");
    return partitionToFileIdPfxIdxMap;
  }

  /**
   * Execute partition using the given partitioner.
   * If sorting is required, will do it within each data partition:
   * - if sortColumnNames is specified, apply sort to the column (the behaviour is the same as `RDDCustomColumnsSortPartitioner`
   * - if table requires sort or BulkInsertSortMode is not None, then sort by record key within partition.
   * By default, do partition only.
   *
   * @param records
   * @param partitioner a default partition that accepts `HoodieKey` as the partition key
   * @return
   */
  private JavaRDD<HoodieRecord<T>> doPartition(JavaRDD<HoodieRecord<T>> records, Partitioner partitioner) {
    if (sortColumnNames != null && sortColumnNames.length > 0) {
      return doPartitionAndCustomColumnSort(records, partitioner);
    } else if (table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE) {
      return doPartitionAndSortByRecordKey(records, partitioner);
    } else {
      // By default, do partition only
      return records.mapToPair(record -> new Tuple2<>(record.getKey(), record))
          .partitionBy(partitioner).map(Tuple2::_2);
    }
  }

  /**
   * Sort by specified column value. The behaviour is the same as `RDDCustomColumnsSortPartitioner`
   *
   * @param records
   * @param partitioner
   * @return
   */
  private JavaRDD<HoodieRecord<T>> doPartitionAndCustomColumnSort(JavaRDD<HoodieRecord<T>> records, Partitioner partitioner) {
    final String[] sortColumns = sortColumnNames;
    final SerializableSchema schema = new SerializableSchema(HoodieAvroUtils.addMetadataFields((new Schema.Parser().parse(table.getConfig().getSchema()))));
    Comparator<HoodieRecord<T>> comparator = (Comparator<HoodieRecord<T>> & Serializable) (t1, t2) -> {
      Object obj1 = t1.getRecordColumnValues(schema.get(), sortColumns, consistentLogicalTimestampEnabled);
      Object obj2 = t2.getRecordColumnValues(schema.get(), sortColumns, consistentLogicalTimestampEnabled);
      return ((Comparable) obj1).compareTo(obj2);
    };

    return records.mapToPair(record -> new Tuple2<>(record, record))
        .repartitionAndSortWithinPartitions(new Partitioner() {
          @Override
          public int numPartitions() {
            return partitioner.numPartitions();
          }

          @Override
          public int getPartition(Object key) {
            return partitioner.getPartition(((HoodieRecord) key).getKey());
          }
        }, comparator).map(Tuple2::_2);
  }

  /**
   * Sort by record key within each partition. The behaviour is the same as BulkInsertSortMode.PARTITION_SORT.
   *
   * @param records
   * @param partitioner
   * @return
   */
  private JavaRDD<HoodieRecord<T>> doPartitionAndSortByRecordKey(JavaRDD<HoodieRecord<T>> records, Partitioner partitioner) {
    if (table.getConfig().getBulkInsertSortMode() == BulkInsertSortMode.GLOBAL_SORT) {
      LOG.warn("Consistent bucket does not support global sort mode, the sort will only be done within each data partition");
    }

    Comparator<HoodieKey> comparator = (Comparator<HoodieKey> & Serializable) (t1, t2) -> {
      return t1.getRecordKey().compareTo(t2.getRecordKey());
    };

    return records.mapToPair(record -> new Tuple2<>(record.getKey(), record))
        .repartitionAndSortWithinPartitions(partitioner, comparator)
        .map(Tuple2::_2);
  }
}
