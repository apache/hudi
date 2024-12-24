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

import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.index.bucket.HoodieSparkConsistentBucketIndex;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.BucketSortBulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Bulk_insert partitioner of Spark row using consistent hashing bucket index.
 */
public class ConsistentBucketIndexBulkInsertPartitionerWithRows extends BucketSortBulkInsertPartitioner<Dataset<Row>> {

  private final String indexKeyFields;

  private final List<String> fileIdPfxList = new ArrayList<>();

  private Map<String/*partition*/, List<ConsistentHashingNode>/*pending resizing related child nodes*/> hashingChildrenNodes;

  private Map<String, ConsistentBucketIdentifier> partitionToIdentifier;

  private final Option<BuiltinKeyGenerator> keyGeneratorOpt;

  private Map<String, Map<String, Integer>> partitionToFileIdPfxIdxMap;

  private final RowRecordKeyExtractor extractor;

  private final boolean populateMetaFields;

  public ConsistentBucketIndexBulkInsertPartitionerWithRows(HoodieTable table,
                                                            Map<String, String> strategyParams, boolean populateMetaFields) {
    this(table, strategyParams, populateMetaFields, null);
  }

  /**
   * Constructor of ConsistentBucketIndexBulkInsertPartitionerWithRows.
   * @param hashingChildrenNodes children nodes for clustering, only used in executing clustering
   */
  public ConsistentBucketIndexBulkInsertPartitionerWithRows(HoodieTable table,
                                                            Map<String, String> strategyParams,
                                                            boolean populateMetaFields, Map<String, List<ConsistentHashingNode>> hashingChildrenNodes) {
    super(table, strategyParams.getOrDefault(PLAN_STRATEGY_SORT_COLUMNS.key(), ""));
    this.indexKeyFields = table.getConfig().getBucketIndexHashField();
    this.populateMetaFields = populateMetaFields;
    if (!populateMetaFields) {
      this.keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.getKeyGenerator(table.getConfig().getProps());
    } else {
      this.keyGeneratorOpt = Option.empty();
    }
    this.extractor = RowRecordKeyExtractor.getRowRecordKeyExtractor(populateMetaFields, keyGeneratorOpt);
    ValidationUtils.checkArgument(table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ),
        "Consistent hash bucket index doesn't support CoW table");
    if (hashingChildrenNodes != null) {
      /**
       * Set pending consistent hashing for partition.
       * The bulk insert will directly use the pending metadata as the consistent hash metadata for writing data to after-resizing buckets.
       * NOTE: Only used in the case of executing bulk insert.
       */
      ValidationUtils.checkArgument(hashingChildrenNodes.values().stream().flatMap(List::stream).noneMatch(n -> n.getTag() == ConsistentHashingNode.NodeTag.NORMAL),
          "children nodes should not be tagged as NORMAL");
      this.hashingChildrenNodes = hashingChildrenNodes;
    }
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    HoodieSparkConsistentBucketIndex index = (HoodieSparkConsistentBucketIndex) table.getIndex();
    HoodieConsistentHashingMetadata metadata =
        ConsistentBucketIndexUtils.loadOrCreateMetadata(this.table, partition, index.getNumBuckets());
    if (hashingChildrenNodes != null) {
      // for executing bucket resizing
      ValidationUtils.checkState(hashingChildrenNodes.containsKey(partition), "children nodes should be provided for clustering");
      metadata.setChildrenNodes(hashingChildrenNodes.get(partition));
    }
    return new ConsistentBucketIdentifier(metadata);
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputPartitions) {
    JavaRDD<Row> rowJavaRDD = rows.toJavaRDD();
    prepareRepartition(rowJavaRDD);

    Dataset<Row> partitionedRows = rows.sparkSession().createDataFrame(rowJavaRDD
        .mapToPair(row -> new Tuple2<>(getBucketId(row), row))
        .partitionBy(new Partitioner() {
          @Override
          public int getPartition(Object key) {
            return (int) key;
          }

          @Override
          public int numPartitions() {
            return fileIdPfxList.size();
          }
        })
        .values(), rows.schema());

    if (isCustomSorted()) {
      partitionedRows = partitionedRows
          .sortWithinPartitions(Arrays.stream(sortColumnNames).map(Column::new).toArray(Column[]::new));
    } else if (isRecordKeySorted()) {
      if (populateMetaFields) {
        partitionedRows = partitionedRows.sortWithinPartitions(HoodieRecord.RECORD_KEY_METADATA_FIELD);
      } else {
        throw new HoodieException("Sorting by record key for consistent hashing bucket index requires meta-fields to be enabled");
      }
    }

    return partitionedRows;
  }

  /**
   * Prepare consistent hashing metadata for repartition
   *
   * @param rows input records
   */
  private void prepareRepartition(JavaRDD<Row> rows) {
    this.partitionToIdentifier = initializeBucketIdentifier(rows);
    this.partitionToFileIdPfxIdxMap = ConsistentBucketIndexUtils.generatePartitionToFileIdPfxIdxMap(partitionToIdentifier);
    partitionToIdentifier.values().forEach(identifier -> {
      fileIdPfxList.addAll(identifier.getNodes().stream().map(ConsistentHashingNode::getFileIdPrefix).collect(Collectors.toList()));
    });
  }

  /**
   * Initialize hashing metadata of input records. The metadata of all related partitions will be loaded, and
   * the mapping from partition to its bucket identifier is constructed.
   */
  private Map<String, ConsistentBucketIdentifier> initializeBucketIdentifier(JavaRDD<Row> rows) {
    if (hashingChildrenNodes != null) {
      return hashingChildrenNodes.keySet().stream().collect(Collectors.toMap(p -> p, this::getBucketIdentifier));
    }
    return rows.map(this.extractor::getPartitionPath).distinct().collect().stream()
        .collect(Collectors.toMap(p -> p, this::getBucketIdentifier));
  }

  private int getBucketId(Row row) {
    String recordKey = extractor.getRecordKey(row);
    String partitionPath = extractor.getPartitionPath(row);
    ConsistentHashingNode node = partitionToIdentifier.get(partitionPath).getBucket(recordKey, indexKeyFields);
    return partitionToFileIdPfxIdxMap.get(partitionPath).get(node.getFileIdPrefix());
  }
}
