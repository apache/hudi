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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.index.bucket.HoodieSparkConsistentBucketIndex;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.ConsistentHashingBucketInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Bulk_insert partitioner of Spark row using consistent hashing bucket index.
 */
public class ConsistentBucketIndexBulkInsertPartitionerWithRows
    implements BulkInsertPartitioner<Dataset<Row>>, ConsistentHashingBucketInsertPartitioner {

  private final HoodieTable table;

  private final String indexKeyFields;

  private final String[] sortColumnNames;

  private final List<String> fileIdPfxList = new ArrayList<>();

  private final Map<String, List<ConsistentHashingNode>> hashingChildrenNodes;

  private Map<String, ConsistentBucketIdentifier> partitionToIdentifier;

  private final Option<BuiltinKeyGenerator> keyGeneratorOpt;

  private Map<String, Map<String, Integer>> partitionToFileIdPfxIdxMap;

  private final RowRecordKeyExtractor extractor;

  public ConsistentBucketIndexBulkInsertPartitionerWithRows(HoodieTable table, Map<String, String> strategyParams, boolean populateMetaFields) {
    this.indexKeyFields = table.getConfig().getBucketIndexHashField();
    this.table = table;
    this.hashingChildrenNodes = new HashMap<>();
    if (!populateMetaFields) {
      this.keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.getKeyGenerator(table.getConfig().getProps());
    } else {
      this.keyGeneratorOpt = Option.empty();
    }
    String sortString = strategyParams.getOrDefault(PLAN_STRATEGY_SORT_COLUMNS.key(), "");
    if (!StringUtils.isNullOrEmpty(sortString)) {
      this.sortColumnNames = sortString.split(",");
    } else {
      this.sortColumnNames = null;
    }
    this.extractor = RowRecordKeyExtractor.getRowRecordKeyExtractor(populateMetaFields, keyGeneratorOpt);
    ValidationUtils.checkArgument(table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ),
        "Consistent hash bucket index doesn't support CoW table");
  }

  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    HoodieSparkConsistentBucketIndex index = (HoodieSparkConsistentBucketIndex) table.getIndex();
    HoodieConsistentHashingMetadata metadata = ConsistentBucketIndexUtils.loadOrCreateMetadata(this.table, partition, index.getNumBuckets());
    if (hashingChildrenNodes.containsKey(partition)) {
      metadata.setChildrenNodes(hashingChildrenNodes.get(partition));
    }
    return new ConsistentBucketIdentifier(metadata);
  }

  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> rows, int outputPartitions) {
    JavaRDD<Row> rowJavaRDD = rows.toJavaRDD();
    prepareRepartition(rowJavaRDD);

    Partitioner partitioner = new Partitioner() {
      @Override
      public int getPartition(Object key) {
        Row row = (Row) key;
        return getBucketId(row);
      }

      @Override
      public int numPartitions() {
        return fileIdPfxList.size();
      }
    };

    if (sortColumnNames != null && sortColumnNames.length > 0) {
      return rows.sparkSession().createDataFrame(rowJavaRDD
              .mapToPair(row -> new Tuple2<>(row, row))
              .repartitionAndSortWithinPartitions(partitioner, new CustomRowColumnsComparator())
              .values(),
          rows.schema());
    } else if (table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE) {
      return rows.sparkSession().createDataFrame(
          rowJavaRDD
              .mapToPair(row -> new Tuple2<>(row, row))
              .repartitionAndSortWithinPartitions(partitioner, new RowRecordKeyComparator())
              .values(),
          rows.schema());
    } else {
      return rows.sparkSession().createDataFrame(rowJavaRDD
          .mapToPair(row -> new Tuple2<>(row, row))
          .partitionBy(partitioner)
          .values(), rows.schema());
    }
  }

  /**
   * A comparator for Rows that compares them based on an array of column names.
   */
  private class CustomRowColumnsComparator implements Comparator<Row>, Serializable {
    @Override
    public int compare(Row row1, Row row2) {
      for (String column : sortColumnNames) {
        Comparable value1 = row1.getAs(column);
        Comparable value2 = row2.getAs(column);
        int comparison = value1.compareTo(value2);
        if (comparison != 0) {
          return comparison;
        }
      }
      return 0;
    }
  }

  /**
   * A comparator for Rows that compares them based on record keys.
   */
  private class RowRecordKeyComparator implements Comparator<Row>, Serializable {
    @Override
    public int compare(Row row1, Row row2) {
      String recordKey1 = extractor.getRecordKey(row1);
      String recordKey2 = extractor.getRecordKey(row2);
      return recordKey1.compareTo(recordKey2);
    }
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
    return rows.map(this.extractor::getPartitionPath).distinct().collect().stream()
        .collect(Collectors.toMap(p -> p, this::getBucketIdentifier));
  }

  @Override
  public void addHashingChildrenNodes(String partition, List<ConsistentHashingNode> nodes) {
    ValidationUtils.checkState(nodes.stream().noneMatch(n -> n.getTag() == ConsistentHashingNode.NodeTag.NORMAL),
        "children nodes should not be tagged as NORMAL");
    hashingChildrenNodes.put(partition, nodes);
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return (sortColumnNames != null && sortColumnNames.length > 0)
        || table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE;
  }

  private Integer getBucketId(Row row) {
    String recordKey = extractor.getRecordKey(row);
    String partitionPath = extractor.getPartitionPath(row);
    ConsistentHashingNode node = partitionToIdentifier.get(partitionPath).getBucket(recordKey, indexKeyFields);
    return partitionToFileIdPfxIdxMap.get(partitionPath).get(node.getFileIdPrefix());
  }
}
