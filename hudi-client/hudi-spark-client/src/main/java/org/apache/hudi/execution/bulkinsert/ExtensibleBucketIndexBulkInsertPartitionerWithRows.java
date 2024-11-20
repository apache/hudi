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

import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.ExtensibleBucketIdentifier;
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils;
import org.apache.hudi.keygen.BuiltinKeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.ExtensibleBucketInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

public class ExtensibleBucketIndexBulkInsertPartitionerWithRows implements BulkInsertPartitioner<Dataset<Row>>, ExtensibleBucketInsertPartitioner {

  private final HoodieTable table;

  private final String indexKeyFields;

  private final boolean populateMetaFields;

  private final Option<BuiltinKeyGenerator> keyGeneratorOpt;

  private final RowRecordKeyExtractor extractor;

  private Map<String/*partition path*/, Pair<ExtensibleBucketIdentifier, Integer/*RDD partition start offset*/>> partitionToIdentifier = new HashMap<>();

  private Map<String/*partition path*/, HoodieExtensibleBucketMetadata/*pending bucket-resizing related metadata*/> pendingMetadata = new HashMap<>();

  private int totalRDDPartitionNum;

  private final String[] sortColumnNames;

  public ExtensibleBucketIndexBulkInsertPartitionerWithRows(HoodieTable table,
                                                            Map<String, String> strategyParams,
                                                            boolean populateMetaFields) {
    ValidationUtils.checkArgument(table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ),
        "ExtensibleBucketIndexBulkInsertPartitionerWithRows is only supported for MergeOnRead tables");
    this.table = table;
    this.indexKeyFields = table.getConfig().getBucketIndexHashField();
    this.populateMetaFields = populateMetaFields;
    if (!populateMetaFields) {
      this.keyGeneratorOpt = HoodieSparkKeyGeneratorFactory.getKeyGenerator(table.getConfig().getProps());
    } else {
      this.keyGeneratorOpt = Option.empty();
    }
    this.extractor = RowRecordKeyExtractor.getRowRecordKeyExtractor(populateMetaFields, keyGeneratorOpt);
    String sortString = strategyParams.getOrDefault(PLAN_STRATEGY_SORT_COLUMNS.key(), "");
    if (!StringUtils.isNullOrEmpty(sortString)) {
      this.sortColumnNames = sortString.split(",");
    } else {
      this.sortColumnNames = null;
    }
  }

  /**
   * Get the RDD partition id for the given row.
   * partition path -> start offset and bucket id, then RDD-Partition-Id = start offset + bucket id
   */
  private int getRDDPartitionId(Row row) {
    String recordKey = extractor.getRecordKey(row);
    String partitionPath = extractor.getPartitionPath(row);
    Pair<ExtensibleBucketIdentifier, Integer> extensibleBucketIdentifierIntegerPair = partitionToIdentifier.get(partitionPath);
    if (extensibleBucketIdentifierIntegerPair == null) {
      throw new HoodieException("Partition path " + partitionPath + " not found in partitionToIdentifier map");
    }
    Integer startOffset = extensibleBucketIdentifierIntegerPair.getRight();
    int bucketId = extensibleBucketIdentifierIntegerPair.getLeft().getBucketId(recordKey, indexKeyFields);
    return startOffset + bucketId;
  }

  @Override
  public void addPendingExtensibleBucketMetadata(HoodieExtensibleBucketMetadata metadata) {
    pendingMetadata.put(metadata.getPartitionPath(), metadata);
  }

  // TODO: resolve unused partitions, for example:
  // bucket-0-0, bucket-1-0 [not exist]    =====bucket-resizing=====>    bucket-0-1, bucket-1-1, bucket-2-1, bucket-3-1
  // but if bucket-1-0 is empty, so only bucket-0-0 ==> bucket-0-1, bucket-2-1 is needed, but we assume RDD partitions num is equal to bucket num (4)
  @Override
  public Dataset<Row> repartitionRecords(Dataset<Row> records, int outputPartitions) {
    JavaRDD<Row> rowJavaRDD = records.toJavaRDD();
    prepareRepartition(rowJavaRDD);

    Dataset<Row> partitionedRows = records.sparkSession().createDataFrame(
        rowJavaRDD.mapToPair(row -> new Tuple2<>(getRDDPartitionId(row), row))
            .partitionBy(new Partitioner() {
              @Override
              public int getPartition(Object key) {
                // bucket id
                return (int) key;
              }

              @Override
              public int numPartitions() {
                return totalRDDPartitionNum;
              }
            })
            .values(), records.schema());

    if (sortColumnNames != null && sortColumnNames.length > 0) {
      partitionedRows = partitionedRows
          .sortWithinPartitions(Arrays.stream(sortColumnNames).map(Column::new).toArray(Column[]::new));
    } else if (table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE) {
      if (populateMetaFields) {
        partitionedRows = partitionedRows.sortWithinPartitions(HoodieRecord.RECORD_KEY_METADATA_FIELD);
      } else {
        throw new HoodieException("Sorting by record key for extensible bucket index requires meta-fields to be enabled");
      }
    }

    return partitionedRows;
  }

  @Override
  public boolean arePartitionRecordsSorted() {
    return (sortColumnNames != null && sortColumnNames.length > 0)
        || table.requireSortedRecords() || table.getConfig().getBulkInsertSortMode() != BulkInsertSortMode.NONE;
  }

  private void prepareRepartition(JavaRDD<Row> rows) {
    List<String> partitions = rows.map(this.extractor::getPartitionPath).distinct().collect();
    int startOffset = 0;
    for (String partition : partitions) {
      ExtensibleBucketIdentifier identifier = getBucketIdentifier(partition);
      partitionToIdentifier.put(partition, Pair.of(identifier, startOffset));
      startOffset += identifier.getBucketNum();
    }
    totalRDDPartitionNum = startOffset;
  }

  private ExtensibleBucketIdentifier getBucketIdentifier(String partition) {
    if (pendingMetadata.containsKey(partition)) {
      return new ExtensibleBucketIdentifier(pendingMetadata.get(partition), true);
    }
    HoodieExtensibleBucketMetadata metadata = ExtensibleBucketIndexUtils.loadOrCreateMetadata(this.table, partition);
    return new ExtensibleBucketIdentifier(metadata);
  }
}
