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

import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.bucket.ExtensibleBucketIdentifier;
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils;
import org.apache.hudi.index.bucket.HoodieSparkExtensibleBucketIndex;
import org.apache.hudi.io.HoodieWriteHandle;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.ExtensibleBucketInsertPartitioner;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

// TODO: complete the implementation for extensible bucket
public class RDDExtensibleBucketBulkInsertPartitioner<T> extends RDDBucketIndexPartitioner<T> implements ExtensibleBucketInsertPartitioner {

  private Map<String/*partition path*/, HoodieExtensibleBucketMetadata/*pending bucket-resizing related metadata*/> pendingMetadata = new HashMap<>();

  public RDDExtensibleBucketBulkInsertPartitioner(HoodieTable table,
                                                  Map<String, String> strategyParams,
                                                  boolean preserveHoodieMetadata) {
    super(table,
        strategyParams.getOrDefault(PLAN_STRATEGY_SORT_COLUMNS.key(), null),
        preserveHoodieMetadata);
    ValidationUtils.checkArgument(table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ),
        "Extensible bucket index doesn't support CoW table");
    ValidationUtils.checkArgument(table.getIndex() instanceof HoodieSparkExtensibleBucketIndex,
        "RDDExtensibleBucketPartitioner can only be used together with extensible bucket index");
  }

  public RDDExtensibleBucketBulkInsertPartitioner(HoodieTable table) {
    this(table, Collections.emptyMap(), false);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputPartitions) {
    Pair<Map<String, Pair<ExtensibleBucketIdentifier, Integer>>, Integer> partitionToIdentifier = initializeBucketIdentifier(records);
    return doPartition(records, new Partitioner() {
      @Override
      public int getPartition(Object key) {
        HoodieKey hoodieKey = (HoodieKey) key;
        String partition = hoodieKey.getPartitionPath();
        Pair<ExtensibleBucketIdentifier, Integer> extensibleBucketIdentifierIntegerPair = partitionToIdentifier.getLeft().get(partition);
        ExtensibleBucketIdentifier identifier = extensibleBucketIdentifierIntegerPair.getLeft();
        int startOffset = extensibleBucketIdentifierIntegerPair.getRight();
        return startOffset + identifier.getBucketId(hoodieKey.getRecordKey(), indexKeyFields);
      }

      @Override
      public int numPartitions() {
        return partitionToIdentifier.getRight();
      }
    });
  }

  @Override
  public void addPendingExtensibleBucketMetadata(HoodieExtensibleBucketMetadata metadata) {
    pendingMetadata.put(metadata.getPartitionPath(), metadata);
  }

  private Pair<Map<String/*partition path*/, Pair<ExtensibleBucketIdentifier, Integer>>, Integer/*total RDD partition num*/> initializeBucketIdentifier(JavaRDD<HoodieRecord<T>> records) {
    List<String> partitions = records.map(HoodieRecord::getPartitionPath).distinct().collect();
    int startOffset = 0;
    Map<String/*partition path*/, Pair<ExtensibleBucketIdentifier, Integer/*RDD partition start offset*/>> partitionToIdentifier = new HashMap<>();
    for (String partition : partitions) {
      ExtensibleBucketIdentifier identifier = getBucketIdentifier(partition);
      partitionToIdentifier.put(partition, Pair.of(identifier, startOffset));
      fileIdPfxList.addAll(identifier.generateFileIdPrefixForAllBuckets().collect(Collectors.toList()));
      if (identifier.isPending()) {
        // for pending bucket-resizing related partition, all buckets in this partition is not exist now, so can't be appended
        doAppend.addAll(Collections.nCopies(identifier.getBucketNum(), false));
      } else {
        // for commited bucket layout, only bucket with absent location in fs can be appended
        doAppend.addAll(identifier.generateRecordLocationForAllBucketsWithLogical().map(location -> !location.isLogicalLocation()).collect(Collectors.toList()));
      }
      startOffset += identifier.getBucketNum();
    }
    return Pair.of(partitionToIdentifier, startOffset);
  }

  private ExtensibleBucketIdentifier getBucketIdentifier(String partition) {
    if (pendingMetadata.containsKey(partition)) {
      return new ExtensibleBucketIdentifier(pendingMetadata.get(partition), true);
    }
    HoodieExtensibleBucketMetadata metadata = ExtensibleBucketIndexUtils.loadOrCreateMetadata(this.table, partition);
    return new ExtensibleBucketIdentifier(metadata);
  }

  @Override
  public Option<WriteHandleFactory> getWriteHandleFactory(int idx) {
    return super.getWriteHandleFactory(idx).map(writeHandleFactory -> new WriteHandleFactory() {
      @Override
      public HoodieWriteHandle create(HoodieWriteConfig config, String commitTime, HoodieTable hoodieTable, String partitionPath, String fileIdPrefix, TaskContextSupplier taskContextSupplier) {
        // Ensure we do not create append handle for extensible bucket bulk_insert, align with `ExtensibleBucketBulkInsertDataInternalWriterHelper`
        ValidationUtils.checkArgument(!doAppend.get(idx), "Extensible Bucket bulk_insert only support write to new file group");
        return writeHandleFactory.create(config, commitTime, hoodieTable, partitionPath, fileIdPrefix, taskContextSupplier);
      }
    });
  }

}
