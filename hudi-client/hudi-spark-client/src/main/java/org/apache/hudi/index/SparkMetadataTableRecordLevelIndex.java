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

package org.apache.hudi.index;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.BucketizedMetadataTableFileGroupIndexParser;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.api.java.JavaRDD;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Index to be used with RLI. Queries the record index for tables with non-global record keys
 */
public class SparkMetadataTableRecordLevelIndex extends SparkMetadataTableGlobalRecordLevelIndex {

  public SparkMetadataTableRecordLevelIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  protected HoodieIndex.IndexType getFallbackIndexType() {
    return IndexType.SIMPLE;
  }

  @Override
  protected <R> HoodiePairData<String, HoodieRecordGlobalLocation> lookupRecords(HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
                                                                                 HoodieTable hoodieTable, Either<Integer, Map<String, Integer>> fileGroupSize) {
    Map<String, Integer> fileGroupCountPerDataPartition = fileGroupSize.asRight();
    int numFileGroups = getTotalFileGroupCount(fileGroupSize);
    Map<String, Integer> partitionOffsetIndexes = BucketizedMetadataTableFileGroupIndexParser.generatePartitionToBaseIndexOffsets(fileGroupCountPerDataPartition);

    // Partition the record keys to lookup such that each partition looks up one record index shard
    JavaRDD<Pair<String, String>> partitionedKeyRDD = HoodieJavaRDD.getJavaRDD(records)
        .filter(record -> partitionOffsetIndexes.containsKey(record.getPartitionPath()))
        .map(record -> Pair.of(record.getPartitionPath(), record.getRecordKey()))
        // get offset from partitionOffsetIndexes then add the hash of the key
        .keyBy(k -> partitionOffsetIndexes.get(k.getLeft()) + HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(k.getRight(), fileGroupCountPerDataPartition.get(k.getLeft())))
        .partitionBy(new PartitionIdPassthrough(numFileGroups))
        .map(t -> t._2);
    ValidationUtils.checkState(partitionedKeyRDD.getNumPartitions() <= numFileGroups);
    // Lookup the keys in the record index
    return HoodieJavaPairRDD.of(partitionedKeyRDD.mapPartitionsToPair(new PartitionedRecordIndexFileGroupLookupFunction(hoodieTable.getTableMetadata())));
  }

  @Override
  protected Either<Integer, Map<String, Integer>> fetchFileGroupSize(HoodieTable hoodieTable) {
    Map<String, Integer> partitionSizes = new HashMap<>();
    Map<String, List<FileSlice>> fileGroups = hoodieTable.getTableMetadata().getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX);
    fileGroups.keySet().forEach(k -> partitionSizes.put(k, fileGroups.get(k).size()));
    return Either.right(partitionSizes);
  }

  @Override
  protected int getTotalFileGroupCount(Either<Integer, Map<String, Integer>> fileGroupSize) {
    return BucketizedMetadataTableFileGroupIndexParser.calculateNumberOfFileGroups(fileGroupSize.asRight());
  }

  @Override
  protected boolean shouldUpdatePartitionPath(HoodieTable hoodieTable) {
    return false;
  }
}
