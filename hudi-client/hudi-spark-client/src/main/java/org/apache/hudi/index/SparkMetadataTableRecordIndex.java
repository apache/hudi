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

package org.apache.hudi.index;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import scala.Tuple2;

import static org.apache.hudi.index.HoodieIndexUtils.tagGlobalLocationBackToRecords;

/**
 * Hoodie Index implementation backed by the record index present in the Metadata Table.
 */
public class SparkMetadataTableRecordIndex extends HoodieIndex<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkMetadataTableRecordIndex.class);
  // The index to fallback upon when record index is not initialized yet.
  // This should be a global index like record index so that the behavior of tagging across partitions is not changed.
  private static final HoodieIndex.IndexType FALLBACK_INDEX_TYPE = IndexType.GLOBAL_SIMPLE;

  public SparkMetadataTableRecordIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(HoodieData<HoodieRecord<R>> records, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    int fileGroupSize;
    try {
      ValidationUtils.checkState(hoodieTable.getMetaClient().getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX));
      fileGroupSize = hoodieTable.getMetadataTable().getNumFileGroupsForPartition(MetadataPartitionType.RECORD_INDEX);
      ValidationUtils.checkState(fileGroupSize > 0, "Record index should have at least one file group");
    } catch (TableNotFoundException | IllegalStateException e) {
      // This means that record index has not been initialized.
      LOG.warn(String.format("Record index not initialized so falling back to %s for tagging records", FALLBACK_INDEX_TYPE.name()));

      // Fallback to another index so that tagLocation is still accurate and there are no duplicates.
      HoodieWriteConfig otherConfig = HoodieWriteConfig.newBuilder().withProperties(config.getProps())
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(FALLBACK_INDEX_TYPE).build()).build();
      HoodieIndex fallbackIndex = SparkHoodieIndexFactory.createIndex(otherConfig);

      // Fallback index needs to be a global index like record index
      ValidationUtils.checkArgument(fallbackIndex.isGlobal(), "Fallback index needs to be a global index like record index");

      return fallbackIndex.tagLocation(records, context, hoodieTable);
    }

    final int numFileGroups = fileGroupSize;

    if (config.getRecordIndexUseCaching()) {
      records.persist(config.getRecordIndexInputStorageLevel());
    }

    // Partition the record keys to lookup such that each partition looks up one record index shard
    SerializableBiFunction<String, Integer, Integer> mappingFunction = HoodieTableMetadataUtil::mapRecordKeyToFileGroupIndex;
    JavaRDD<String> partitionedKeyRDD = HoodieJavaRDD.getJavaRDD(records)
        .map(HoodieRecord::getRecordKey)
        .keyBy(k -> mappingFunction.apply(k, numFileGroups))
        .partitionBy(new PartitionIdPassthrough(numFileGroups))
        .map(t -> t._2);
    ValidationUtils.checkState(partitionedKeyRDD.getNumPartitions() <= numFileGroups);

    // Lookup the keys in the record index
    HoodiePairData<String, HoodieRecordGlobalLocation> keyAndExistingLocations =
        HoodieJavaPairRDD.of(partitionedKeyRDD.mapPartitionsToPair(new RecordIndexFileGroupLookupFunction(hoodieTable)));

    // Tag the incoming records, as inserts or updates, by joining with existing record keys
    boolean shouldUpdatePartitionPath = config.getRecordIndexUpdatePartitionPath() && hoodieTable.isPartitioned();
    HoodieData<HoodieRecord<R>> taggedRecords = tagGlobalLocationBackToRecords(records, keyAndExistingLocations,
        false, shouldUpdatePartitionPath, config, hoodieTable);

    // The number of partitions in the taggedRecords is expected to the maximum of the partitions in
    // keyToLocationPairRDD and records RDD.

    if (config.getRecordIndexUseCaching()) {
      records.unpersist();
    }

    return taggedRecords;
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    // This is a no-op as metadata record index updates are automatically maintained within the metadata table.
    return writeStatuses;
  }

  @Override
  public boolean rollbackCommit(String instantTime) {
    // Only those deltacommits which have a valid completed commit on the dataset are read. Since, the instantTime
    // is being rolled back on the dataset, we will anyway not load the records from the deltacommit.
    //
    // In other words, there is no need to rollback anything here as the MDT rollback mechanism will take care of it.
    return true;
  }

  @Override
  public boolean isGlobal() {
    return true;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  /**
   * Function that lookups a list of keys in a single shard of the record index
   */
  private static class RecordIndexFileGroupLookupFunction implements PairFlatMapFunction<Iterator<String>, String, HoodieRecordGlobalLocation> {
    private final HoodieTable hoodieTable;

    public RecordIndexFileGroupLookupFunction(HoodieTable hoodieTable) {
      this.hoodieTable = hoodieTable;
    }

    @Override
    public Iterator<Tuple2<String, HoodieRecordGlobalLocation>> call(Iterator<String> recordKeyIterator) {
      List<String> keysToLookup = new ArrayList<>();
      recordKeyIterator.forEachRemaining(keysToLookup::add);

      // recordIndexInfo object only contains records that are present in record_index.
      HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData =
          hoodieTable.getMetadataTable().readRecordIndex(HoodieListData.eager(keysToLookup));
      try {
        Map<String, HoodieRecordGlobalLocation> recordIndexInfo = HoodieDataUtils.dedupeAndCollectAsMap(recordIndexData);
        return recordIndexInfo.entrySet().stream()
            .map(e -> new Tuple2<>(e.getKey(), e.getValue())).iterator();
      } finally {
        // Clean up the RDD to avoid memory leaks
        recordIndexData.unpersistWithDependencies();
      }
    }
  }

  /**
   * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
   * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
   *
   * NOTE: This is a workaround for SPARK-39391, which moved the PartitionIdPassthrough from
   * {@link org.apache.spark.sql.execution.ShuffledRowRDD} to {@link Partitioner}.
   */
  private class PartitionIdPassthrough extends Partitioner {

    private final int numPartitions;

    public PartitionIdPassthrough(int numPartitions) {
      this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
      return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
      return (int) key;
    }
  }
}
