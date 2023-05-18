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

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.execution.PartitionIdPassthrough;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;

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
      ValidationUtils.checkState(hoodieTable.getMetaClient().getTableConfig().isMetadataPartitionEnabled(MetadataPartitionType.RECORD_INDEX));
      fileGroupSize = hoodieTable.getMetadataTable().getNumShards(MetadataPartitionType.RECORD_INDEX);
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

    // final variable required for lamda functions below
    final int numFileGroups = fileGroupSize;

    // Partition the record keys to lookup such that each partition looks up one record index shard
    JavaRDD<String> partitionedKeyRDD = HoodieJavaRDD.getJavaRDD(records)
        .map(HoodieRecord::getRecordKey)
        .keyBy(k -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(k, numFileGroups))
        .partitionBy(new PartitionIdPassthrough(numFileGroups))
        .map(t -> t._2);
    ValidationUtils.checkState(partitionedKeyRDD.getNumPartitions() <= numFileGroups);

    // Lookup the keys in the record index
    HoodiePairData<String, HoodieRecordGlobalLocation> keyToLocationPairRDD =
        HoodieJavaPairRDD.of(partitionedKeyRDD.mapPartitionsToPair(new RecordIndexFileGroupLookupFunction(hoodieTable)));

    // Tag the incoming records, as inserts or updates, by joining with existing record keys
    HoodieData<HoodieRecord<R>> taggedRecords = tagLocationBackToRecords(keyToLocationPairRDD, records);

    // The number of partitions in the taggedRecords is expected to the maximum of the partitions in
    // keyToLocationPairRDD and records RDD.

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
    // is being rolled back on the dataset, we will not load the records from the deltacommit and it is virtually
    // rolled back. In other words, there is no need to rollback any deltacommit here except if the deltacommit
    // was compacted and a new basefile has been created.
    try {
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
          .setBasePath(HoodieTableMetadata.getMetadataTableBasePath(config.getBasePath()))
          .setConf(new Configuration()).build();
      HoodieTimeline commitTimeline = metaClient.getCommitTimeline().filterCompletedInstants();
      if (commitTimeline.empty()) {
        // No compaction yet so no need to check for deltacommits due to the logic above
        return true;
      }

      if (HoodieTimeline.compareTimestamps(instantTime, GREATER_THAN, commitTimeline.lastInstant().get().getTimestamp())) {
        // After the last compaction so no rollback required as per logic above
        return true;
      }
      LOG.warn("Cannot rollback instant " + instantTime + " because the corresponding deltacommit has been compacted "
          + " in " + commitTimeline.lastInstant().get().getTimestamp());
      return false;
    } catch (TableNotFoundException e) {
      // Metadata table is not setup.  Nothing to rollback.  Exit gracefully.
      LOG.warn("Cannot rollback instant " + instantTime + " as metadata table is not found");
      return true;
    }
  }

  @Override
  public boolean isGlobal() {
    return true;
  }

  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

  private <R> HoodieData<HoodieRecord<R>> tagLocationBackToRecords(
      HoodiePairData<String, HoodieRecordGlobalLocation> keyFilenamePair,
      HoodieData<HoodieRecord<R>> records) {
    HoodiePairData<String, HoodieRecord<R>> keyRecordPairs =
        records.mapToPair(record -> new ImmutablePair<>(record.getRecordKey(), record));
    // Here as the records might have more data than keyFilenamePairs (some row keys' not found in record index),
    // we will do left outer join.
    return keyRecordPairs.leftOuterJoin(keyFilenamePair).values()
        .map(v -> {
          HoodieRecord<R> record = v.getLeft();
          Option<HoodieRecordGlobalLocation> location = Option.ofNullable(v.getRight().orElse(null));
          if (!location.isPresent()) {
            // No location found.
            return record;
          }
          // Ensure the partitionPath is also set correctly in the key
          if (!record.getPartitionPath().equals(location.get().getPartitionPath())) {
            record = new HoodieAvroRecord(new HoodieKey(record.getRecordKey(), location.get().getPartitionPath()), (HoodieRecordPayload) record.getData());
          }

          // Perform the tagging. Not using HoodieIndexUtils.getTaggedRecord to prevent an additional copy which is not necessary for this index.
          record.unseal();
          record.setCurrentLocation(location.get());
          record.seal();
          return record;
        });
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
      Map<String, HoodieRecordGlobalLocation> recordIndexInfo = hoodieTable.getMetadataTable().readRecordIndex(keysToLookup);

      HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
      HoodieTimeline commitsTimeline = metaClient.getCommitsTimeline().filterCompletedInstants();
      return recordIndexInfo.entrySet().stream()
          .filter(e -> HoodieIndexUtils.checkIfValidCommit(commitsTimeline, e.getValue().getInstantTime()))
          .map(e -> new Tuple2<>(e.getKey(), e.getValue())).iterator();
    }
  }
}