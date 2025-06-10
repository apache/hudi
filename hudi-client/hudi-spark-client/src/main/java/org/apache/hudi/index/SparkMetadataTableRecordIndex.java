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
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.index.HoodieIndexUtils.tagGlobalLocationBackToRecords;
import static org.apache.hudi.metadata.HoodieTableMetadata.EMPTY_PARTITION_NAME;

/**
 * Hoodie Index implementation backed by the record index present in the Metadata Table.
 */
public class SparkMetadataTableRecordIndex extends HoodieIndex<Object, Object> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkMetadataTableRecordIndex.class);

  public SparkMetadataTableRecordIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(HoodieData<HoodieRecord<R>> records, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    Either<Integer, Map<String, Integer>> fileGroupSize;
    try {
      ValidationUtils.checkState(hoodieTable.getMetaClient().getTableConfig().isMetadataPartitionAvailable(MetadataPartitionType.RECORD_INDEX));
      fileGroupSize = fetchFileGroupSize(hoodieTable);
      ValidationUtils.checkState(getTotalFileGroupCount(fileGroupSize) > 0, "Record index should have at least one file group");
    } catch (TableNotFoundException | IllegalStateException e) {
      // This means that record index has not been initialized.
      LOG.warn(String.format("Record index not initialized so falling back to %s for tagging records", getFallbackIndexType().name()));

      // Fallback to another index so that tagLocation is still accurate and there are no duplicates.
      HoodieWriteConfig otherConfig = HoodieWriteConfig.newBuilder().withProperties(config.getProps())
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(getFallbackIndexType()).build()).build();
      HoodieIndex fallbackIndex = SparkHoodieIndexFactory.createIndex(otherConfig);

      // Fallback index needs to be a global index like record index
      ValidationUtils.checkArgument(isGlobal() == fallbackIndex.isGlobal(), "Fallback index needs to have same isGlobal() as the record index");

      return fallbackIndex.tagLocation(records, context, hoodieTable);
    }

    if (config.getRecordIndexUseCaching()) {
      records.persist(new HoodieConfig(config.getProps()).getString(HoodieIndexConfig.RECORD_INDEX_INPUT_STORAGE_LEVEL_VALUE));
    }

    HoodiePairData<String, HoodieRecordGlobalLocation> keyAndExistingLocations = lookupRecords(records, context, hoodieTable, fileGroupSize);

    // Tag the incoming records, as inserts or updates, by joining with existing record keys
    HoodieData<HoodieRecord<R>> taggedRecords = tagGlobalLocationBackToRecords(records, keyAndExistingLocations,
        false, shouldUpdatePartitionPath(hoodieTable), config, hoodieTable);

    if (config.getRecordIndexUseCaching()) {
      records.unpersist();
    }

    return taggedRecords;
  }

  /**
   * The index to fallback upon when record index is not initialized yet.
   * This should be a global index like record index so that the behavior of tagging across partitions is not changed.
   */
  protected HoodieIndex.IndexType getFallbackIndexType() {
    return IndexType.GLOBAL_SIMPLE;
  }

  protected <R> HoodiePairData<String, HoodieRecordGlobalLocation> lookupRecords(HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
                                                                                 HoodieTable hoodieTable, Either<Integer, Map<String, Integer>> fileGroupSize) {
    int numFileGroups = fileGroupSize.asLeft();
    // Partition the record keys to lookup such that each partition looks up one record index shard
    JavaRDD<String> partitionedKeyRDD = HoodieJavaRDD.getJavaRDD(records)
        .map(HoodieRecord::getRecordKey)
        .keyBy(k -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(k, numFileGroups))
        .partitionBy(new PartitionIdPassthrough(numFileGroups))
        .map(t -> t._2);
    // The number of partitions in the taggedRecords is expected to the maximum of the partitions in
    // keyToLocationPairRDD and records RDD.
    ValidationUtils.checkState(partitionedKeyRDD.getNumPartitions() <= numFileGroups);

    // Lookup the keys in the record index
    HoodiePairData<String, HoodieRecordGlobalLocation> keyAndExistingLocations =
        HoodieJavaPairRDD.of(partitionedKeyRDD.mapPartitionsToPair(new RecordIndexFileGroupLookupFunction(hoodieTable)));

    return mayBeValidateAgainstFilesPartition(keyAndExistingLocations, hoodieTable);
  }

  protected Either<Integer, Map<String, Integer>> fetchFileGroupSize(HoodieTable hoodieTable) {
    return Either.left(hoodieTable.getMetadataTable().getNumFileGroupsForPartition(MetadataPartitionType.RECORD_INDEX));
  }

  protected int getTotalFileGroupCount(Either<Integer, Map<String, Integer>> fileGroupSize) {
    return fileGroupSize.asLeft();
  }

  protected boolean shouldUpdatePartitionPath(HoodieTable hoodieTable) {
    return config.getRecordIndexUpdatePartitionPath() && hoodieTable.isPartitioned();
  }

  @VisibleForTesting
  protected HoodiePairData<String, HoodieRecordGlobalLocation> mayBeValidateAgainstFilesPartition(HoodiePairData<String, HoodieRecordGlobalLocation> keyAndExistingLocations,
                                                                                                HoodieTable hoodieTable) {
    if (config.getRecordIndexValidateAgainstFilesPartitionOnReads()) {
      List<List<String>> outOfSyncFileIdListOfLists =
          keyAndExistingLocations.mapValues((SerializableFunction<HoodieRecordGlobalLocation, Pair<String, String>>) v1 -> Pair.of(v1.getPartitionPath(), v1.getFileId()))
              .values().distinct()
              .mapToPair((SerializablePairFunction<Pair<String, String>, String, String>) t -> t).groupByKey().map(new ValidateRliLocationsWithFiles(hoodieTable)).collectAsList();
      List<String> outOfSyncFileIds = new ArrayList<>();
      outOfSyncFileIdListOfLists.forEach(fileIdList -> {
            if (!fileIdList.isEmpty()) {
              outOfSyncFileIds.addAll(fileIdList);
            }
          }
      );
      if (!outOfSyncFileIds.isEmpty()) {
        throw new HoodieIndexException("There are few FileIDs found in RLI which are not referenced in FILES partition in MDT " + Arrays.toString(outOfSyncFileIds.toArray()));
      }
    }
    return keyAndExistingLocations;
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
   * Assist with validating that all fileIds referenced in RLI is present in FILES partition from MDT.
   */
  private static class ValidateRliLocationsWithFiles implements SerializableFunction<Pair<String, Iterable<String>>, List<String>> {

    private final HoodieTable hoodieTable;

    public ValidateRliLocationsWithFiles(HoodieTable hoodieTable) {
      this.hoodieTable = hoodieTable;
    }

    @Override
    public List<String> apply(Pair<String, Iterable<String>> v1) throws Exception {
      String partitionPath = v1.getKey();
      String absolutePartitionPath = partitionPath.equals(EMPTY_PARTITION_NAME) ? hoodieTable.getMetaClient().getBasePath() : hoodieTable.getMetaClient().getBasePath() + "/" + partitionPath;
      List<String> uniqueFileIdsFromFiles = Arrays.stream(hoodieTable.getMetadataTable().getAllFilesInPartition(new Path(absolutePartitionPath))).map(fileStatus ->
          FSUtils.getFileId(fileStatus.getPath().getName())).distinct().collect(Collectors.toList());
      List<String> outOfSyncFileIds = new ArrayList<>();
      v1.getRight().forEach(rliFileId -> {
        if (!uniqueFileIdsFromFiles.contains(rliFileId)) {
          outOfSyncFileIds.add(rliFileId);
        }
      });
      return outOfSyncFileIds;
    }
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
      return recordIndexInfo.entrySet().stream()
          .map(e -> new Tuple2<>(e.getKey(), e.getValue())).iterator();
    }
  }

  /**
   * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
   * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
   *
   * NOTE: This is a workaround for SPARK-39391, which moved the PartitionIdPassthrough from
   * {@link org.apache.spark.sql.execution.ShuffledRowRDD} to {@link Partitioner}.
   */
  class PartitionIdPassthrough extends Partitioner {

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
