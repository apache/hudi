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

package org.apache.hudi.index.simple;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.keygen.BaseKeyGenerator;
import org.apache.hudi.table.HoodieTable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

/**
 * A global simple index which reads interested fields(record key and partition path) from base files and
 * joins with incoming records to find the tagged location.
 */
public class HoodieGlobalSimpleIndex extends HoodieSimpleIndex {
  public HoodieGlobalSimpleIndex(HoodieWriteConfig config, Option<BaseKeyGenerator> keyGeneratorOpt) {
    super(config, keyGeneratorOpt);
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable) {
    return tagLocationInternal(records, context, hoodieTable);
  }

  /**
   * Tags records location for incoming records.
   *
   * @param inputRecords {@link HoodieData} of incoming records
   * @param context      instance of {@link HoodieEngineContext} to use
   * @param hoodieTable  instance of {@link HoodieTable} to use
   * @return {@link HoodieData} of records with record locations set
   */
  @Override
  protected <R> HoodieData<HoodieRecord<R>> tagLocationInternal(
      HoodieData<HoodieRecord<R>> inputRecords, HoodieEngineContext context,
      HoodieTable hoodieTable) {

    HoodiePairData<String, HoodieRecord<R>> keyedInputRecords =
        inputRecords.mapToPair(entry -> new ImmutablePair<>(entry.getRecordKey(), entry));
    HoodiePairData<HoodieKey, HoodieRecordLocation> allRecordLocationsInTable =
        fetchAllRecordLocations(context, hoodieTable, config.getGlobalSimpleIndexParallelism());
    return getTaggedRecords(keyedInputRecords, allRecordLocationsInTable);
  }

  /**
   * Fetch record locations for passed in {@link HoodieKey}s.
   *
   * @param context     instance of {@link HoodieEngineContext} to use
   * @param hoodieTable instance of {@link HoodieTable} of interest
   * @param parallelism parallelism to use
   * @return {@link HoodiePairData} of {@link HoodieKey} and {@link HoodieRecordLocation}
   */
  protected HoodiePairData<HoodieKey, HoodieRecordLocation> fetchAllRecordLocations(
      HoodieEngineContext context, HoodieTable hoodieTable, int parallelism) {
    List<Pair<String, HoodieBaseFile>> latestBaseFiles = getAllBaseFilesInTable(context, hoodieTable);
    return fetchRecordLocations(context, hoodieTable, parallelism, latestBaseFiles);
  }

  /**
   * Load all files for all partitions as <Partition, filename> pair data.
   */
  protected List<Pair<String, HoodieBaseFile>> getAllBaseFilesInTable(
      final HoodieEngineContext context, final HoodieTable hoodieTable) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(context, config.getMetadataConfig(), metaClient.getBasePath());
    // Obtain the latest data files from all the partitions.
    return getLatestBaseFilesForAllPartitions(allPartitionPaths, context, hoodieTable);
  }

  /**
   * Tag records with right {@link HoodieRecordLocation}.
   *
   * @param incomingRecords incoming {@link HoodieRecord}s
   * @param existingRecords existing records with {@link HoodieRecordLocation}s
   * @return {@link HoodieData} of {@link HoodieRecord}s with tagged {@link HoodieRecordLocation}s
   */
  private <R> HoodieData<HoodieRecord<R>> getTaggedRecords(
      HoodiePairData<String, HoodieRecord<R>> incomingRecords,
      HoodiePairData<HoodieKey, HoodieRecordLocation> existingRecords) {
    HoodiePairData<String, Pair<String, HoodieRecordLocation>> existingRecordByRecordKey =
        existingRecords.mapToPair(
            entry -> new ImmutablePair<>(entry.getLeft().getRecordKey(),
                Pair.of(entry.getLeft().getPartitionPath(), entry.getRight())));

    return incomingRecords.leftOuterJoin(existingRecordByRecordKey).values()
        .flatMap(entry -> {
          HoodieRecord<R> inputRecord = entry.getLeft();
          Option<Pair<String, HoodieRecordLocation>> partitionPathLocationPair = Option.ofNullable(entry.getRight().orElse(null));
          List<HoodieRecord<R>> taggedRecords;

          if (partitionPathLocationPair.isPresent()) {
            String partitionPath = partitionPathLocationPair.get().getKey();
            HoodieRecordLocation location = partitionPathLocationPair.get().getRight();
            if (config.getGlobalSimpleIndexUpdatePartitionPath() && !(inputRecord.getPartitionPath().equals(partitionPath))) {
              // Create an empty record to delete the record in the old partition
              HoodieRecord<R> deleteRecord = new HoodieAvroRecord(new HoodieKey(inputRecord.getRecordKey(), partitionPath), new EmptyHoodieRecordPayload());
              deleteRecord.setCurrentLocation(location);
              deleteRecord.seal();
              // Tag the incoming record for inserting to the new partition
              HoodieRecord<R> insertRecord = (HoodieRecord<R>) HoodieIndexUtils.getTaggedRecord(inputRecord, Option.empty());
              taggedRecords = Arrays.asList(deleteRecord, insertRecord);
            } else {
              // Ignore the incoming record's partition, regardless of whether it differs from its old partition or not.
              // When it differs, the record will still be updated at its old partition.
              HoodieRecord<R> newRecord = new HoodieAvroRecord(new HoodieKey(inputRecord.getRecordKey(), partitionPath), (HoodieRecordPayload) inputRecord.getData());
              taggedRecords = Collections.singletonList((HoodieRecord<R>) HoodieIndexUtils.getTaggedRecord(newRecord, Option.ofNullable(location)));
            }
          } else {
            taggedRecords = Collections.singletonList((HoodieRecord<R>) HoodieIndexUtils.getTaggedRecord(inputRecord, Option.empty()));
          }
          return taggedRecords.iterator();
        });
  }

  @Override
  public boolean isGlobal() {
    return true;
  }
}
