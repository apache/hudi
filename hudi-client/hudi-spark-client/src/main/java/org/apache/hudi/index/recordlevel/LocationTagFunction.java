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

package org.apache.hudi.index.recordlevel;

import org.apache.hudi.avro.model.HoodieMetadataRecordLevelIndex;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;

import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class LocationTagFunction<R extends HoodieRecordPayload> implements FlatMapFunction<Iterator<HoodieRecord<R>>, HoodieRecord<R>> {

  final HoodieTable hoodieTable;
  final HoodieBackedTableMetadata metaTable;
  Map<String, Map<Integer, Pair<String, String>>> partitionPathFileIDList = new HashMap<>();

  public LocationTagFunction(HoodieTable hoodieTable) {
    this.hoodieTable = hoodieTable;
    final HoodieTableMetadata metadata = hoodieTable.getMetadata();
    ValidationUtils.checkState(metadata != null, "meta table should not be null when use record level index.");
    this.metaTable = (HoodieBackedTableMetadata) metadata;
  }

  @Override
  public Iterator<HoodieRecord<R>> call(Iterator<HoodieRecord<R>> hoodieRecordIterator) {
    final HoodieWriteConfig tableConfig = hoodieTable.getConfig();

    //Index data store as hfile, so ordering the rowkey before lookup
    List<HoodieRecord<R>> taggedRecords = new ArrayList<>();
    Map<String, Integer> keyToIndexMap = new HashMap<>(taggedRecords.size());

    while (hoodieRecordIterator.hasNext()) {
      HoodieRecord<R> nextRecord = hoodieRecordIterator.next();
      String recordKey = nextRecord.getRecordKey();
      //de-duplicate records, when use record level index can set hoodie.combine.before.*=false
      if (keyToIndexMap.containsKey(recordKey)) {
        Integer index = keyToIndexMap.get(recordKey);
        HoodieRecord<R> preRecord = taggedRecords.get(index);
        final R nexData = nextRecord.getData();
        final R preData = preRecord.getData();
        // combine
        final R payload = (R) preData.preCombine(nexData, null);
        HoodieRecord<R> retainRecord = preRecord.getData().equals(payload) ? preRecord : nextRecord;
        // reset
        taggedRecords.set(index, retainRecord);
      } else {
        taggedRecords.add(nextRecord);
        keyToIndexMap.put(recordKey, taggedRecords.size() - 1);
      }
    }

    if (taggedRecords.size() > 0) {
      List<String> lookUpKeys = new ArrayList<>(keyToIndexMap.keySet());

      //lookup index with keys
      final List<Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>>> taggedResults = metaTable.getRecordsByKeys(
          lookUpKeys,
          MetadataPartitionType.RECORD_LEVEL_INDEX.getPartitionPath());

      for (Pair<String, Option<HoodieRecord<HoodieMetadataPayload>>> keyAndRecord : taggedResults) {

        Option<HoodieRecord<HoodieMetadataPayload>> indexValue = keyAndRecord.getValue();

        //index not found or deleted
        if (!indexValue.isPresent()) {
          continue;
        }

        final Option<HoodieMetadataRecordLevelIndex> recordLevelIndexMetadata = indexValue.get().getData().getRecordLevelIndexMetadata();
        if (!recordLevelIndexMetadata.isPresent()) {
          continue;
        }

        final HoodieMetadataRecordLevelIndex hoodieMetadataRecordLevelIndex = recordLevelIndexMetadata.get();
        if (hoodieMetadataRecordLevelIndex.getIsDeleted()) {
          continue;
        }


        final String oldPartition = hoodieMetadataRecordLevelIndex.getPartition();
        final String oldFileId = hoodieMetadataRecordLevelIndex.getFileId();
        final String oldCommitTime = hoodieMetadataRecordLevelIndex.getCommitTime();
        final Integer oldRowGroupIndex = hoodieMetadataRecordLevelIndex.getRowGroupIndex();

        String key = keyAndRecord.getKey();

        HoodieRecordGlobalLocation taggedLocation = new HoodieRecordGlobalLocation(oldPartition, oldCommitTime, oldFileId);
        // taggedLocation.setRowGroupId(oldRowGroupIndex);


        Option<String> layoutPartitionerClass = hoodieTable.getStorageLayout().layoutPartitionerClass();

        //If fileId named whit bucket index style
        if (layoutPartitionerClass.isPresent() && layoutPartitionerClass.get().equals(SparkBucketIndexPartitioner.class.getName())) {
          final int bucketId;
          if (oldFileId.contains("-")) {
            bucketId = Integer.parseInt(oldFileId.split("-")[0]);
          } else {
            bucketId = Integer.parseInt(oldFileId);
          }
          if (!partitionPathFileIDList.containsKey(oldPartition)) {
            partitionPathFileIDList.put(oldPartition, loadPartitionBucketIdFileIdMapping(hoodieTable, oldPartition));
          }

          if (partitionPathFileIDList.get(oldPartition).containsKey(bucketId)) {
            Pair<String, String> fileInfo = partitionPathFileIDList.get(oldPartition).get(bucketId);
            taggedLocation = new HoodieRecordGlobalLocation(oldPartition, taggedLocation.getInstantTime(), fileInfo.getLeft());
            //taggedLocation.setRowGroupId(taggedLocation.getRowGroupId());
          } else {
            new IllegalStateException("bucketId " + bucketId + " not found in partition " + oldPartition);
          }
        }

        //get record to update
        Integer index = keyToIndexMap.get(key);
        HoodieRecord<R> toUpdateRecord = taggedRecords.get(index);
        String toUpdatePartition = toUpdateRecord.getPartitionPath();

        if (!tableConfig.shouldUpdatePartition() || oldPartition.equals(toUpdatePartition)) {
          HoodieKey hoodieKey = new HoodieKey(toUpdateRecord.getRecordKey(), oldPartition);
          toUpdateRecord = new HoodieAvroRecord(hoodieKey, toUpdateRecord.getData());
          toUpdateRecord.unseal();
          toUpdateRecord.setCurrentLocation(taggedLocation);
          toUpdateRecord.seal();
          taggedRecords.set(index, toUpdateRecord);
        } else {
          // Partition changed, add delete record to old fileGroup
          HoodieRecord deleteRecord = new HoodieAvroRecord(new HoodieKey(toUpdateRecord.getRecordKey(), oldPartition),
              new EmptyHoodieRecordPayload(), HoodieOperation.UPDATE_BEFORE);
          deleteRecord.unseal();
          deleteRecord.setCurrentLocation(taggedLocation);
          deleteRecord.seal();
          taggedRecords.add(deleteRecord);
        }
      }
    }
    keyToIndexMap.clear();
    partitionPathFileIDList.clear();
    return taggedRecords.iterator();
  }

  private Map<Integer, Pair<String, String>> loadPartitionBucketIdFileIdMapping(
      HoodieTable hoodieTable,
      String partition) {
    // bucketId -> fileIds
    Map<Integer, Pair<String, String>> fileIDList = new HashMap<>();
    HoodieIndexUtils
        .getLatestBaseFilesForPartition(partition, hoodieTable)
        .forEach(file -> {
          String fileId = file.getFileId();
          String commitTime = file.getCommitTime();
          int bucketId = BucketIdentifier.bucketIdFromFileId(fileId);
          if (!fileIDList.containsKey(bucketId)) {
            fileIDList.put(bucketId, Pair.of(fileId, commitTime));
          } else {
            // check if bucket data is valid
            throw new HoodieIOException("Find multiple files at partition path="
                + partition + " belongs to the same bucket id = " + bucketId);
          }
        });
    return fileIDList;
  }
}