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

package org.apache.hudi.index.bucket;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.utils.LazyIterableIterator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Hash indexing mechanism.
 */
public class HoodieBucketIndex extends HoodieIndex<Object, Object> {

  private static final Logger LOG =  LogManager.getLogger(HoodieBucketIndex.class);

  private final int numBuckets;

  public HoodieBucketIndex(HoodieWriteConfig config) {
    super(config);
    numBuckets = config.getBucketIndexNumBuckets();
    LOG.info("use bucket index, numBuckets=" + numBuckets);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses,
      HoodieEngineContext context,
      HoodieTable hoodieTable)
      throws HoodieIndexException {
    return writeStatuses;
  }

  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(
      HoodieData<HoodieRecord<R>> records, HoodieEngineContext context,
      HoodieTable hoodieTable)
      throws HoodieIndexException {
    HoodieData<HoodieRecord<R>> taggedRecords = records.mapPartitions(recordIter -> {
      // partitionPath -> bucketId -> fileInfo
      Map<String, Map<Integer, Pair<String, String>>> partitionPathFileIDList = new HashMap<>();
      return new LazyIterableIterator<HoodieRecord<R>, HoodieRecord<R>>(recordIter) {

        @Override
        protected void start() {

        }

        @Override
        protected HoodieRecord<R> computeNext() {
          HoodieRecord record = recordIter.next();
          int bucketId = BucketIdentifier.getBucketId(record, config.getBucketIndexHashField(), numBuckets);
          String partitionPath = record.getPartitionPath();
          if (!partitionPathFileIDList.containsKey(partitionPath)) {
            partitionPathFileIDList.put(partitionPath, loadPartitionBucketIdFileIdMapping(hoodieTable, partitionPath));
          }
          if (partitionPathFileIDList.get(partitionPath).containsKey(bucketId)) {
            Pair<String, String> fileInfo = partitionPathFileIDList.get(partitionPath).get(bucketId);
            return HoodieIndexUtils.getTaggedRecord(record, Option.of(
                new HoodieRecordLocation(fileInfo.getRight(), fileInfo.getLeft())
            ));
          }
          return record;
        }

        @Override
        protected void end() {

        }
      };
    }, true);
    return taggedRecords;
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

  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }

  @Override
  public boolean requiresTagging(WriteOperationType operationType) {
    switch (operationType) {
      case INSERT:
      case INSERT_OVERWRITE:
      case UPSERT:
        return true;
      default:
        return false;
    }
  }

  public int getNumBuckets() {
    return numBuckets;
  }
}
