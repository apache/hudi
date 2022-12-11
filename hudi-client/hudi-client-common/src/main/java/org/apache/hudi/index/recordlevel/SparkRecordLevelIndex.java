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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.index.bucket.BucketIdentifier;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SparkBucketIndexPartitioner;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.execution.PartitionIdPassthrough;

import java.text.ParseException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class SparkRecordLevelIndex extends HoodieIndex<Object, Object> {
  private static final Logger LOG = LogManager.getLogger(SparkRecordLevelIndex.class);

  public SparkRecordLevelIndex(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains
   * the row (if it is actually present).
   *
   * @param records
   * @param context
   * @param hoodieTable
   */
  @Override
  public <R> HoodieData<HoodieRecord<R>> tagLocation(HoodieData<HoodieRecord<R>> records, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    final int numShards = config.getRecordLevelIndexShardCount();
    final JavaRDD<HoodieRecord<R>> recordJavaRDD = HoodieJavaRDD.getJavaRDD(records);
    int numPartitions = recordJavaRDD.getNumPartitions();
    LOG.info(String.format("tag location records num partitions %s shards %s", numPartitions, numShards));

    JavaRDD<HoodieRecord<R>> y = recordJavaRDD.keyBy(r -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(r.getRecordKey(), numShards))
        .partitionBy(new PartitionIdPassthrough(numShards))
        .map(t -> t._2);

    ValidationUtils.checkState(y.getNumPartitions() <= numShards);
    JavaRDD<HoodieRecord<R>> hoodieRecordJavaRDD = y.mapPartitions(new LocationTagFunction(hoodieTable));

    return HoodieJavaRDD.of(hoodieRecordJavaRDD);
  }

  public static boolean isColdData(String partition, Integer liveTime) {
    try {
      int containEq = partition.indexOf("=");
      partition = partition.substring(containEq + 1);
      Date date = org.apache.commons.lang3.time.DateUtils.parseDate(partition, new String[] {"yyyyMMdd", "yyyy-MM-dd", "yyyy/MM/dd", "yyyyMMddHH"});
      return System.currentTimeMillis() - date.getTime() > liveTime * 24 * 60 * 60 * 1000L;
    } catch (ParseException e) {
      LOG.warn("parse error", e);
    }

    return false;
  }

  /**
   * Extracts the location of written records, and updates the index.
   *
   * @param writeStatuses
   * @param context
   * @param hoodieTable
   */
  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses, HoodieEngineContext context, HoodieTable hoodieTable) throws HoodieIndexException {
    return writeStatuses;
  }

  public HoodieData<HoodieRecord> updateLocationToMetadata(JavaRDD<WriteStatus> writeStatusRDD, HoodieTable hoodieTable) {
    Integer liveTime = config.tableIndexTTL();

    long[] counts = {0L, 0L, 0L}; // insert, update, delete
    JavaRDD<HoodieRecord> indexUpdateRDD = writeStatusRDD.flatMap(writeStatus -> {
      List<HoodieRecord> records = new LinkedList<>();
      for (HoodieRecord writtenRecord : writeStatus.getWrittenRecords()) {
        if (!writeStatus.isErrored(writtenRecord.getKey())) {
          HoodieRecord indexRecord = null;
          HoodieKey key = writtenRecord.getKey();
          Option<HoodieRecordLocation> newLocation = writtenRecord.getNewLocation();
          final HoodieRecordLocation currentLocation = writtenRecord.getCurrentLocation();
          HoodieOperation operation = writtenRecord.getOperation();

          String partitionPath = key.getPartitionPath();
          // ttl
          if (liveTime > 0 && partitionPath != null && isColdData(partitionPath, liveTime)) {
            continue;
          }

          if (HoodieOperation.isUpdateBefore(operation) || HoodieOperation.isDelete(operation)) {
            // Delete
            counts[2] += 1;
            // Delete existing index for a deleted record
            if (!HoodieOperation.isUpdateBefore(operation)) {
              indexRecord = HoodieMetadataPayload.createRecordLevelIndexRecord(key.getRecordKey(), partitionPath,
                  currentLocation.getFileId(), currentLocation.getRowGroupId(), true, currentLocation.getInstantTime(), HoodieOperation.UPDATE_BEFORE);
            }
          } else if (currentLocation != null || HoodieOperation.isUpdateAfter(operation)) {
            // Update
            counts[1] += 1;
            // Location not change needn't update index
            continue;
          } else {
            // Insert
            counts[0] += 1;
            HoodieRecordLocation hoodieRecordLocation = newLocation.get();
            // Data file names have a -D suffix to denote the index (D = integer) of the file written
            String fileId = hoodieRecordLocation.getFileId();
            final int index = fileId.lastIndexOf("-");
            int fileIndex = Integer.parseInt(fileId.substring(index + 1));
            String uuid = fileId.substring(0, index);
            Option<String> layoutPartitionerClass = hoodieTable.getStorageLayout().layoutPartitionerClass();

            //If fileId named whit bucket index style
            if (layoutPartitionerClass.isPresent() && layoutPartitionerClass.get().equals(SparkBucketIndexPartitioner.class.getName())) {
              fileId = String.valueOf(BucketIdentifier.bucketIdFromFileId(fileId));
            }

            Integer rowGroupId = hoodieRecordLocation.getRowGroupId();
            if (rowGroupId == null) {
              rowGroupId = -1;
            }
            indexRecord = HoodieMetadataPayload.createRecordLevelIndexRecord(key.getRecordKey(), partitionPath,
                fileId, rowGroupId, false, hoodieRecordLocation.getInstantTime(), HoodieOperation.INSERT);
          }
          if (indexRecord != null) {
            records.add(indexRecord);
          }
        }
      }
      return records.iterator();
    });
    return HoodieJavaRDD.of(indexUpdateRDD);
  }

  /**
   * Rollback the effects of the commit made at instantTime.
   *
   * @param instantTime
   */
  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  /**
   * An index is `global` if {@link HoodieKey} to fileID mapping, does not depend on the `partitionPath`. Such an
   * implementation is able to obtain the same mapping, for two hoodie keys with same `recordKey` but different
   * `partitionPath`
   *
   * @return whether or not, the index implementation is global in nature
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * This is used by storage to determine, if its safe to send inserts, straight to the log, i.e having a
   * {@link FileSlice}, with no data file.
   *
   * @return Returns true/false depending on whether the impl has this capability
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * An index is "implicit" with respect to storage, if just writing new data to a file slice, updates the index as
   * well. This is used by storage, to save memory footprint in certain cases.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }
}
