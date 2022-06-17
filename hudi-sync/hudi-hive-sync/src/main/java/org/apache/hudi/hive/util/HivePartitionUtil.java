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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.util.PartitionPathEncodeUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.PartitionValueExtractor;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DECODE_PARTITION;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;

public class HivePartitionUtil {
  private static final Logger LOG = LogManager.getLogger(HivePartitionUtil.class);

  /**
   * Build String, example as year=2021/month=06/day=25
   */
  public static String getPartitionClauseForDrop(String partition, PartitionValueExtractor partitionValueExtractor, HiveSyncConfig config) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(config.getSplitStrings(META_SYNC_PARTITION_FIELDS).size() == partitionValues.size(),
        "Partition key parts " + config.getSplitStrings(META_SYNC_PARTITION_FIELDS) + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < config.getSplitStrings(META_SYNC_PARTITION_FIELDS).size(); i++) {
      String partitionValue = partitionValues.get(i);
      // decode the partition before sync to hive to prevent multiple escapes of HIVE
      if (config.getBoolean(META_SYNC_DECODE_PARTITION)) {
        // This is a decode operator for encode in KeyGenUtils#getRecordPartitionPath
        partitionValue = PartitionPathEncodeUtils.unescapePathName(partitionValue);
      }
      partBuilder.add(config.getSplitStrings(META_SYNC_PARTITION_FIELDS).get(i) + "=" + partitionValue);
    }
    return String.join("/", partBuilder);
  }

  public static Boolean partitionExists(IMetaStoreClient client, String tableName, String partitionPath,
                                        PartitionValueExtractor partitionValueExtractor, HiveSyncConfig config) {
    Partition newPartition;
    try {
      List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partitionPath);
      newPartition = client.getPartition(config.getString(META_SYNC_DATABASE_NAME), tableName, partitionValues);
    } catch (NoSuchObjectException ignored) {
      newPartition = null;
    } catch (TException e) {
      LOG.error("Failed to get partition " + partitionPath, e);
      throw new HoodieHiveSyncException("Failed to get partition " + partitionPath, e);
    }
    return newPartition != null;
  }
}
