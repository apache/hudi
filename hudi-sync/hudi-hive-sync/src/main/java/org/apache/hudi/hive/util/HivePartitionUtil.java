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
import org.apache.hudi.hive.PartitionValueExtractor;

import java.util.ArrayList;
import java.util.List;

public class HivePartitionUtil {

  /**
   * Build String, example as year=2021/month=06/day=25
   */
  public static String getPartitionClauseForDrop(String partition, PartitionValueExtractor partitionValueExtractor, HiveSyncConfig config) {
    List<String> partitionValues = partitionValueExtractor.extractPartitionValuesInPath(partition);
    ValidationUtils.checkArgument(config.partitionFields.size() == partitionValues.size(),
        "Partition key parts " + config.partitionFields + " does not match with partition values " + partitionValues
            + ". Check partition strategy. ");
    List<String> partBuilder = new ArrayList<>();
    for (int i = 0; i < config.partitionFields.size(); i++) {
      String partitionValue = partitionValues.get(i);
      // decode the partition before sync to hive to prevent multiple escapes of HIVE
      if (config.decodePartition) {
        // This is a decode operator for encode in KeyGenUtils#getRecordPartitionPath
        partitionValue = PartitionPathEncodeUtils.unescapePathName(partitionValue);
      }
      partBuilder.add(config.partitionFields.get(i) + "=" + partitionValue);
    }
    return String.join("/", partBuilder);
  }
}
