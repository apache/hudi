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

package org.apache.hudi.utilities.mongo;

import java.util.Collections;
import java.util.List;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.hive.PartitionValueExtractor;

/**
 * HDFS or S3 file paths contain hive partition values for the keys it is partitioned on. This class
 * extracts the partition value from the file path.
 */
public class MongoHashPartitionValueExtractor implements PartitionValueExtractor {

  public MongoHashPartitionValueExtractor() {
  }

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // Partition path is not expected to contain "/"
    if (partitionPath.contains("/")) {
      throw new HoodieKeyException(
          "Mongo hash partition path " + partitionPath + " should not contain backslash");
    }
    // Get the partition part
    String[] splits = partitionPath.split("=");
    String shard = (splits.length > 1) ? splits[1] : splits[0];
    return Collections.singletonList(shard);
  }
}
