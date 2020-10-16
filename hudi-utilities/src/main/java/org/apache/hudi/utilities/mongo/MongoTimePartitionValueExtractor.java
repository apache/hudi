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

import java.util.ArrayList;
import java.util.List;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.hive.PartitionValueExtractor;

/**
 * HDFS or S3 file paths contain hive partition values for the keys it is partitioned on. This class
 * extracts the partition value from the file path.
 */
public class MongoTimePartitionValueExtractor implements PartitionValueExtractor {

  public MongoTimePartitionValueExtractor() {
  }

  static private String partValue(String origVal) {
    String[] parts = origVal.split("=");
    return (parts.length > 1) ? parts[1] : parts[0];
  }

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // Partition path is expected to contain "/"
    String[] splits = partitionPath.split("/");
    if (splits.length != 2 && splits.length != 1) {
      throw new HoodieKeyException(
          "Mongo hourly partition path " + partitionPath + " should be yyyy-mm-dd or yyyy-mm-dd/HH");
    }
    // Get the partition key values
    ArrayList<String> values = new ArrayList<>();
    values.add(partValue(splits[0]));

    if (splits.length == 2) {
      values.add(partValue(splits[1]));
    }
    return values;
  }
}
