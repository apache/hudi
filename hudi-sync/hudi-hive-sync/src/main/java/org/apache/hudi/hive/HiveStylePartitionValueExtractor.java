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

package org.apache.hudi.hive;

import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import java.util.Collections;
import java.util.List;

/**
 * Extractor for Hive Style Partitioned tables, when the partition folders are key value pairs.
 *
 * <p>This implementation extracts the partition value of yyyy-mm-dd from the path of type datestr=yyyy-mm-dd.
 */
public class HiveStylePartitionValueExtractor implements PartitionValueExtractor {
  private static final long serialVersionUID = 1L;

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // partition path is expected to be in this format partition_key=partition_value.
    String[] splits = partitionPath.split("=");
    if (splits.length != 2) {
      throw new IllegalArgumentException(
              "Partition path " + partitionPath + " is not in the form partition_key=partition_value.");
    }
    return Collections.singletonList(splits[1]);
  }
}
