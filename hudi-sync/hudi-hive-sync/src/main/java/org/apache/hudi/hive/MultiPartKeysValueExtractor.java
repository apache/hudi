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

import java.util.Collections;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Partition Key extractor treating each value delimited by slash as separate key.
 */
public class MultiPartKeysValueExtractor implements PartitionValueExtractor {

  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    // If the partitionPath is empty string( which means none-partition table), the partition values
    // should be empty list.
    if (partitionPath.isEmpty()) {
      return Collections.emptyList();
    }
    String[] splits = partitionPath.split("/");
    return Arrays.stream(splits).map(s -> {
      if (s.contains("=")) {
        String[] moreSplit = s.split("=");
        ValidationUtils.checkArgument(moreSplit.length == 2, "Partition Field (" + s + ") not in expected format");
        return moreSplit[1];
      }
      return s;
    }).collect(Collectors.toList());
  }
}
