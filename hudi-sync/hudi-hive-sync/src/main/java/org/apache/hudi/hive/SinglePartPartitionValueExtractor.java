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

package org.apache.hudi.hive;

import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import java.util.Collections;
import java.util.List;

/**
 * Extractor for a partition path from a single column.
 * <p>
 * This implementation extracts the partition value from the partition path as a single part
 * even if the relative partition path contains slashes, e.g., the `TimestampBasedKeyGenerator`
 * transforms the timestamp column into the partition path in the format of "yyyyMM/dd/HH".
 * The slash (`/`) is replaced with dash (`-`), e.g., `202210/01/20` -> `202210-01-20`.
 */
public class SinglePartPartitionValueExtractor implements PartitionValueExtractor {
  @Override
  public List<String> extractPartitionValuesInPath(String partitionPath) {
    return Collections.singletonList(partitionPath.replace('/', '-'));
  }
}
