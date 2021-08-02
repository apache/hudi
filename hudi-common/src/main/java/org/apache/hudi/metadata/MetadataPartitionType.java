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

package org.apache.hudi.metadata;

import java.util.Arrays;
import java.util.List;

public enum MetadataPartitionType {
  FILES("files"),
  RECORD_LEVEL_INDEX("record_index");

  private final String partitionPath;

  MetadataPartitionType(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public String partitionPath() {
    return partitionPath;
  }

  public static List<String> all() {
    return Arrays.asList(MetadataPartitionType.FILES.partitionPath(), MetadataPartitionType.RECORD_LEVEL_INDEX.partitionPath());
  }
}
