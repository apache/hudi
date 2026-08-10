/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializer specialization for partitioned tables that dictionary-encodes the partition paths.
 *
 * <p>Serialized bytes can only be deserialized correctly by the same serializer instance that encoded them.
 */
public class CodedRecordGlobalLocationSerializer extends RecordGlobalLocationSerializer {
  private final Map<String, Integer> partitionPathToDictId = new HashMap<>();
  private final List<String> dictIdToPartitionPath = new ArrayList<>();

  @Override
  protected void writePartitionPath(String partitionPath) throws IOException {
    outputSerializer.writeInt(getOrCreatePartitionPathId(partitionPath));
  }

  @Override
  protected String readPartitionPath() throws IOException {
    return getPartitionPath(inputDeserializer.readInt());
  }

  private int getOrCreatePartitionPathId(String partitionPath) {
    Integer existingId = partitionPathToDictId.get(partitionPath);
    if (existingId != null) {
      return existingId;
    }

    int newId = dictIdToPartitionPath.size();
    partitionPathToDictId.put(partitionPath, newId);
    dictIdToPartitionPath.add(partitionPath);
    return newId;
  }

  private String getPartitionPath(int partitionPathId) {
    if (partitionPathId < 0 || partitionPathId >= dictIdToPartitionPath.size()) {
      throw new IllegalStateException("Unknown partition path dictionary id " + partitionPathId
          + ", dictionary size is " + dictIdToPartitionPath.size());
    }
    return dictIdToPartitionPath.get(partitionPathId);
  }
}
