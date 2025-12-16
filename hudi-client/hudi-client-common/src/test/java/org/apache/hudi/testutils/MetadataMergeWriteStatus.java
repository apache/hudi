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

package org.apache.hudi.testutils;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;

import lombok.AccessLevel;
import lombok.Getter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A custom {@link WriteStatus} that merges passed metadata key value map to {@code WriteStatus.markSuccess()} and
 * {@code WriteStatus.markFailure()}.
 */
public class MetadataMergeWriteStatus extends WriteStatus {

  @Getter(AccessLevel.PRIVATE)
  private Map<String, String> mergedMetadataMap = new HashMap<>();

  public MetadataMergeWriteStatus(Boolean trackSuccessRecords, Double failureFraction) {
    super(trackSuccessRecords, failureFraction);
  }

  public MetadataMergeWriteStatus(Boolean trackSuccessRecords, Double failureFraction,
                                  Boolean isMetadataTable) {
    super(trackSuccessRecords, failureFraction, isMetadataTable);
  }

  public static Map<String, String> mergeMetadataForWriteStatuses(List<WriteStatus> writeStatuses) {
    Map<String, String> allWriteStatusMergedMetadataMap = new HashMap<>();
    for (WriteStatus writeStatus : writeStatuses) {
      MetadataMergeWriteStatus.mergeMetadataMaps(((MetadataMergeWriteStatus) writeStatus).getMergedMetadataMap(),
          allWriteStatusMergedMetadataMap);
    }
    return allWriteStatusMergedMetadataMap;
  }

  private static void mergeMetadataMaps(Map<String, String> mergeFromMap, Map<String, String> mergeToMap) {
    for (Entry<String, String> entry : mergeFromMap.entrySet()) {
      String key = entry.getKey();
      if (!mergeToMap.containsKey(key)) {
        mergeToMap.put(key, "0");
      }
      mergeToMap.put(key, addStrsAsLong(entry.getValue(), mergeToMap.get(key)));
    }
  }

  private static String addStrsAsLong(String a, String b) {
    return String.valueOf(Long.parseLong(a) + Long.parseLong(b));
  }

  @Override
  public void markSuccess(HoodieRecord record, Option<Map<String, String>> recordMetadata) {
    super.markSuccess(record, recordMetadata);
    if (recordMetadata.isPresent()) {
      mergeMetadataMaps(recordMetadata.get(), mergedMetadataMap);
    }
  }

  @Override
  public void markFailure(HoodieRecord record, Throwable t, Option<Map<String, String>> recordMetadata) {
    super.markFailure(record, t, recordMetadata);
    if (recordMetadata.isPresent()) {
      mergeMetadataMaps(recordMetadata.get(), mergedMetadataMap);
    }
  }
}
