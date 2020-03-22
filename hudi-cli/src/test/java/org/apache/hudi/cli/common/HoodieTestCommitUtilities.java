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

package org.apache.hudi.cli.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hudi.avro.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods to commit instant for test.
 */
public class HoodieTestCommitUtilities {

  /**
   * Converter HoodieCommitMetadata to avro format and ordered by partition.
   */
  public static org.apache.hudi.avro.model.HoodieCommitMetadata commitMetadataConverterOrdered(
      HoodieCommitMetadata hoodieCommitMetadata) {
    return orderCommitMetadata(commitMetadataConverter(hoodieCommitMetadata));
  }

  /**
   * Converter HoodieCommitMetadata to avro format.
   */
  public static org.apache.hudi.avro.model.HoodieCommitMetadata commitMetadataConverter(
      HoodieCommitMetadata hoodieCommitMetadata) {
    ObjectMapper mapper = new ObjectMapper();
    // Need this to ignore other public get() methods
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    org.apache.hudi.avro.model.HoodieCommitMetadata avroMetaData =
        mapper.convertValue(hoodieCommitMetadata, org.apache.hudi.avro.model.HoodieCommitMetadata.class);
    // Do not archive Rolling Stats, cannot set to null since AVRO will throw null pointer
    avroMetaData.getExtraMetadata().put(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY, "");
    return avroMetaData;
  }

  /**
   * Ordered by partition asc.
   */
  public static org.apache.hudi.avro.model.HoodieCommitMetadata orderCommitMetadata(
      org.apache.hudi.avro.model.HoodieCommitMetadata hoodieCommitMetadata) {
    Map<String, List<HoodieWriteStat>> result = new LinkedHashMap<>();
    hoodieCommitMetadata.getPartitionToWriteStats().entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEachOrdered(e -> result.put(e.getKey(), e.getValue()));
    hoodieCommitMetadata.setPartitionToWriteStats(result);
    return hoodieCommitMetadata;
  }
}
