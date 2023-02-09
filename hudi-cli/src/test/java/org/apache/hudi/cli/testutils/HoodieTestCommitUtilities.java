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

package org.apache.hudi.cli.testutils;

import org.apache.hudi.avro.model.HoodieWriteStat;
import org.apache.hudi.client.utils.MetadataConversionUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;

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
  public static org.apache.hudi.avro.model.HoodieCommitMetadata convertAndOrderCommitMetadata(
      HoodieCommitMetadata hoodieCommitMetadata) {
    return orderCommitMetadata(MetadataConversionUtils.convertCommitMetadata(hoodieCommitMetadata));
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
