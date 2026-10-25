/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

public class HoodieTableConfigUtils {
  /**
   * Infers the appropriate PartitionValueExtractor class based on table configuration.
   * This function determines the correct extractor based on the number of partition fields
   * and partitioning style (Hive-style vs non-Hive-style).
   *
   * @param cfg HoodieConfig containing table configuration
   * @return Option containing the inferred PartitionValueExtractor class name
   */
  public static Option<String> inferPartitionValueExtractorClass(HoodieConfig cfg) {
    Option<String> partitionFieldsOpt = HoodieTableConfig.getPartitionFieldProp(cfg)
        .or(() -> Option.ofNullable(cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME)));
    if (!partitionFieldsOpt.isPresent()) {
      return Option.empty();
    }

    String partitionFields = partitionFieldsOpt.get();
    if (StringUtils.nonEmpty(partitionFields)) {
      int numOfPartFields = partitionFields.split(",").length;
      if (numOfPartFields == 1) {
        if (cfg.contains(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key())
            && cfg.getString(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key()).equals("true")) {
          return Option.of("org.apache.hudi.hive.HiveStylePartitionValueExtractor");
        } else if (cfg.contains(KeyGeneratorOptions.SLASH_SEPARATED_DATE_PARTITIONING)
            && cfg.getString(KeyGeneratorOptions.SLASH_SEPARATED_DATE_PARTITIONING).equals("true")) {
          return Option.of("org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor");
        } else {
          return Option.of("org.apache.hudi.hive.SinglePartPartitionValueExtractor");
        }
      } else {
        return Option.of("org.apache.hudi.hive.MultiPartKeysValueExtractor");
      }
    } else {
      return Option.of("org.apache.hudi.hive.NonPartitionedExtractor");
    }
  }
}
