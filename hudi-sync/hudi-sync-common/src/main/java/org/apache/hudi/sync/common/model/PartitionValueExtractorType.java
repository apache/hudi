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

package org.apache.hudi.sync.common.model;

import org.apache.hudi.common.config.EnumDescription;
import org.apache.hudi.common.config.EnumFieldDescription;

@EnumDescription("How to extract partition values from Hudi? from the datasource into Hudi? from a catalog.")
public enum PartitionValueExtractorType {

  @EnumFieldDescription("This implementation extracts datestr=yyyy-mm-dd-HH from path of type /yyyy/mm/dd/HH")
  HOUR_SLASH_ENCODED("org.apache.hudi.hive.SlashEncodedHourPartitionValueExtractor"),

  @EnumFieldDescription("This implementation extracts datestr=yyyy-mm-dd from path of type /yyyy/mm/dd")
  DAY_SLASH_ENCODED("org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor"),

  @EnumFieldDescription("This implementation extracts the partition value from the partition path as a single part "
    + "even if the relative partition path contains slashes.")
  SINGLE_PART("org.apache.hudi.hive.SinglePartPartitionValueExtractor"),

  @EnumFieldDescription("Extractor for Non-partitioned hive tables.")
  NON_PARTITIONED("org.apache.hudi.hive.NonPartitionedExtractor"),

  @EnumFieldDescription("Partition Key extractor treating each value delimited by slash as separate key.")
  MULTI_PART("org.apache.hudi.hive.MultiPartKeysValueExtractor"),

  @EnumFieldDescription("Extractor for Hive Style Partitioned tables, when the partition folders are key value pairs.")
  HIVE_STYLE("org.apache.hudi.hive.HiveStylePartitionValueExtractor"),

  @EnumFieldDescription("Use the partition value extractor set in `hoodie.datasource.hive_sync.partition_extractor_class`")
  CUSTOM("");
  public final String classPath;

  PartitionValueExtractorType(String classPath) {
    this.classPath = classPath;
  }
}
