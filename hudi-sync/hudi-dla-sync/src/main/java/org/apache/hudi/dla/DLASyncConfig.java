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

package org.apache.hudi.dla;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configs needed to sync data into DLA.
 */
public class DLASyncConfig implements Serializable {

  @Parameter(names = {"--database"}, description = "name of the target database in DLA", required = true)
  public String databaseName;

  @Parameter(names = {"--table"}, description = "name of the target table in DLA", required = true)
  public String tableName;

  @Parameter(names = {"--user"}, description = "DLA username", required = true)
  public String dlaUser;

  @Parameter(names = {"--pass"}, description = "DLA password", required = true)
  public String dlaPass;

  @Parameter(names = {"--jdbc-url"}, description = "DLA jdbc connect url", required = true)
  public String jdbcUrl;

  @Parameter(names = {"--base-path"}, description = "Basepath of hoodie table to sync", required = true)
  public String basePath;

  @Parameter(names = "--partitioned-by", description = "Fields in the schema partitioned by")
  public List<String> partitionFields = new ArrayList<>();

  @Parameter(names = "--partition-value-extractor", description = "Class which implements PartitionValueExtractor "
      + "to extract the partition values from HDFS path")
  public String partitionValueExtractorClass = SlashEncodedDayPartitionValueExtractor.class.getName();

  @Parameter(names = {"--assume-date-partitioning"}, description = "Assume standard yyyy/mm/dd partitioning, this"
      + " exists to support backward compatibility. If you use hoodie 0.3.x, do not set this parameter")
  public Boolean assumeDatePartitioning = false;

  @Parameter(names = {"--skip-ro-suffix"}, description = "Skip the `_ro` suffix for Read optimized table, when registering")
  public Boolean skipROSuffix = false;

  @Parameter(names = {"--skip-rt-sync"}, description = "Skip the RT table syncing")
  public Boolean skipRTSync = false;

  @Parameter(names = {"--hive-style-partitioning"}, description = "Use DLA hive style partitioning, true if like the following style: field1=value1/field2=value2")
  public Boolean useDLASyncHiveStylePartitioning = false;

  @Parameter(names = {"--use-file-listing-from-metadata"}, description = "Fetch file listing from Hudi's metadata")
  public Boolean useFileListingFromMetadata = HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  @Parameter(names = {"--support-timestamp"}, description = "If true, converts int64(timestamp_micros) to timestamp type")
  public Boolean supportTimestamp = false;

  public static DLASyncConfig copy(DLASyncConfig cfg) {
    DLASyncConfig newConfig = new DLASyncConfig();
    newConfig.databaseName = cfg.databaseName;
    newConfig.tableName = cfg.tableName;
    newConfig.dlaUser = cfg.dlaUser;
    newConfig.dlaPass = cfg.dlaPass;
    newConfig.jdbcUrl = cfg.jdbcUrl;
    newConfig.basePath = cfg.basePath;
    newConfig.partitionFields = cfg.partitionFields;
    newConfig.partitionValueExtractorClass = cfg.partitionValueExtractorClass;
    newConfig.assumeDatePartitioning = cfg.assumeDatePartitioning;
    newConfig.skipROSuffix = cfg.skipROSuffix;
    newConfig.skipRTSync = cfg.skipRTSync;
    newConfig.useDLASyncHiveStylePartitioning = cfg.useDLASyncHiveStylePartitioning;
    newConfig.useFileListingFromMetadata = cfg.useFileListingFromMetadata;
    newConfig.supportTimestamp = cfg.supportTimestamp;
    return newConfig;
  }

  @Override
  public String toString() {
    return "DLASyncConfig{databaseName='" + databaseName + '\'' + ", tableName='" + tableName + '\''
        + ", dlaUser='" + dlaUser + '\'' + ", dlaPass='" + dlaPass + '\'' + ", jdbcUrl='" + jdbcUrl + '\''
        + ", basePath='" + basePath + '\'' + ", partitionFields=" + partitionFields + ", partitionValueExtractorClass='"
        + partitionValueExtractorClass + '\'' + ", assumeDatePartitioning=" + assumeDatePartitioning
        + ", useDLASyncHiveStylePartitioning=" + useDLASyncHiveStylePartitioning
        + ", useFileListingFromMetadata=" + useFileListingFromMetadata
        + ", help=" + help + '}';
  }
}
