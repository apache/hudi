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

import org.apache.hudi.common.config.HoodieMetadataConfig;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configs needed to sync data into Hive.
 */
public class HiveSyncConfig implements Serializable {

  @Parameter(names = {"--database"}, description = "name of the target database in Hive", required = true)
  public String databaseName;

  @Parameter(names = {"--table"}, description = "name of the target table in Hive", required = true)
  public String tableName;

  @Parameter(names = {"--base-file-format"}, description = "Format of the base files (PARQUET (or) HFILE)")
  public String baseFileFormat = "PARQUET";

  @Parameter(names = {"--user"}, description = "Hive username")
  public String hiveUser;

  @Parameter(names = {"--pass"}, description = "Hive password")
  public String hivePass;

  @Parameter(names = {"--jdbc-url"}, description = "Hive jdbc connect url")
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

  @Parameter(names = {"--use-pre-apache-input-format"},
      description = "Use InputFormat under com.uber.hoodie package "
          + "instead of org.apache.hudi package. Use this when you are in the process of migrating from "
          + "com.uber.hoodie to org.apache.hudi. Stop using this after you migrated the table definition to "
          + "org.apache.hudi input format.")
  public Boolean usePreApacheInputFormat = false;

  @Deprecated
  @Parameter(names = {"--use-jdbc"}, description = "Hive jdbc connect url")
  public Boolean useJdbc = true;

  @Parameter(names = {"--sync-mode"}, description = "Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql")
  public String syncMode;

  @Parameter(names = {"--auto-create-database"}, description = "Auto create hive database")
  public Boolean autoCreateDatabase = true;

  @Parameter(names = {"--ignore-exceptions"}, description = "Ignore hive exceptions")
  public Boolean ignoreExceptions = false;

  @Parameter(names = {"--skip-ro-suffix"}, description = "Skip the `_ro` suffix for Read optimized table, when registering")
  public Boolean skipROSuffix = false;

  @Parameter(names = {"--use-file-listing-from-metadata"}, description = "Fetch file listing from Hudi's metadata")
  public Boolean useFileListingFromMetadata = HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;

  @Parameter(names = {"--table-properties"}, description = "Table properties to hive table")
  public String tableProperties;

  @Parameter(names = {"--serde-properties"}, description = "Serde properties to hive table")
  public String serdeProperties;

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  @Parameter(names = {"--support-timestamp"}, description = "'INT64' with original type TIMESTAMP_MICROS is converted to hive 'timestamp' type."
      + "Disabled by default for backward compatibility.")
  public Boolean supportTimestamp = false;

  @Parameter(names = {"--decode-partition"}, description = "Decode the partition value if the partition has encoded during writing")
  public Boolean decodePartition = false;

  @Parameter(names = {"--managed-table"}, description = "Create a managed table")
  public Boolean createManagedTable = false;

  @Parameter(names = {"--batch-sync-num"}, description = "The number of partitions one batch when synchronous partitions to hive")
  public Integer batchSyncNum = 1000;

  @Parameter(names = {"--spark-datasource"}, description = "Whether sync this table as spark data source table.")
  public Boolean syncAsSparkDataSourceTable = true;

  @Parameter(names = {"--spark-schema-length-threshold"}, description = "The maximum length allowed in a single cell when storing additional schema information in Hive's metastore.")
  public int sparkSchemaLengthThreshold = 4000;

  @Parameter(names = {"--with-operation-field"}, description = "Whether to include the '_hoodie_operation' field in the metadata fields")
  public Boolean withOperationField = false;

  // enhance the similar function in child class
  public static HiveSyncConfig copy(HiveSyncConfig cfg) {
    HiveSyncConfig newConfig = new HiveSyncConfig();
    newConfig.basePath = cfg.basePath;
    newConfig.assumeDatePartitioning = cfg.assumeDatePartitioning;
    newConfig.databaseName = cfg.databaseName;
    newConfig.hivePass = cfg.hivePass;
    newConfig.hiveUser = cfg.hiveUser;
    newConfig.partitionFields = cfg.partitionFields;
    newConfig.partitionValueExtractorClass = cfg.partitionValueExtractorClass;
    newConfig.jdbcUrl = cfg.jdbcUrl;
    newConfig.tableName = cfg.tableName;
    newConfig.usePreApacheInputFormat = cfg.usePreApacheInputFormat;
    newConfig.useFileListingFromMetadata = cfg.useFileListingFromMetadata;
    newConfig.supportTimestamp = cfg.supportTimestamp;
    newConfig.decodePartition = cfg.decodePartition;
    newConfig.tableProperties = cfg.tableProperties;
    newConfig.serdeProperties = cfg.serdeProperties;
    newConfig.createManagedTable = cfg.createManagedTable;
    newConfig.batchSyncNum = cfg.batchSyncNum;
    newConfig.syncAsSparkDataSourceTable = cfg.syncAsSparkDataSourceTable;
    newConfig.sparkSchemaLengthThreshold = cfg.sparkSchemaLengthThreshold;
    newConfig.withOperationField = cfg.withOperationField;
    return newConfig;
  }

  @Override
  public String toString() {
    return "HiveSyncConfig{"
      + "databaseName='" + databaseName + '\''
      + ", tableName='" + tableName + '\''
      + ", baseFileFormat='" + baseFileFormat + '\''
      + ", hiveUser='" + hiveUser + '\''
      + ", hivePass='" + hivePass + '\''
      + ", jdbcUrl='" + jdbcUrl + '\''
      + ", basePath='" + basePath + '\''
      + ", partitionFields=" + partitionFields
      + ", partitionValueExtractorClass='" + partitionValueExtractorClass + '\''
      + ", assumeDatePartitioning=" + assumeDatePartitioning
      + ", usePreApacheInputFormat=" + usePreApacheInputFormat
      + ", useJdbc=" + useJdbc
      + ", autoCreateDatabase=" + autoCreateDatabase
      + ", ignoreExceptions=" + ignoreExceptions
      + ", skipROSuffix=" + skipROSuffix
      + ", useFileListingFromMetadata=" + useFileListingFromMetadata
      + ", tableProperties='" + tableProperties + '\''
      + ", serdeProperties='" + serdeProperties + '\''
      + ", help=" + help
      + ", supportTimestamp=" + supportTimestamp
      + ", decodePartition=" + decodePartition
      + ", createManagedTable=" + createManagedTable
      + ", syncAsSparkDataSourceTable=" + syncAsSparkDataSourceTable
      + ", sparkSchemaLengthThreshold=" + sparkSchemaLengthThreshold
      + ", withOperationField=" + withOperationField
      + '}';
  }
}
