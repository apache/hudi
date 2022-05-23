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

package org.apache.hudi.snowflake.sync;

import org.apache.hudi.common.config.TypedProperties;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configs needed to sync data into Snowflake.
 */
public class SnowflakeSyncConfig implements Serializable {
  public static String SNOWFLAKE_SYNC_PROPERTIES_FILE = "hoodie.snowflake.sync.properties_file";
  public static String SNOWFLAKE_SYNC_STORAGE_INTEGRATION = "hoodie.snowflake.sync.storage_integration";
  public static String SNOWFLAKE_SYNC_TABLE_NAME = "hoodie.snowflake.sync.table_name";
  public static String SNOWFLAKE_SYNC_SYNC_BASE_PATH = "hoodie.snowflake.sync.base_path";
  public static String SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT = "hoodie.snowflake.sync.base_file_format";
  public static String SNOWFLAKE_SYNC_PARTITION_FIELDS = "hoodie.snowflake.sync.partition_fields";
  public static String SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION = "hoodie.snowflake.sync.partition_extract_expression";
  public static String SNOWFLAKE_SYNC_USE_FILE_LISTING_FROM_METADATA = "hoodie.snowflake.sync.use_file_listing_from_metadata";
  public static String SNOWFLAKE_SYNC_ASSUME_DATE_PARTITIONING = "hoodie.snowflake.sync.assume_date_partitioning";

  @Parameter(names = {"--properties-file"}, description = "name of the snowflake profile properties file.", required = true)
  public String propertiesFile;
  @Parameter(names = {"--storage-integration"}, description = "name of the storage integration in snowflake", required = true)
  public String storageIntegration;
  @Parameter(names = {"--table-name"}, description = "name of the target table in snowflake", required = true)
  public String tableName;
  @Parameter(names = {"--base-path"}, description = "Base path of the hoodie table to sync", required = true)
  public String basePath;
  @Parameter(names = {"--base-file-format"}, description = "Base path of the hoodie table to sync")
  public String baseFileFormat = "PARQUET";
  @Parameter(names = {"--partitioned-by"}, description = "Comma-delimited partition fields. Default to non-partitioned.")
  public List<String> partitionFields = new ArrayList<>();
  @Parameter(names = {"--partition-extract-expr"}, description = "Comma-delimited partition extract expression. Default to non-partitioned.")
  public List<String> partitionExtractExpr = new ArrayList<>();
  @Parameter(names = {"--use-file-listing-from-metadata"}, description = "Fetch file listing from Hudi's metadata")
  public Boolean useFileListingFromMetadata = false;
  @Parameter(names = {"--assume-date-partitioning"}, description = "Assume standard yyyy/mm/dd partitioning, this"
      + " exists to support backward compatibility. If you use hoodie 0.3.x, do not set this parameter")
  public Boolean assumeDatePartitioning = false;
  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public static SnowflakeSyncConfig copy(SnowflakeSyncConfig cfg) {
    SnowflakeSyncConfig newConfig = new SnowflakeSyncConfig();
    newConfig.propertiesFile = cfg.propertiesFile;
    newConfig.storageIntegration = cfg.storageIntegration;
    newConfig.tableName = cfg.tableName;
    newConfig.basePath = cfg.basePath;
    newConfig.baseFileFormat = cfg.baseFileFormat;
    newConfig.partitionFields = cfg.partitionFields;
    newConfig.partitionExtractExpr = cfg.partitionExtractExpr;
    newConfig.useFileListingFromMetadata = cfg.useFileListingFromMetadata;
    newConfig.assumeDatePartitioning = cfg.assumeDatePartitioning;
    newConfig.help = cfg.help;
    return newConfig;
  }

  public TypedProperties toProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(SNOWFLAKE_SYNC_PROPERTIES_FILE, propertiesFile);
    properties.put(SNOWFLAKE_SYNC_STORAGE_INTEGRATION, storageIntegration);
    properties.put(SNOWFLAKE_SYNC_TABLE_NAME, tableName);
    properties.put(SNOWFLAKE_SYNC_SYNC_BASE_PATH, basePath);
    properties.put(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT, baseFileFormat);
    properties.put(SNOWFLAKE_SYNC_PARTITION_FIELDS, String.join(",", partitionFields));
    properties.put(SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION, String.join(",", partitionExtractExpr));
    properties.put(SNOWFLAKE_SYNC_USE_FILE_LISTING_FROM_METADATA, useFileListingFromMetadata);
    properties.put(SNOWFLAKE_SYNC_ASSUME_DATE_PARTITIONING, assumeDatePartitioning);
    return properties;
  }

  public static SnowflakeSyncConfig fromProps(TypedProperties props) {
    SnowflakeSyncConfig config = new SnowflakeSyncConfig();
    config.propertiesFile = props.getString(SNOWFLAKE_SYNC_PROPERTIES_FILE);
    config.storageIntegration = props.getString(SNOWFLAKE_SYNC_STORAGE_INTEGRATION);
    config.tableName = props.getString(SNOWFLAKE_SYNC_TABLE_NAME);
    config.basePath = props.getString(SNOWFLAKE_SYNC_SYNC_BASE_PATH);
    config.baseFileFormat = props.getString(SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT);
    config.partitionFields = props.getStringList(SNOWFLAKE_SYNC_PARTITION_FIELDS, ",", Collections.emptyList());
    config.partitionExtractExpr = props.getStringList(SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION, ",", Collections.emptyList());
    config.useFileListingFromMetadata = props.getBoolean(SNOWFLAKE_SYNC_USE_FILE_LISTING_FROM_METADATA, false);
    config.assumeDatePartitioning = props.getBoolean(SNOWFLAKE_SYNC_ASSUME_DATE_PARTITIONING, false);
    return config;
  }

  @Override
  public String toString() {
    return "SnowflakeSyncConfig{propertiesFile='" + propertiesFile
        + "', storageIntegration'" + storageIntegration
        + "', tableName='" + tableName
        + "', basePath='" + basePath
        + "', baseFileFormat='" + baseFileFormat
        + "', partitionFields='" + partitionFields
        + "', partitionExtractExpr='" + partitionExtractExpr
        + "', useFileListingFromMetadata='" + useFileListingFromMetadata
        + "', assumeDataPartitioning='" + assumeDatePartitioning
        + "', help=" + help + "}";
  }
}
