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

package org.apache.hudi.gcp.bigquery;

import org.apache.hudi.common.config.TypedProperties;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Configs needed to sync data into BigQuery.
 */
public class BigQuerySyncConfig implements Serializable {

  public static String BIGQUERY_SYNC_PROJECT_ID = "hoodie.gcp.bigquery.sync.project_id";
  public static String BIGQUERY_SYNC_DATASET_NAME = "hoodie.gcp.bigquery.sync.dataset_name";
  public static String BIGQUERY_SYNC_DATASET_LOCATION = "hoodie.gcp.bigquery.sync.dataset_location";
  public static String BIGQUERY_SYNC_TABLE_NAME = "hoodie.gcp.bigquery.sync.table_name";
  public static String BIGQUERY_SYNC_SOURCE_URI = "hoodie.gcp.bigquery.sync.source_uri";
  public static String BIGQUERY_SYNC_SOURCE_URI_PREFIX = "hoodie.gcp.bigquery.sync.source_uri_prefix";
  public static String BIGQUERY_SYNC_SYNC_BASE_PATH = "hoodie.gcp.bigquery.sync.base_path";
  public static String BIGQUERY_SYNC_PARTITION_FIELDS = "hoodie.gcp.bigquery.sync.partition_fields";
  public static String BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA = "hoodie.gcp.bigquery.sync.use_file_listing_from_metadata";
  public static String BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING = "hoodie.gcp.bigquery.sync.assume_date_partitioning";

  @Parameter(names = {"--project-id"}, description = "name of the target project in BigQuery", required = true)
  public String projectId;
  @Parameter(names = {"--dataset-name"}, description = "name of the target dataset in BigQuery", required = true)
  public String datasetName;
  @Parameter(names = {"--dataset-location"}, description = "location of the target dataset in BigQuery", required = true)
  public String datasetLocation;
  @Parameter(names = {"--table-name"}, description = "name of the target table in BigQuery", required = true)
  public String tableName;
  @Parameter(names = {"--source-uri"}, description = "name of the source uri gcs path of the table", required = true)
  public String sourceUri;
  @Parameter(names = {"--source-uri-prefix"}, description = "name of the source uri gcs path prefix of the table", required = true)
  public String sourceUriPrefix;
  @Parameter(names = {"--base-path"}, description = "Base path of the hoodie table to sync", required = true)
  public String basePath;
  @Parameter(names = {"--partitioned-by"}, description = "Comma-delimited partition fields. Default to non-partitioned.")
  public List<String> partitionFields = new ArrayList<>();
  @Parameter(names = {"--use-file-listing-from-metadata"}, description = "Fetch file listing from Hudi's metadata")
  public Boolean useFileListingFromMetadata = false;
  @Parameter(names = {"--assume-date-partitioning"}, description = "Assume standard yyyy/mm/dd partitioning, this"
      + " exists to support backward compatibility. If you use hoodie 0.3.x, do not set this parameter")
  public Boolean assumeDatePartitioning = false;
  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;

  public static BigQuerySyncConfig copy(BigQuerySyncConfig cfg) {
    BigQuerySyncConfig newConfig = new BigQuerySyncConfig();
    newConfig.projectId = cfg.projectId;
    newConfig.datasetName = cfg.datasetName;
    newConfig.datasetLocation = cfg.datasetLocation;
    newConfig.tableName = cfg.tableName;
    newConfig.sourceUri = cfg.sourceUri;
    newConfig.sourceUriPrefix = cfg.sourceUriPrefix;
    newConfig.basePath = cfg.basePath;
    newConfig.partitionFields = cfg.partitionFields;
    newConfig.useFileListingFromMetadata = cfg.useFileListingFromMetadata;
    newConfig.assumeDatePartitioning = cfg.assumeDatePartitioning;
    newConfig.help = cfg.help;
    return newConfig;
  }

  public TypedProperties toProps() {
    TypedProperties properties = new TypedProperties();
    properties.put(BIGQUERY_SYNC_PROJECT_ID, projectId);
    properties.put(BIGQUERY_SYNC_DATASET_NAME, datasetName);
    properties.put(BIGQUERY_SYNC_DATASET_LOCATION, datasetLocation);
    properties.put(BIGQUERY_SYNC_TABLE_NAME, tableName);
    properties.put(BIGQUERY_SYNC_SOURCE_URI, sourceUri);
    properties.put(BIGQUERY_SYNC_SOURCE_URI_PREFIX, sourceUriPrefix);
    properties.put(BIGQUERY_SYNC_SYNC_BASE_PATH, basePath);
    properties.put(BIGQUERY_SYNC_PARTITION_FIELDS, String.join(",", partitionFields));
    properties.put(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA, useFileListingFromMetadata);
    properties.put(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING, assumeDatePartitioning);
    return properties;
  }

  public static BigQuerySyncConfig fromProps(TypedProperties props) {
    BigQuerySyncConfig config = new BigQuerySyncConfig();
    config.projectId = props.getString(BIGQUERY_SYNC_PROJECT_ID);
    config.datasetName = props.getString(BIGQUERY_SYNC_DATASET_NAME);
    config.datasetLocation = props.getString(BIGQUERY_SYNC_DATASET_LOCATION);
    config.tableName = props.getString(BIGQUERY_SYNC_TABLE_NAME);
    config.sourceUri = props.getString(BIGQUERY_SYNC_SOURCE_URI);
    config.sourceUriPrefix = props.getString(BIGQUERY_SYNC_SOURCE_URI_PREFIX);
    config.basePath = props.getString(BIGQUERY_SYNC_SYNC_BASE_PATH);
    config.partitionFields = props.getStringList(BIGQUERY_SYNC_PARTITION_FIELDS, ",", Collections.emptyList());
    config.useFileListingFromMetadata = props.getBoolean(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA, false);
    config.assumeDatePartitioning = props.getBoolean(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING, false);
    return config;
  }

  @Override
  public String toString() {
    return "BigQuerySyncConfig{projectId='" + projectId
        + "', datasetName='" + datasetName
        + "', datasetLocation='" + datasetLocation
        + "', tableName='" + tableName
        + "', sourceUri='" + sourceUri
        + "', sourceUriPrefix='" + sourceUriPrefix
        + "', basePath='" + basePath + "'"
        + ", partitionFields=" + partitionFields
        + "', useFileListingFromMetadata='" + useFileListingFromMetadata
        + "', assumeDataPartitioning='" + assumeDatePartitioning
        + "', help=" + help + "}";
  }
}
