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

package org.apache.hudi.bigquery;

import org.apache.hudi.common.config.HoodieMetadataConfig;

import com.beust.jcommander.Parameter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Configs needed to sync data into BigQuery.
 */
public class BigQuerySyncConfig implements Serializable {

  @Parameter(names = {"--help", "-h"}, help = true)
  public Boolean help = false;
  @Parameter(names = {"--project-id"}, description = "name of the target project in BigQuery", required = true)
  public String projectId;
  @Parameter(names = {"--dataset-name"}, description = "name of the target dataset in BigQuery", required = true)
  public String datasetName;
  @Parameter(names = {"--table-name"}, description = "name of the target table in BigQuery", required = true)
  public String tableName;
  @Parameter(names = {"--source-uri"}, description = "name of the source uri gcs path of the table", required = true)
  public String sourceUri;
  @Parameter(names = {"--source-uri-prefix"}, description = "name of the source uri gcs path prefix of the table")
  public String sourceUriPrefix;
  @Parameter(names = {"--base-path"}, description = "Basepath of hoodie table to sync", required = true)
  public String basePath;
  @Parameter(names = {"--partitioned-by"}, description = "Fields in the schema partitioned by")
  public List<String> partitionFields = new ArrayList<>();
  @Parameter(names = {"--assume-date-partitioning"}, description = "Assume standard yyyy/mm/dd partitioning, this"
      + " exists to support backward compatibility. If you use hoodie 0.3.x, do not set this parameter")
  public Boolean assumeDatePartitioning = false;
  @Parameter(names = {"--use-file-listing-from-metadata"}, description = "Fetch file listing from Hudi's metadata")
  public Boolean useFileListingFromMetadata = HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;

  public static BigQuerySyncConfig copy(BigQuerySyncConfig cfg) {
    BigQuerySyncConfig newConfig = new BigQuerySyncConfig();
    newConfig.projectId = cfg.projectId;
    newConfig.datasetName = cfg.datasetName;
    newConfig.tableName = cfg.tableName;
    newConfig.sourceUri = cfg.sourceUri;
    newConfig.sourceUriPrefix = cfg.sourceUriPrefix;
    newConfig.basePath = cfg.basePath;
    newConfig.partitionFields = cfg.partitionFields;
    newConfig.assumeDatePartitioning = cfg.assumeDatePartitioning;
    newConfig.useFileListingFromMetadata = cfg.useFileListingFromMetadata;
    return newConfig;
  }

  @Override
  public String toString() {
    return "BigQuerySyncConfig{datasetName='" + datasetName + "', tableName='" + tableName
        + "', projectId='" + projectId + "', sourceUri='" + sourceUri
        + "', sourceUriPrefix='" + sourceUriPrefix + "', assumeDataPartitioning='" + assumeDatePartitioning
        + "', basePath='" + basePath + "'" + ", partitionFields=" + partitionFields
        + "', useFileListingFromMetadata='" + useFileListingFromMetadata + "', help=" + help + "}";
  }
}
