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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Configs needed to sync data into BigQuery.
 */
public class BigQuerySyncConfig extends HoodieSyncConfig implements Serializable {

  public static final ConfigProperty<String> BIGQUERY_SYNC_PROJECT_ID = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.project_id")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> BIGQUERY_SYNC_DATASET_NAME = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.dataset_name")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> BIGQUERY_SYNC_DATASET_LOCATION = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.dataset_location")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> BIGQUERY_SYNC_TABLE_NAME = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.table_name")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> BIGQUERY_SYNC_SOURCE_URI = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.source_uri")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> BIGQUERY_SYNC_SOURCE_URI_PREFIX = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.source_uri_prefix")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> BIGQUERY_SYNC_SYNC_BASE_PATH = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.base_path")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<String> BIGQUERY_SYNC_PARTITION_FIELDS = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.partition_fields")
      .noDefaultValue()
      .withDocumentation("");

  public static final ConfigProperty<Boolean> BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.use_file_listing_from_metadata")
      .defaultValue(true)
      .withDocumentation("");

  public static final ConfigProperty<Boolean> BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.assume_date_partitioning")
      .defaultValue(false)
      .withDocumentation("");

  public BigQuerySyncConfig(Properties props) {
    super(props);
  }

  public static class BigQuerySyncConfigParams {

    @ParametersDelegate()
    public final HoodieSyncConfigParams hoodieSyncConfigParams = new HoodieSyncConfigParams();

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
    public boolean useFileListingFromMetadata = false;
    @Parameter(names = {"--assume-date-partitioning"}, description = "Assume standard yyyy/mm/dd partitioning, this"
        + " exists to support backward compatibility. If you use hoodie 0.3.x, do not set this parameter")
    public boolean assumeDatePartitioning = false;

    public boolean isHelp() {
      return hoodieSyncConfigParams.isHelp();
    }

    public Properties toProps() {
      final Properties props = hoodieSyncConfigParams.toProps();
      props.setProperty(BIGQUERY_SYNC_PROJECT_ID.key(), projectId);
      props.setProperty(BIGQUERY_SYNC_DATASET_NAME.key(), datasetName);
      props.setProperty(BIGQUERY_SYNC_DATASET_LOCATION.key(), datasetLocation);
      props.setProperty(BIGQUERY_SYNC_TABLE_NAME.key(), tableName);
      props.setProperty(BIGQUERY_SYNC_SOURCE_URI.key(), sourceUri);
      props.setProperty(BIGQUERY_SYNC_SOURCE_URI_PREFIX.key(), sourceUriPrefix);
      props.setProperty(BIGQUERY_SYNC_SYNC_BASE_PATH.key(), basePath);
      props.setProperty(BIGQUERY_SYNC_PARTITION_FIELDS.key(), String.join(",", partitionFields));
      props.setProperty(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), String.valueOf(useFileListingFromMetadata));
      props.setProperty(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING.key(), String.valueOf(assumeDatePartitioning));
      return props;
    }
  }
}
