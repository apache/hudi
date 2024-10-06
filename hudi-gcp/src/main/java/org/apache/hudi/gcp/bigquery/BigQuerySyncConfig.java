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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import javax.annotation.concurrent.Immutable;

import java.io.Serializable;
import java.util.Properties;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ENABLE_FOR_READERS;
import static org.apache.hudi.common.table.HoodieTableConfig.DATABASE_NAME;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_TABLE_NAME_KEY;
import static org.apache.hudi.common.table.HoodieTableConfig.HOODIE_WRITE_TABLE_NAME_KEY;

/**
 * Configs needed to sync data into BigQuery.
 */
@Immutable
@ConfigClassProperty(name = "BigQuery Sync Configs",
    groupName = ConfigGroups.Names.META_SYNC,
    description = "Configurations used by the Hudi to sync metadata to Google BigQuery.")
public class BigQuerySyncConfig extends HoodieSyncConfig implements Serializable {

  public static final ConfigProperty<String> BIGQUERY_SYNC_PROJECT_ID = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.project_id")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Name of the target project in BigQuery");

  public static final ConfigProperty<String> BIGQUERY_SYNC_BILLING_PROJECT_ID = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.billing.project.id")
      .noDefaultValue()
      .sinceVersion("0.15.1")
      .markAdvanced()
      .withDocumentation("Name of the billing project id in BigQuery. By default it uses the "
          + "configuration from `hoodie.gcp.bigquery.sync.project_id` if this configuration is "
          + "not set. This can only be used with manifest file based approach");
  
  public static final ConfigProperty<String> BIGQUERY_SYNC_DATASET_NAME = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.dataset_name")
      .noDefaultValue()
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(DATABASE_NAME)))
      .markAdvanced()
      .withDocumentation("Name of the target dataset in BigQuery");

  public static final ConfigProperty<String> BIGQUERY_SYNC_DATASET_LOCATION = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.dataset_location")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Location of the target dataset in BigQuery");

  public static final ConfigProperty<String> BIGQUERY_SYNC_TABLE_NAME = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.table_name")
      .noDefaultValue()
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(HOODIE_TABLE_NAME_KEY))
          .or(() -> Option.ofNullable(cfg.getString(HOODIE_WRITE_TABLE_NAME_KEY))))
      .markAdvanced()
      .withDocumentation("Name of the target table in BigQuery");

  public static final ConfigProperty<Boolean> BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.use_bq_manifest_file")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("If true, generate a manifest file with data file absolute paths and use BigQuery manifest file support to "
          + "directly create one external table over the Hudi table. If false (default), generate a manifest file with data file "
          + "names and create two external tables and one view in BigQuery. Query the view for the same results as querying the Hudi table");

  public static final ConfigProperty<String> BIGQUERY_SYNC_SOURCE_URI = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.source_uri")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Name of the source uri gcs path of the table");

  public static final ConfigProperty<String> BIGQUERY_SYNC_SOURCE_URI_PREFIX = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.source_uri_prefix")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("Name of the source uri gcs path prefix of the table");

  public static final ConfigProperty<String> BIGQUERY_SYNC_SYNC_BASE_PATH = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.base_path")
      .noDefaultValue()
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(META_SYNC_BASE_PATH)))
      .markAdvanced()
      .withDocumentation("Base path of the hoodie table to sync");

  public static final ConfigProperty<String> BIGQUERY_SYNC_PARTITION_FIELDS = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.partition_fields")
      .noDefaultValue()
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(HoodieTableConfig.PARTITION_FIELDS))
          .or(() -> Option.ofNullable(cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME))))
      .markAdvanced()
      .withDocumentation("Comma-delimited partition fields. Default to non-partitioned.");

  public static final ConfigProperty<Boolean> BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.use_file_listing_from_metadata")
      .defaultValue(DEFAULT_METADATA_ENABLE_FOR_READERS)
      .withInferFunction(cfg -> Option.of(cfg.getBooleanOrDefault(HoodieMetadataConfig.ENABLE, DEFAULT_METADATA_ENABLE_FOR_READERS)))
      .markAdvanced()
      .withDocumentation("Fetch file listing from Hudi's metadata");

  public static final ConfigProperty<String> BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.assume_date_partitioning")
      .defaultValue(HoodieMetadataConfig.ASSUME_DATE_PARTITIONING.defaultValue())
      .withInferFunction(cfg -> Option.ofNullable(cfg.getString(HoodieMetadataConfig.ASSUME_DATE_PARTITIONING)))
      .markAdvanced()
      .withDocumentation("Assume standard yyyy/mm/dd partitioning, this"
          + " exists to support backward compatibility. If you use hoodie 0.3.x, do not set this parameter");

  public static final ConfigProperty<Boolean> BIGQUERY_SYNC_REQUIRE_PARTITION_FILTER = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.require_partition_filter")
      .defaultValue(false)
      .sinceVersion("0.14.1")
      .markAdvanced()
      .withDocumentation("If true, configure table to require a partition filter to be specified when querying the table");

  public static final ConfigProperty<String> BIGQUERY_SYNC_BIG_LAKE_CONNECTION_ID = ConfigProperty
      .key("hoodie.gcp.bigquery.sync.big_lake_connection_id")
      .noDefaultValue()
      .sinceVersion("0.14.1")
      .markAdvanced()
      .withDocumentation("The Big Lake connection ID to use");

  public BigQuerySyncConfig(Properties props) {
    super(props);
    setDefaults(BigQuerySyncConfig.class.getName());
  }

  public static class BigQuerySyncConfigParams {

    @ParametersDelegate()
    public final HoodieSyncConfigParams hoodieSyncConfigParams = new HoodieSyncConfigParams();

    @Parameter(names = {"--project-id"}, description = "Name of the target project in BigQuery", required = true)
    public String projectId;
    @Parameter(names = {"--billing-project-id"}, description = "Name of the billing project in BigQuery. This can only be used with --use-bq-manifest-file", required = false)
    public String billingProjectId;
    @Parameter(names = {"--dataset-name"}, description = "Name of the target dataset in BigQuery", required = true)
    public String datasetName;
    @Parameter(names = {"--dataset-location"}, description = "Location of the target dataset in BigQuery", required = true)
    public String datasetLocation;
    @Parameter(names = {"--use-bq-manifest-file"}, description = "If true, generate a manifest file with data file absolute paths and use "
        + " BigQuery manifest file support to directly create one external table over the Hudi table. If false (default), generate a manifest "
        + " file with data file names and create two external tables and one view in BigQuery. Query the view for the same results as querying "
        + "the Hudi table")
    public Boolean useBqManifestFile;
    @Parameter(names = {"--source-uri"}, description = "Name of the source uri gcs path of the table", required = true)
    public String sourceUri;
    @Parameter(names = {"--source-uri-prefix"}, description = "Name of the source uri gcs path prefix of the table", required = false)
    public String sourceUriPrefix;
    @Parameter(names = {"--big-lake-connection-id"}, description = "The Big Lake connection ID to use when creating the table if using the manifest file approach.")
    public String bigLakeConnectionId;
    @Parameter(names = {"--require-partition-filter"}, description = "If true, configure table to require a partition filter to be specified when querying the table")
    public Boolean requirePartitionFilter;

    public boolean isHelp() {
      return hoodieSyncConfigParams.isHelp();
    }

    public TypedProperties toProps() {
      final TypedProperties props = hoodieSyncConfigParams.toProps();
      props.setPropertyIfNonNull(BIGQUERY_SYNC_PROJECT_ID.key(), projectId);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_BILLING_PROJECT_ID.key(), billingProjectId);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_DATASET_NAME.key(), datasetName);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_DATASET_LOCATION.key(), datasetLocation);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_TABLE_NAME.key(), hoodieSyncConfigParams.tableName);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_USE_BQ_MANIFEST_FILE.key(), useBqManifestFile);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_SOURCE_URI.key(), sourceUri);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_SOURCE_URI_PREFIX.key(), sourceUriPrefix);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_SYNC_BASE_PATH.key(), hoodieSyncConfigParams.basePath);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_PARTITION_FIELDS.key(), StringUtils.join(",", hoodieSyncConfigParams.partitionFields));
      props.setPropertyIfNonNull(BIGQUERY_SYNC_USE_FILE_LISTING_FROM_METADATA.key(), hoodieSyncConfigParams.useFileListingFromMetadata);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_ASSUME_DATE_PARTITIONING.key(), hoodieSyncConfigParams.assumeDatePartitioning);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_BIG_LAKE_CONNECTION_ID.key(), bigLakeConnectionId);
      props.setPropertyIfNonNull(BIGQUERY_SYNC_REQUIRE_PARTITION_FILTER.key(), requirePartitionFilter);
      return props;
    }
  }
}
