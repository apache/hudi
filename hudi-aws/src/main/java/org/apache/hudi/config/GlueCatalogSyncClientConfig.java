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

package org.apache.hudi.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

/**
 * Hoodie Configs for Glue.
 */
@ConfigClassProperty(name = "Glue catalog sync based client Configurations",
    groupName = ConfigGroups.Names.META_SYNC,
    subGroupName = ConfigGroups.SubGroupNames.NONE,
    description = "Configs that control Glue catalog sync based client.")
public class GlueCatalogSyncClientConfig extends HoodieConfig {
  public static final String GLUE_CLIENT_PROPERTY_PREFIX = "hoodie.datasource.meta.sync.glue.";

  public static final ConfigProperty<Boolean> GLUE_SKIP_TABLE_ARCHIVE = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "skip_table_archive")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Glue catalog sync based client will skip archiving the table version if this config is set to true");

  public static final ConfigProperty<Boolean> GLUE_METADATA_FILE_LISTING = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "metadata_file_listing")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Makes athena use the metadata table to list partitions and files. Currently it won't benefit from other features such stats indexes");

  public static final ConfigProperty<Boolean> RECREATE_GLUE_TABLE_ON_ERROR = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "recreate_table_on_error")
      .defaultValue(false)
      .sinceVersion("0.14.0")
      .markAdvanced()
      .withDocumentation("Glue sync may fail if the Glue table exists with partitions differing from the Hoodie table or if schema evolution is not supported by Glue."
          + "Enabling this configuration will drop and create the table to match the Hoodie config");


  public static final ConfigProperty<String> GLUE_SYNC_DATABASE_NAME = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "database_name")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("The name of the destination database that we should sync the hudi table to.");

  public static final ConfigProperty<String> GLUE_CATALOG_ID = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "catalogId")
      .noDefaultValue()
      .sinceVersion("0.15.0")
      .markAdvanced()
      .withDocumentation("The catalogId needs to be populated for syncing hoodie tables in a different AWS account");

  public static final ConfigProperty<String> GLUE_SYNC_TABLE_NAME = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "table_name")
      .noDefaultValue()
      .markAdvanced()
      .withDocumentation("The name of the destination table that we should sync the hudi table to.");

  public static final ConfigProperty<Integer> GLUE_SYNC_MAX_PARTITIONS_PER_REQUEST = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "max_partitions_per_request")
      .defaultValue(100)
      .sinceVersion("1.1.0")
      .markAdvanced()
      .withDocumentation("The maximum number of partitions to be synced in a single request to Glue.");

  public static final ConfigProperty<Integer> GLUE_SYNC_MAX_CONCURRENT_REQUESTS = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "max_concurrent_requests")
      .defaultValue(100)
      .sinceVersion("1.1.0")
      .markAdvanced()
      .withDocumentation("The maximum number of requests that can be run concurrently. Helps prevent throttling when syncing tables with many partitions.");

  public static final ConfigProperty<String> GLUE_SYNC_RESOURCE_TAGS = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "resource_tags")
      .noDefaultValue()
      .sinceVersion("1.1.0")
      .markAdvanced()
      .withDocumentation("Tags to be applied to AWS Glue databases and tables during sync. Format: key1:value1,key2:value2");

}
