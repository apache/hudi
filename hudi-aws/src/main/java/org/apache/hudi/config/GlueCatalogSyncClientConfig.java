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
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.util.stream.IntStream;

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

  public static final ConfigProperty<Integer> ALL_PARTITIONS_READ_PARALLELISM = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "all_partitions_read_parallelism")
      .defaultValue(1)
      .markAdvanced()
      .withValidValues(IntStream.rangeClosed(1, 10).mapToObj(Integer::toString).toArray(String[]::new))
      .sinceVersion("0.15.0")
      .withDocumentation("Parallelism for listing all partitions(first time sync). Should be in interval [1, 10].");

  public static final ConfigProperty<Integer> CHANGED_PARTITIONS_READ_PARALLELISM = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "changed_partitions_read_parallelism")
      .defaultValue(1)
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Parallelism for listing changed partitions(second and subsequent syncs).");

  public static final ConfigProperty<Integer> PARTITION_CHANGE_PARALLELISM = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "partition_change_parallelism")
      .defaultValue(1)
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Parallelism for change operations - such as create/update/delete.");

  public static final ConfigProperty<Boolean> GLUE_METADATA_FILE_LISTING = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "metadata_file_listing")
      .defaultValue(false)
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Makes athena use the metadata table to list partitions and files. Currently it won't benefit from other features such stats indexes");

  public static final ConfigProperty<Boolean> META_SYNC_PARTITION_INDEX_FIELDS_ENABLE = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "partition_index_fields.enable")
      .defaultValue(false)
      .sinceVersion("0.15.0")
      .withDocumentation("Enable aws glue partition index feature, to speedup partition based query pattern");

  public static final ConfigProperty<String> META_SYNC_PARTITION_INDEX_FIELDS = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "partition_index_fields")
      .noDefaultValue()
      .withInferFunction(cfg -> HoodieTableConfig.getPartitionFieldProp(cfg)
          .or(() -> Option.ofNullable(cfg.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME))))
      .sinceVersion("0.15.0")
      .withDocumentation(String.join(" ", "Specify the partitions fields to index on aws glue. Separate the fields by semicolon.",
          "By default, when the feature is enabled, all the partition will be indexed.",
          "You can create up to three indexes, separate them by comma. Eg: col1;col2;col3,col2,col3"));

  public static final ConfigProperty<Boolean> RECREATE_GLUE_TABLE_ON_ERROR = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "recreate_table_on_error")
      .defaultValue(false)
      .sinceVersion("0.14.0")
      .markAdvanced()
      .withDocumentation("Glue sync may fail if the Glue table exists with partitions differing from the Hoodie table or if schema evolution is not supported by Glue."
          + "Enabling this configuration will drop and create the table to match the Hoodie config");

  public static final ConfigProperty<String> GLUE_CATALOG_ID = ConfigProperty
      .key(GLUE_CLIENT_PROPERTY_PREFIX + "catalogId")
      .noDefaultValue()
      .sinceVersion("0.15.0")
      .markAdvanced()
      .withDocumentation("The catalogId needs to be populated for syncing hoodie tables in a different AWS account");
}
