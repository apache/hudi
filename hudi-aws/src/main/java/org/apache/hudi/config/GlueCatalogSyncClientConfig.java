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

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.hive.HiveSyncConfig;

import java.util.Properties;

@ConfigClassProperty(name = "Glue catalog sync based client Configurations",
        groupName = ConfigGroups.Names.META_SYNC,
        subGroupName = ConfigGroups.SubGroupNames.NONE,
        description = "Configs that control Glue catalog sync based client.")
public class GlueCatalogSyncClientConfig extends HiveSyncConfig {

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

  public static final ConfigProperty<String> GLUE_AWS_REGION = ConfigProperty
          .key("hoodie.aws.region")
          .noDefaultValue()
          .withDocumentation("AWS region");

  public static final ConfigProperty<Integer> GLUE_MAX_CONNECTIONS = ConfigProperty
          .key("hoodie.glue.sync.maxConnections")
          .defaultValue(50)
          .withDocumentation("Maximum number of open HTTP connections to glue at any given point in time. AWS allows 50 connections by default."
                  + "The maximum quota of such connections by default is set as 1000. Please refer the link for more - https://docs.aws.amazon.com/general/latest/gr/glue.html");

  public GlueCatalogSyncClientConfig(Properties properties) {
    super(properties);
  }

  public static class GlueSyncConfigParams {
    @ParametersDelegate
    public final HiveSyncConfigParams hiveSyncConfigParams = new HiveSyncConfigParams();

    @Parameter(names = {"--region"}, description = "AWS region")
    public String glueRegion;

    @Parameter(names = {"--max-connections"}, description = "Max connections to glue for concurrent access")
    public String maxConnections;

    public boolean isHelp() {
      return hiveSyncConfigParams.isHelp();
    }

    public TypedProperties toProps() {
      final TypedProperties props = hiveSyncConfigParams.toProps();
      props.setPropertyIfNonNull(GLUE_AWS_REGION.key(), glueRegion);
      props.setPropertyIfNonNull(GLUE_MAX_CONNECTIONS.key(), maxConnections);
      return props;
    }
  }
}
