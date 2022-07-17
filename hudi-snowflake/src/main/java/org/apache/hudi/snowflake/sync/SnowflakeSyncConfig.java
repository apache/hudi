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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.sync.common.HoodieSyncConfig;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Configs needed to sync data into Snowflake.
 */
public class SnowflakeSyncConfig extends HoodieSyncConfig implements Serializable {
  public static final ConfigProperty<String> SNOWFLAKE_SYNC_PROPERTIES_FILE = ConfigProperty
      .key("hoodie.snowflake.sync.properties_file")
      .noDefaultValue()
      .withDocumentation("Name of the snowflake properties file");

  public static final ConfigProperty<String> SNOWFLAKE_SYNC_STORAGE_INTEGRATION = ConfigProperty
      .key("hoodie.snowflake.sync.storage_integration")
      .noDefaultValue()
      .withDocumentation("Name of the snowflake storage integration");

  public static final ConfigProperty<String> SNOWFLAKE_SYNC_TABLE_NAME = ConfigProperty
      .key("hoodie.snowflake.sync.table_name")
      .noDefaultValue()
      .withDocumentation("Name of the target table in Snowflake");

  public static final ConfigProperty<String> SNOWFLAKE_SYNC_SYNC_BASE_PATH = ConfigProperty
      .key("hoodie.snowflake.sync.base_path")
      .noDefaultValue()
      .withDocumentation("Base path of the hoodie table to sync.");

  public static final ConfigProperty<String> SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT = ConfigProperty
      .key("hoodie.snowflake.sync.base_file_format")
      .noDefaultValue()
      .withDocumentation("Name of the file format");

  public static final ConfigProperty<String> SNOWFLAKE_SYNC_PARTITION_FIELDS = ConfigProperty
      .key("hoodie.snowflake.sync.partition_fields")
      .noDefaultValue()
      .withDocumentation("Comma-delimited partition fields. Default to non-partitioned.");

  public static final ConfigProperty<String> SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION = ConfigProperty
      .key("hoodie.snowflake.sync.partition_extract_expression")
      .noDefaultValue()
      .withDocumentation("Comma-delimited partition extract expression. Default to non-partitioned.");

  public SnowflakeSyncConfig(final Properties props) {
    super(props);
  }

  public static class SnowflakeSyncConfigParams {

    @ParametersDelegate()
    public final HoodieSyncConfigParams hoodieSyncConfigParams = new HoodieSyncConfigParams();
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
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    public Properties toProps() {
      final Properties props = hoodieSyncConfigParams.toProps();
      props.put(SnowflakeSyncConfig.SNOWFLAKE_SYNC_PROPERTIES_FILE, this.propertiesFile);
      props.put(SnowflakeSyncConfig.SNOWFLAKE_SYNC_STORAGE_INTEGRATION, this.storageIntegration);
      props.put(SnowflakeSyncConfig.SNOWFLAKE_SYNC_TABLE_NAME, this.tableName);
      props.put(SnowflakeSyncConfig.SNOWFLAKE_SYNC_SYNC_BASE_PATH, this.basePath);
      props.put(SnowflakeSyncConfig.SNOWFLAKE_SYNC_SYNC_BASE_FILE_FORMAT, this.baseFileFormat);
      props.put(SnowflakeSyncConfig.SNOWFLAKE_SYNC_PARTITION_FIELDS, String.join(",", this.partitionFields));
      props.put(SnowflakeSyncConfig.SNOWFLAKE_SYNC_PARTITION_EXTRACT_EXPRESSION, String.join(",", this.partitionExtractExpr));
      return props;
    }

    public boolean isHelp() {
      return hoodieSyncConfigParams.isHelp();
    }
  }
}
