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

package org.apache.hudi.utilities.config;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.HoodieConfig;

import javax.annotation.concurrent.Immutable;

import static org.apache.hudi.common.util.ConfigUtils.DELTA_STREAMER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;

/**
 * Parquet DFS Source Configs
 */
@Immutable
@ConfigClassProperty(name = "Parquet DFS Source Configs",
        groupName = ConfigGroups.Names.HUDI_STREAMER,
        subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
        description = "Configurations controlling the behavior of Parquet DFS source in Hudi Streamer.")
public class ParquetDFSSourceConfig extends HoodieConfig {

  public static final ConfigProperty<Boolean> PARQUET_DFS_MERGE_SCHEMA = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.parquet.dfs.merge_schema.enable")
      .defaultValue(false)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.parquet.dfs.merge_schema.enable")
      .markAdvanced()
      .sinceVersion("0.15.0")
      .withDocumentation("Merge schema across parquet files within a single write");
}
