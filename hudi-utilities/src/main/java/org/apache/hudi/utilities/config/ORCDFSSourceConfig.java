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

import static org.apache.hudi.common.util.ConfigUtils.STREAMER_CONFIG_PREFIX;

/**
 * ORC DFS Source Configs
 */
@Immutable
@ConfigClassProperty(name = "ORC DFS Source Configs",
        groupName = ConfigGroups.Names.HUDI_STREAMER,
        subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
        description = "Configurations controlling the behavior of ORC DFS source in Hudi Streamer.")
public class ORCDFSSourceConfig extends HoodieConfig {

  public static final ConfigProperty<Boolean> ORC_DFS_MERGE_SCHEMA = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.orc.dfs.merge.schema.enable")
      .defaultValue(true)
      .markAdvanced()
      .sinceVersion("1.2.0")
      .withDocumentation("Whether to merge schema across ORC files within a single read. "
          + "Defaults to true: heterogeneous-schema source files (e.g. during bootstrap or "
          + "evolving producers) get a unioned schema instead of silently dropping columns "
          + "that exist only in some files. Requires spark.sql.orc.impl=native (default since "
          + "Spark 2.4); the option is silently ignored under spark.sql.orc.impl=hive.");
}
