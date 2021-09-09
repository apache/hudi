/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;

/**
 * Flink sql metadata table related config.
 */
@ConfigClassProperty(name = "Flink Metadata table Options",
    groupName = ConfigGroups.Names.FLINK_SQL,
    description = "Flink jobs using the SQL can be configured through the options in WITH clause."
        + "Configurations that control flink Metadata table behavior on Hudi tables are listed below.")
public class FlinkMetadataTableOptions {

  public static final ConfigOption<Boolean> METADATA_ENABLED = ConfigOptions
      .key("metadata.enabled")
      .booleanType()
      .defaultValue(false)
      .withDescription("Enable the internal metadata table which serves table metadata like level file listings, default false.");

  public static final ConfigOption<Integer> METADATA_COMPACTION_DELTA_COMMITS = ConfigOptions
      .key("metadata.compaction.delta_commits")
      .intType()
      .defaultValue(24)
      .withDescription("Max delta commits for metadata table to trigger compaction, default 24.");
}
