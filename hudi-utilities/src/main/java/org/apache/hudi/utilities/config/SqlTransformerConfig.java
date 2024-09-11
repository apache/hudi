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
 * Hudi Streamer SQL Transformer Configs
 */
@Immutable
@ConfigClassProperty(name = "Hudi Streamer SQL Transformer Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    description = "Configurations controlling the behavior of SQL transformer in Hudi Streamer.")
public class SqlTransformerConfig extends HoodieConfig {
  public static final ConfigProperty<String> TRANSFORMER_SQL_FILE = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "transformer.sql.file")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "transformer.sql.file")
      .withDocumentation("File with a SQL script to be executed during write");

  public static final ConfigProperty<String> TRANSFORMER_SQL = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "transformer.sql")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "transformer.sql")
      .withDocumentation("SQL Query to be executed during write");
}
