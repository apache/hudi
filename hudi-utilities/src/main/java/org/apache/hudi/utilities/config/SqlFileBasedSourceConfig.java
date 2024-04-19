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

@Immutable
@ConfigClassProperty(name = "File-based SQL Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of File-based SQL Source in Hudi Streamer.")
public class SqlFileBasedSourceConfig extends HoodieConfig {

  public static final ConfigProperty<String> SOURCE_SQL_FILE = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.sql.file")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.sql.file")
      .sinceVersion("0.14.0")
      .withDocumentation("SQL file path containing the SQL query to read source data.");

  public static final ConfigProperty<Boolean> EMIT_EPOCH_CHECKPOINT = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "source.sql.checkpoint.emit")
      .defaultValue(false)
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "source.sql.checkpoint.emit")
      .markAdvanced()
      .sinceVersion("0.14.0")
      .withDocumentation("Whether to emit the current epoch as the streamer checkpoint.");
}
