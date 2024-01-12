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
 * JDBC Source Configs
 */
@Immutable
@ConfigClassProperty(name = "JDBC Source Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling the behavior of JDBC source in Hudi Streamer.")
public class JdbcSourceConfig extends HoodieConfig {

  public static final ConfigProperty<String> URL = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.url")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.url")
      .markAdvanced()
      .withDocumentation("JDBC url for the Hoodie datasource.");

  public static final ConfigProperty<String> USER = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.user")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.user")
      .markAdvanced()
      .withDocumentation("Username used for JDBC connection");

  public static final ConfigProperty<String> PASSWORD = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.password")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.password")
      .markAdvanced()
      .withDocumentation("Password used for JDBC connection");

  public static final ConfigProperty<String> PASSWORD_FILE = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.password.file")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.password.file")
      .markAdvanced()
      .withDocumentation("Base-path for the JDBC password file.");

  public static final ConfigProperty<String> DRIVER_CLASS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.driver.class")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.driver.class")
      .markAdvanced()
      .withDocumentation("Driver class used for JDBC connection");

  public static final ConfigProperty<String> RDBMS_TABLE_NAME = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.table.name")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.table.name")
      .markAdvanced()
      .withDocumentation("RDBMS table to pull");

  public static final ConfigProperty<String> INCREMENTAL_COLUMN = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.table.incr.column.name")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.table.incr.column.name")
      .markAdvanced()
      .withDocumentation("If run in incremental mode, this field is to pull new data incrementally");

  public static final ConfigProperty<String> IS_INCREMENTAL = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.incr.pull")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.incr.pull")
      .markAdvanced()
      .withDocumentation("Will the JDBC source do an incremental pull?");

  public static final ConfigProperty<String> EXTRA_OPTIONS = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.extra.options.")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.extra.options.")
      .markAdvanced()
      .withDocumentation("Used to set any extra options the user specifies for jdbc");

  public static final ConfigProperty<String> STORAGE_LEVEL = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.storage.level")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.storage.level")
      .markAdvanced()
      .withDocumentation("Used to control the persistence level. Default value: MEMORY_AND_DISK_SER");

  public static final ConfigProperty<String> FALLBACK_TO_FULL_FETCH = ConfigProperty
      .key(STREAMER_CONFIG_PREFIX + "jdbc.incr.fallback.to.full.fetch")
      .noDefaultValue()
      .withAlternatives(DELTA_STREAMER_CONFIG_PREFIX + "jdbc.incr.fallback.to.full.fetch")
      .markAdvanced()
      .withDocumentation("If set true, makes incremental fetch to fallback to full fetch in case of any error");
}
