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

import static org.apache.hudi.common.util.ConfigUtils.OLD_SCHEMAPROVIDER_CONFIG_PREFIX;
import static org.apache.hudi.common.util.ConfigUtils.SCHEMAPROVIDER_CONFIG_PREFIX;

/**
 * JDBC-based Schema Provider Configs.
 */
@Immutable
@ConfigClassProperty(name = "JDBC-based Schema Provider Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.SCHEMA_PROVIDER,
    description = "Configurations for JDBC-based schema provider.")
public class JdbcbasedSchemaProviderConfig extends HoodieConfig {
  public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_CONNECTION_URL = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.connection.url")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.connection.url")
      .markAdvanced()
      .withDocumentation("The JDBC URL to connect to. The source-specific connection properties may be specified in the URL."
          + " e.g., jdbc:postgresql://localhost/test?user=fred&password=secret");

  public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_DRIVER_TYPE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.driver.type")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.driver.type")
      .markAdvanced()
      .withDocumentation("The class name of the JDBC driver to use to connect to this URL. e.g. org.h2.Driver");

  public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_USERNAME = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.username")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.username")
      .markAdvanced()
      .withDocumentation("Username for the connection e.g. fred");

  public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_PASSWORD = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.password")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.password")
      .markAdvanced()
      .withDocumentation("Password for the connection e.g. secret");

  public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_DBTABLE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.dbtable")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.dbtable")
      .markAdvanced()
      .withDocumentation("The table with the schema to reference e.g. test_database.test1_table or test1_table");

  public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_TIMEOUT = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.timeout")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.timeout")
      .markAdvanced()
      .withDocumentation("The number of seconds the driver will wait for a Statement object to execute. Zero means there is no limit. "
          + "In the write path, this option depends on how JDBC drivers implement the API setQueryTimeout, e.g., the h2 JDBC driver "
          + "checks the timeout of each query instead of an entire JDBC batch. It defaults to 0.");

  public static final ConfigProperty<String> SOURCE_SCHEMA_JDBC_NULLABLE = ConfigProperty
      .key(SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.nullable")
      .noDefaultValue()
      .withAlternatives(OLD_SCHEMAPROVIDER_CONFIG_PREFIX + "source.schema.jdbc.nullable")
      .markAdvanced()
      .withDocumentation("If true, all the columns are nullable.");
}
