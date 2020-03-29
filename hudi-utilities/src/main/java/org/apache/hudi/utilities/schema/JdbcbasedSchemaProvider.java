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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

/**
 * A schema provider to get metadata through Jdbc.
 */
public class JdbcbasedSchemaProvider extends SchemaProvider {
  private Schema sourceSchema;
  private Map<String, String> options = new HashMap<>();

  /**
   * Configs supported.
   */
  public static class Config {
    // The JDBC URL to connect to. The source-specific connection properties may be specified in the URL.
    // e.g., jdbc:postgresql://localhost/test?user=fred&password=secret
    private static final String SOURCE_SCHEMA_JDBC_CONNECTION_URL = "hoodie.deltastreamer.schemaprovider.source.schema.jdbc.connection.url";
    // The class name of the JDBC driver to use to connect to this URL. such as org.h2.Driver
    private static final String SOURCE_SCHEMA_JDBC_DRIVER_TYPE = "hoodie.deltastreamer.schemaprovider.source.schema.jdbc.driver.type";
    private static final String SOURCE_SCHEMA_JDBC_USERNAME = "hoodie.deltastreamer.schemaprovider.source.schema.jdbc.username";
    private static final String SOURCE_SCHEMA_JDBC_PASSWORD = "hoodie.deltastreamer.schemaprovider.source.schema.jdbc.password";
    // example : test_database.test1_table or test1_table
    private static final String SOURCE_SCHEMA_JDBC_DBTABLE = "hoodie.deltastreamer.schemaprovider.source.schema.jdbc.dbtable";
    // The number of seconds the driver will wait for a Statement object to execute to the given number of seconds.
    // Zero means there is no limit. In the write path, this option depends on how JDBC drivers implement the API setQueryTimeout,
    // e.g., the h2 JDBC driver checks the timeout of each query instead of an entire JDBC batch. It defaults to 0.
    private static final String SOURCE_SCHEMA_JDBC_TIMEOUT = "hoodie.deltastreamer.schemaprovider.source.schema.jdbc.timeout";
    // If true, all the columns are nullable.
    private static final String SOURCE_SCHEMA_JDBC_NULLABLE = "hoodie.deltastreamer.schemaprovider.source.schema.jdbc.nullable";
  }

  public JdbcbasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    options.put("url", props.getString(Config.SOURCE_SCHEMA_JDBC_CONNECTION_URL));
    options.put("driver", props.getString(Config.SOURCE_SCHEMA_JDBC_DRIVER_TYPE));
    options.put("user", props.getString(Config.SOURCE_SCHEMA_JDBC_USERNAME));
    options.put("password", props.getString(Config.SOURCE_SCHEMA_JDBC_PASSWORD));
    options.put("dbtable", props.getString(Config.SOURCE_SCHEMA_JDBC_DBTABLE));
    // the number of seconds the driver will wait for a Statement object to execute to the given
    // number of seconds. Zero means there is no limit.
    options.put("queryTimeout", props.getString(Config.SOURCE_SCHEMA_JDBC_TIMEOUT, "0"));
    options.put("nullable", props.getString(Config.SOURCE_SCHEMA_JDBC_NULLABLE, "true"));
  }

  @Override
  public Schema getSourceSchema() {
    if (this.sourceSchema != null) {
      return sourceSchema;
    }

    try {
      sourceSchema = UtilHelpers.getJDBCSchema(options);
    } catch (Exception e) {
      throw new HoodieException("Failed to get Schema through jdbc. ", e);
    }
    return sourceSchema;
  }
}
