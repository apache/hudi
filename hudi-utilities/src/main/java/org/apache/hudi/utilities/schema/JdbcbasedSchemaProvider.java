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

import org.apache.avro.Schema;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.UtilHelpers;
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
    private static final String SOURCE_SCHEMA_JDBC_CONNECTION_URL = "hoodie.deltastreamer.schemaprovider.source.schema.jdbc.connection.url";
    private static final String TARGET_SCHEMA_JDBC_DRIVER_TYPE = "hoodie.deltastreamer.schemaprovider.target.schema.jdbc.driver.type";
    private static final String TARGET_SCHEMA_JDBC_USERNAME = "hoodie.deltastreamer.schemaprovider.target.schema.jdbc.username";
    private static final String TARGET_SCHEMA_JDBC_PASSWORD = "hoodie.deltastreamer.schemaprovider.target.schema.jdbc.password";
    private static final String TARGET_SCHEMA_JDBC_DBTABLE = "hoodie.deltastreamer.schemaprovider.target.schema.jdbc.dbtable";
    private static final String TARGET_SCHEMA_JDBC_TIMEOUT = "hoodie.deltastreamer.schemaprovider.target.schema.jdbc.timeout";
  }

  public JdbcbasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    options.put("url", props.getString(Config.SOURCE_SCHEMA_JDBC_CONNECTION_URL));
    options.put("driver", props.getString(Config.TARGET_SCHEMA_JDBC_DRIVER_TYPE));
    options.put("user", props.getString(Config.TARGET_SCHEMA_JDBC_USERNAME));
    options.put("password", props.getString(Config.TARGET_SCHEMA_JDBC_PASSWORD));
    options.put("dbtable", props.getString(Config.TARGET_SCHEMA_JDBC_DBTABLE));
    // the number of seconds the driver will wait for a Statement object to execute to the given
    // number of seconds. Zero means there is no limit.
    options.put("timeout", props.getString(Config.TARGET_SCHEMA_JDBC_TIMEOUT, "0"));
  }

  @Override
  public Schema getSourceSchema() {
    if (this.sourceSchema != null) {
      return sourceSchema;
    }

    try {
      sourceSchema = UtilHelpers.getSchema(options);
    } catch (Exception e) {
      throw new HoodieException("Failed to get Schema through jdbc. ", e);
    }
    return sourceSchema;
  }
}
