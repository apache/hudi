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
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.config.JdbcbasedSchemaProviderConfig;

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

  public JdbcbasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    options.put("url", props.getString(JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_CONNECTION_URL.key()));
    options.put("driver", props.getString(JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_DRIVER_TYPE.key()));
    options.put("user", props.getString(JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_USERNAME.key()));
    options.put("password", props.getString(JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_PASSWORD.key()));
    options.put("dbtable", props.getString(JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_DBTABLE.key()));
    // the number of seconds the driver will wait for a Statement object to execute to the given
    // number of seconds. Zero means there is no limit.
    options.put("queryTimeout", props.getString(JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_TIMEOUT.key(), "0"));
    options.put("nullable", props.getString(JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_NULLABLE.key(), "true"));
  }

  @Override
  public Schema getSourceSchema() {
    if (this.sourceSchema != null) {
      return sourceSchema;
    }

    return UtilHelpers.getJDBCSchema(options);
  }
}
