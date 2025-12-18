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

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_CONNECTION_URL;
import static org.apache.hudi.utilities.config.JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_DBTABLE;
import static org.apache.hudi.utilities.config.JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_DRIVER_TYPE;
import static org.apache.hudi.utilities.config.JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_NULLABLE;
import static org.apache.hudi.utilities.config.JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_PASSWORD;
import static org.apache.hudi.utilities.config.JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_TIMEOUT;
import static org.apache.hudi.utilities.config.JdbcbasedSchemaProviderConfig.SOURCE_SCHEMA_JDBC_USERNAME;

/**
 * A schema provider to get metadata through Jdbc.
 */
public class JdbcbasedSchemaProvider extends SchemaProvider {
  private Schema sourceSchema;
  private final Map<String, String> options = new HashMap<>();

  public JdbcbasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    options.put("url", getStringWithAltKeys(props, SOURCE_SCHEMA_JDBC_CONNECTION_URL));
    options.put("driver", getStringWithAltKeys(props, SOURCE_SCHEMA_JDBC_DRIVER_TYPE));
    options.put("user", getStringWithAltKeys(props, SOURCE_SCHEMA_JDBC_USERNAME));
    options.put("password", getStringWithAltKeys(props, SOURCE_SCHEMA_JDBC_PASSWORD));
    options.put("dbtable", getStringWithAltKeys(props, SOURCE_SCHEMA_JDBC_DBTABLE));
    // the number of seconds the driver will wait for a Statement object to execute to the given
    // number of seconds. Zero means there is no limit.
    options.put("queryTimeout", getStringWithAltKeys(props, SOURCE_SCHEMA_JDBC_TIMEOUT, "0"));
    options.put("nullable", getStringWithAltKeys(props, SOURCE_SCHEMA_JDBC_NULLABLE, "true"));
  }

  @Override
  public Schema getSourceSchema() {
    if (this.sourceSchema != null) {
      return sourceSchema;
    }

    return UtilHelpers.getJDBCSchema(options).toAvroSchema();
  }
}
