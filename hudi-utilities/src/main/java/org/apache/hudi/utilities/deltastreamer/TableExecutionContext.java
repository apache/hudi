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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;

import java.util.Objects;

/**
 * Wrapper over TableConfig objects.
 * Useful for incrementally syncing multiple tables one by one via HoodieMultiTableDeltaStreamer.java class.
 */
public class TableExecutionContext {

  private TypedProperties properties;
  private HoodieDeltaStreamer.Config config;
  private String database;
  private String tableName;

  public HoodieDeltaStreamer.Config getConfig() {
    return config;
  }

  public void setConfig(HoodieDeltaStreamer.Config config) {
    this.config = config;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public TypedProperties getProperties() {
    return properties;
  }

  public void setProperties(TypedProperties properties) {
    this.properties = properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TableExecutionContext that = (TableExecutionContext) o;
    return Objects.equals(properties, that.properties) && Objects.equals(database, that.database) && Objects.equals(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, database, tableName);
  }
}
