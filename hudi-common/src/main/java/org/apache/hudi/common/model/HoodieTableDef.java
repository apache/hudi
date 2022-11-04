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

package org.apache.hudi.common.model;

import org.apache.hudi.internal.schema.InternalSchema;

import java.util.Objects;

public class HoodieTableDef {

  private final HoodieTableId tableId;

  // Table's base path
  private final String basePath;

  // Table's latest schema
  private final InternalSchema schema;

  // Table's type (COW, MOR)
  private final HoodieTableType tableType;

  public HoodieTableDef(HoodieTableId tableId, String basePath, InternalSchema schema, HoodieTableType tableType) {
    this.tableId = tableId;
    this.basePath = basePath;
    this.schema = schema;
    this.tableType = tableType;
  }

  public HoodieTableId getTableId() {
    return tableId;
  }

  public String getBasePath() {
    return basePath;
  }

  public InternalSchema getSchema() {
    return schema;
  }

  public HoodieTableType getTableType() {
    return tableType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieTableDef that = (HoodieTableDef) o;
    return Objects.equals(tableId, that.tableId) && Objects.equals(basePath, that.basePath) && Objects.equals(schema, that.schema) && tableType == that.tableType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tableId, basePath, schema, tableType);
  }

  @Override
  public String toString() {
    return "HoodieTableDef{"
        + "tableId=" + tableId
        + ", basePath='" + basePath + '\''
        + ", schema=" + schema
        + ", tableType=" + tableType
        + '}';
  }
}
