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

package org.apache.hudi.table.format;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.internal.schema.InternalSchema;

import java.io.Serializable;

/**
 * Data class to pass schema evolution info from table source to input format.
 */
public final class SchemaEvoContext implements Serializable {
  private final boolean enabled;
  private final InternalSchema querySchema;
  private final HoodieTableMetaClient metaClient;

  public SchemaEvoContext(boolean enabled, InternalSchema querySchema, HoodieTableMetaClient metaClient) {
    this.enabled = enabled;
    this.querySchema = querySchema;
    this.metaClient = metaClient;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public InternalSchema querySchema() {
    return querySchema;
  }

  public HoodieTableMetaClient metaClient() {
    return metaClient;
  }

  public String tableName() {
    return metaClient.getTableConfig().getTableName();
  }
}
