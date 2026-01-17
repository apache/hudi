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

package org.apache.hudi.common.util;

import org.apache.hudi.common.schema.HoodieSchema;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * A HoodieSchema cache implementation for managing different versions of schemas.
 * This is a local cache; the versionId only works for local thread in one container/executor.
 * A map of {version_id, schema} is maintained.
 */
@NotThreadSafe
public class LocalHoodieSchemaCache implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<Integer, HoodieSchema> versionIdToSchema; // the mapping from version_id -> schema
  private final Map<HoodieSchema, Integer> schemaToVersionId; // the mapping from schema -> version_id

  private int nextVersionId = 0;

  private LocalHoodieSchemaCache() {
    this.versionIdToSchema = new HashMap<>();
    this.schemaToVersionId = new HashMap<>();
  }

  public static LocalHoodieSchemaCache getInstance() {
    return new LocalHoodieSchemaCache();
  }

  public Integer cacheSchema(HoodieSchema schema) {
    Integer versionId = this.schemaToVersionId.get(schema);
    if (versionId == null) {
      versionId = nextVersionId++;
      this.schemaToVersionId.put(schema, versionId);
      this.versionIdToSchema.put(versionId, schema);
    }
    return versionId;
  }

  public Option<HoodieSchema> getSchema(Integer versionId) {
    return Option.ofNullable(this.versionIdToSchema.get(versionId));
  }
}