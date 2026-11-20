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

package org.apache.hudi.common.schema;

import org.apache.hudi.common.util.Option;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A HoodieSchema cache implementation for managing different versions of schemas.
 * A map of {version_id, schema} is maintained; a version id is only meaningful inside the JVM that issued it.
 *
 * <p>Thread-safe: the static field-accessor record contexts (e.g. {@code AvroRecordContext.getFieldAccessorInstance()})
 * hold one instance that every task thread of an executor shares, so a version id issued on one thread must resolve
 * to its schema from any other thread. Lookups are lock-free; issuing a new id is serialized so that an equal schema
 * never receives two ids and an id is visible in {@link #getSchema} before it is returned.
 */
@ThreadSafe
public class LocalHoodieSchemaCache implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Map<Integer, HoodieSchema> versionIdToSchema; // the mapping from version_id -> schema
  private final Map<HoodieSchema, Integer> schemaToVersionId; // the mapping from schema -> version_id

  // guarded by this
  private int nextVersionId = 0;
  // the id issued for a null schema (a concurrent map cannot hold a null key); guarded by this
  private Integer nullSchemaVersionId;

  private LocalHoodieSchemaCache() {
    this.versionIdToSchema = new ConcurrentHashMap<>();
    this.schemaToVersionId = new ConcurrentHashMap<>();
  }

  /**
   * Returns a new, empty cache. Each caller owns its own version-id space.
   */
  public static LocalHoodieSchemaCache create() {
    return new LocalHoodieSchemaCache();
  }

  /**
   * Returns the version id of {@code schema}, issuing a new one the first time an equal schema is seen.
   */
  public Integer cacheSchema(HoodieSchema schema) {
    if (schema != null) {
      Integer versionId = this.schemaToVersionId.get(schema);
      if (versionId != null) {
        return versionId;
      }
    }
    return issueVersionId(schema);
  }

  private synchronized Integer issueVersionId(HoodieSchema schema) {
    if (schema == null) {
      if (nullSchemaVersionId == null) {
        nullSchemaVersionId = nextVersionId++;
      }
      return nullSchemaVersionId;
    }
    Integer versionId = this.schemaToVersionId.get(schema);
    if (versionId == null) {
      versionId = nextVersionId++;
      // publish id -> schema first, so any thread that can see the id can resolve it
      this.versionIdToSchema.put(versionId, schema);
      this.schemaToVersionId.put(schema, versionId);
    }
    return versionId;
  }

  /**
   * Serializes under the lock that guards id issuing, so a copy never pairs a counter with maps from a different
   * moment (which would let the copy re-issue an id it already holds).
   */
  private synchronized void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  /**
   * Returns the schema issued under {@code versionId}, or empty for a null or unknown id (e.g. a delete record).
   */
  public Option<HoodieSchema> getSchema(Integer versionId) {
    return versionId == null ? Option.empty() : Option.ofNullable(this.versionIdToSchema.get(versionId));
  }
}
