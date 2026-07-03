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

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.avro.AvroRuntimeException;

/**
 * A global cache for HoodieSchema instances to ensure that there is only one
 * variable instance of the same schema within an entire JVM lifetime.
 *
 * <p>This is a global cache which works for a JVM lifecycle.
 * A collection of schema instances are maintained.
 *
 * <p>This value-keyed pool is the canonicalization mechanism behind
 * {@link HoodieSchema#fromAvroSchema}, and can also be used directly to intern schemas
 * produced without an Avro source (builders, converters).
 *
 * <p>Interning is never lossy: entries are keyed on the schema's full serialized content,
 * NOT on {@link HoodieSchema#equals}. Avro equality (which HoodieSchema equality delegates
 * to) ignores doc strings and aliases, so keying on it would collapse schemas that differ
 * only in that metadata and silently drop it -- docs drive catalog sync column comments and
 * aliases drive schema-evolution field matching. Schemas that differ in docs or aliases
 * intern to distinct canonical instances even though they are {@code equals()}.
 *
 * <p>NOTE: The schema which is used frequently should be cached through this cache.
 */
public class HoodieSchemaCache {

  // Ensure that there is only one variable instance of the same schema within an entire JVM lifetime
  private static final LoadingCache<SchemaContentKey, HoodieSchema> SCHEMA_CACHE =
      Caffeine.newBuilder().weakValues().maximumSize(1024).build(key -> key.schema);

  /**
   * Get schema variable from global cache. If not found, put it into the cache and then return it.
   *
   * <p>Two schemas converge on one canonical instance only when their full serialized form
   * (including doc strings and aliases) is identical; see the class javadoc.
   *
   * <p>A schema that is valid in memory but cannot be serialized to JSON -- e.g. two distinct
   * nested records that share a name, as some projection/reader paths produce -- has no content
   * key, so it is returned uncached instead of interned. Canonicalization is only a
   * de-duplication optimization, so skipping it stays correct.
   *
   * @param schema schema to get
   * @return if found, return the exist schema variable, otherwise return the param itself.
   */
  public static HoodieSchema intern(HoodieSchema schema) {
    SchemaContentKey key;
    try {
      key = new SchemaContentKey(schema);
    } catch (AvroRuntimeException e) {
      // Not serializable -> no content key derivable; skip interning rather than fail the caller.
      return schema;
    }
    return SCHEMA_CACHE.get(key);
  }

  /**
   * Content-complete cache key: the serialized JSON form covers doc strings and aliases that
   * Avro equality ignores. The wrapper class is part of the key so a logical-type subclass
   * (e.g. {@link HoodieSchema.Decimal}) never collapses onto a plain wrapper of equal content,
   * which would break downcasts.
   */
  private static final class SchemaContentKey {
    private final HoodieSchema schema;
    private final String contentJson;

    SchemaContentKey(HoodieSchema schema) {
      this.schema = schema;
      this.contentJson = schema.getAvroSchema().toString();
    }

    @Override
    public int hashCode() {
      return contentJson.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof SchemaContentKey)) {
        return false;
      }
      SchemaContentKey that = (SchemaContentKey) obj;
      return schema.getClass() == that.schema.getClass() && contentJson.equals(that.contentJson);
    }
  }
}
