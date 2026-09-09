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
import org.apache.avro.Schema;

/**
 * A global cache mapping Avro {@link Schema} instances to their canonical {@link HoodieSchema}.
 *
 * <p>This is an Avro-schema-keyed view onto {@link HoodieSchemaCache} for per-record call sites:
 * {@code weakKeys} gives identity-based lookups (records of one file share the same {@link Schema}
 * instance), so the hot path is a single cache hit with no wrapper allocation or type dispatch.
 * Misses convert and then value-intern through {@link HoodieSchemaCache}, so equal but distinct Avro
 * schema instances still converge on one canonical {@link HoodieSchema}.
 *
 * <p>This is a global cache which works for a JVM lifecycle.
 */
public class HoodieAvroSchemaCache {

  private static final LoadingCache<Schema, HoodieSchema> AVRO_SCHEMA_CACHE =
      Caffeine.newBuilder().weakKeys().maximumSize(1024)
          .build(avroSchema -> HoodieSchemaCache.intern(HoodieSchema.fromAvroSchema(avroSchema)));

  /**
   * Returns the canonical {@link HoodieSchema} wrapping the given Avro schema, converting and
   * interning it on first use.
   *
   * @param avroSchema Avro schema to look up
   * @return the canonical HoodieSchema for the given Avro schema
   */
  public static HoodieSchema intern(Schema avroSchema) {
    return AVRO_SCHEMA_CACHE.get(avroSchema);
  }
}
