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

/**
 * A global cache for HoodieSchema instances to ensure that there is only one
 * variable instance of the same schema within an entire JVM lifetime.
 *
 * <p>This is a global cache which works for a JVM lifecycle.
 * A collection of schema instances are maintained.
 *
 * <p>NOTE: The schema which is used frequently should be cached through this cache.
 */
public class HoodieSchemaCache {

  // Ensure that there is only one variable instance of the same schema within an entire JVM lifetime
  private static final LoadingCache<HoodieSchema, HoodieSchema> SCHEMA_CACHE =
      Caffeine.newBuilder().weakValues().maximumSize(1024).build(k -> k);

  /**
   * Get schema variable from global cache. If not found, put it into the cache and then return it.
   *
   * @param schema schema to get
   * @return if found, return the exist schema variable, otherwise return the param itself.
   */
  public static HoodieSchema intern(HoodieSchema schema) {
    return SCHEMA_CACHE.get(schema);
  }
}