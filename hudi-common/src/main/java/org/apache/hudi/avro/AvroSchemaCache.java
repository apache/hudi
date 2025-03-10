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

package org.apache.hudi.avro;

import org.apache.avro.Schema;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache avro schema variables.
 * NOTE: All methods that create/get Schema instances should go through this class for caching.
 */
public class AvroSchemaCache {


  // Ensure that there is only one variable instance of the same schema within a entire JVM lifetime
  private static final ConcurrentHashMap<Schema, Schema> SCHEMA_CACHE = new ConcurrentHashMap<>();

  /**
   * Get schema variable from global cache. If not found, put it into the cache and then return it.
   * @param schema schema to get
   * @return if found, return the exist schema variable, otherwise return the param itself.
   */
  public static Schema cacheAndGetSchema(Schema schema) {
    Schema cached = SCHEMA_CACHE.get(schema);
    if (cached == null) {
      cached = SCHEMA_CACHE.computeIfAbsent(schema, (k) -> schema);
    }
    return cached;
  }

}
