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

package org.apache.hudi.common.schema.evolution;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.utils.SerDeHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * HoodieSchema-shaped façade for evolution-schema JSON serialization.
 *
 * <p>The on-disk format is fixed: the {@code latest_schema} blob in commit metadata
 * and the {@code .hoodie/.schema/} history files use the same JSON layout that
 * {@link SerDeHelper} has always produced ({@code schemas} array containing
 * objects with {@code version_id}, {@code max_column_id}, {@code type}, {@code fields}, etc.).
 * Old tables must remain readable, so this façade delegates to {@link SerDeHelper}
 * verbatim and converts at the HoodieSchema/InternalSchema boundary via
 * {@link HoodieSchemaInternalSchemaBridge}, preserving field ids on the way out.</p>
 *
 * <p>Phase 5 of the InternalSchema removal will rewrite the JSON serializer in
 * pure HoodieSchema terms behind this stable interface — but the byte-for-byte
 * compatibility constraint stays in force.</p>
 */
public final class HoodieSchemaSerDe {

  /**
   * Commit-metadata key under which the latest schema's JSON is stored. Carried
   * over verbatim from {@link SerDeHelper#LATEST_SCHEMA} so callers reading old
   * commit metadata pick up the same blob.
   */
  public static final String LATEST_SCHEMA = SerDeHelper.LATEST_SCHEMA;

  /**
   * JSON object key that wraps the array of historical schemas. Same as
   * {@link SerDeHelper#SCHEMAS}.
   */
  public static final String SCHEMAS = SerDeHelper.SCHEMAS;

  private HoodieSchemaSerDe() {
  }

  /**
   * Serializes a single schema to JSON. Output format matches the legacy
   * {@link SerDeHelper#toJson(InternalSchema)} byte for byte.
   */
  public static String toJson(HoodieSchema schema) {
    return SerDeHelper.toJson(HoodieSchemaInternalSchemaBridge.toInternalSchema(schema));
  }

  /**
   * Serializes a history of schemas to JSON. Output format matches the legacy
   * {@link SerDeHelper#toJson(List)} byte for byte.
   */
  public static String toJsonHistory(List<HoodieSchema> schemas) {
    List<InternalSchema> converted = new ArrayList<>(schemas.size());
    for (HoodieSchema s : schemas) {
      converted.add(HoodieSchemaInternalSchemaBridge.toInternalSchema(s));
    }
    return SerDeHelper.toJson(converted);
  }

  /**
   * Parses a single-schema JSON blob (typically the {@code latest_schema} value
   * from commit metadata). Returns empty if the input is null/empty so callers
   * can pass through optional commit metadata fields.
   */
  public static Option<HoodieSchema> fromJson(String json) {
    Option<InternalSchema> internal = SerDeHelper.fromJson(json);
    if (!internal.isPresent()) {
      return Option.empty();
    }
    return Option.of(HoodieSchemaInternalSchemaBridge.toHoodieSchema(internal.get(), defaultRecordName(internal.get())));
  }

  /**
   * Variant of {@link #fromJson(String)} that lets the caller fix the record name
   * on the resulting HoodieSchema. Equivalent to the legacy
   * {@code SerDeHelper.fromJson(...).map(is -> InternalSchemaConverter.convert(is, recordName))}
   * pattern, collapsed into a single call.
   */
  public static Option<HoodieSchema> fromJson(String json, String recordName) {
    Option<InternalSchema> internal = SerDeHelper.fromJson(json);
    if (!internal.isPresent()) {
      return Option.empty();
    }
    return Option.of(HoodieSchemaInternalSchemaBridge.toHoodieSchema(internal.get(), recordName));
  }

  /**
   * Parses the history-schemas JSON layout (array of versioned schemas) and
   * returns them keyed by {@code version_id}. Field ids are preserved on each
   * returned HoodieSchema so the resulting map is directly usable by the
   * read/merge path.
   */
  public static TreeMap<Long, HoodieSchema> fromJsonHistory(String json) {
    TreeMap<Long, InternalSchema> internals = SerDeHelper.parseSchemas(json);
    TreeMap<Long, HoodieSchema> out = new TreeMap<>();
    for (Long versionId : internals.keySet()) {
      InternalSchema is = internals.get(versionId);
      HoodieSchema hs = HoodieSchemaInternalSchemaBridge.toHoodieSchema(is, defaultRecordName(is));
      out.put(versionId, hs);
    }
    return out;
  }

  /**
   * Appends a freshly-evolved schema to an existing serialized history blob and
   * returns the new blob. Mirrors {@link SerDeHelper#inheritSchemas(InternalSchema, String)}
   * — the {@code oldHistoryJson} is the prior {@code .hoodie/.schema/} contents
   * (or empty for the first commit).
   */
  public static String inheritHistory(HoodieSchema newSchema, String oldHistoryJson) {
    return SerDeHelper.inheritSchemas(
        HoodieSchemaInternalSchemaBridge.toInternalSchema(newSchema), oldHistoryJson);
  }

  /**
   * Resolves the schema-history entry that applies to a given version id — exact
   * match if present, else the largest entry strictly less than {@code versionId},
   * else {@code null}. HoodieSchema-shaped replacement for
   * {@link org.apache.hudi.internal.schema.utils.InternalSchemaUtils#searchSchema}.
   *
   * <p>Note: legacy returned {@code InternalSchema.getEmptyInternalSchema()} on
   * miss; this returns {@code null} so callers can choose their own empty
   * sentinel via {@link HoodieSchema#empty()}. Most callers null-check + fall
   * back, so the change is benign.</p>
   */
  public static HoodieSchema searchSchema(long versionId, java.util.TreeMap<Long, HoodieSchema> history) {
    if (history.containsKey(versionId)) {
      return history.get(versionId);
    }
    java.util.SortedMap<Long, HoodieSchema> headMap = history.headMap(versionId);
    return headMap.isEmpty() ? null : headMap.get(headMap.lastKey());
  }

  private static String defaultRecordName(InternalSchema internalSchema) {
    if (internalSchema == null || internalSchema.getRecord() == null) {
      return "hoodieSchema";
    }
    String name = internalSchema.getRecord().name();
    return name != null && !name.isEmpty() ? name : "hoodieSchema";
  }
}
