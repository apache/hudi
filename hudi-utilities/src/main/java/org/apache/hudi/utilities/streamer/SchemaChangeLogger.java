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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCompatibilityChecker;
import org.apache.hudi.common.schema.HoodieSchemaCompatibilityChecker.SchemaPairCompatibility;
import org.apache.hudi.common.schema.HoodieSchemaField;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Logs schema change events in DeltaStreamer in a way that is easy to scan when an incident
 * happens. When a customer changes a column's type, adds a column, removes a column, or otherwise
 * mutates the schema (either through a schema-registry update or by editing a schema file consumed
 * via {@link org.apache.hudi.utilities.schema.FilebasedSchemaProvider}), oncall typically wants to
 * answer three questions quickly:
 *
 * <ol>
 *   <li>What changed? (added/removed/type-changed fields)</li>
 *   <li>Is the change compatible with what Hudi expects?</li>
 *   <li>If a write is failing immediately after, is the schema change the likely culprit?</li>
 * </ol>
 *
 * <p>This logger emits a single structured INFO line per detected change with a per-field diff,
 * plus a WARN line when the change is classified as INCOMPATIBLE per Avro reader/writer rules
 * (e.g. {@code double -> decimal} type change, removal of a required field, etc.). The full
 * pre/post schema text is only emitted at WARN level, so log volume stays low for routine additive
 * evolution but bumps up for the cases that need attention.
 *
 * <p>Comparison is shallow (top-level fields). Nested record evolution is summarised at the
 * top-level field's type change. The full schemas are still logged on INCOMPATIBLE so an operator
 * can drill down.
 */
public final class SchemaChangeLogger {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeLogger.class);
  private static final String NULL_PLACEHOLDER = "null";

  private SchemaChangeLogger() {
    // Utility class — no instances.
  }

  /**
   * Logs the difference between {@code previous} and {@code current} for a given role
   * ("Source" or "Target") if they differ.
   *
   * @param role        Human-readable label, used as a prefix on the log line. Convention:
   *                    {@code "Source"} for the schema records arrive in, {@code "Target"} for
   *                    the schema Hudi will write.
   * @param previous    The previously-active schema for this role, or {@code null} if this is the
   *                    first observation.
   * @param current     The current schema (just observed). May be {@code null} if the role isn't
   *                    being used in this batch.
   * @param providerClass The fully-qualified class name of the SchemaProvider that produced
   *                    {@code current}. Logged so operators can tell whether the schema came from
   *                    the schema registry, a file, an upstream Hudi table, etc. May be
   *                    {@code null} if not known.
   */
  public static void logIfChanged(String role,
                                  HoodieSchema previous,
                                  HoodieSchema current,
                                  String providerClass) {
    if (current == null) {
      return;
    }
    if (Objects.equals(previous, current)) {
      return;
    }
    String provider = providerClass == null ? "unknown" : providerClass;
    if (previous == null) {
      LOG.info("{} schema initialised. provider={}, fields={}",
          role, provider, summariseTopLevel(current));
      return;
    }

    SchemaDiff diff = computeTopLevelDiff(previous, current);

    if (diff.isEmpty()) {
      // Different identity, structurally identical. Almost always a fingerprint cache miss.
      LOG.info("{} schema reference changed but content is identical. provider={}",
          role, provider);
      return;
    }

    Compatibility compat = classify(previous, current);
    LOG.info(
        "{} schema changed [{}]. provider={}, added={}, removed={}, type_changes={}",
        role, compat.label, provider, diff.added, diff.removed, diff.typeChanges);

    if (compat == Compatibility.INCOMPATIBLE) {
      // Log the full schemas only on the unsafe path so we have everything needed to investigate.
      LOG.warn(
          "{} schema change is INCOMPATIBLE per Avro reader/writer rules. Reason: {}\n"
              + "Previous schema:\n{}\nNew schema:\n{}",
          role,
          compat.detail,
          previous.toString(true),
          current.toString(true));
    }
  }

  // -------- internals --------

  private static SchemaDiff computeTopLevelDiff(HoodieSchema previous, HoodieSchema current) {
    SchemaDiff diff = new SchemaDiff();
    if (!previous.hasFields() || !current.hasFields()) {
      // Top-level isn't a record on at least one side. Treat as a single "type change" for
      // visibility; the full schemas will be logged if the compatibility check flags it.
      diff.typeChanges.put("<root>",
          new TypeChange(typeOf(previous), typeOf(current)));
      return diff;
    }

    Map<String, HoodieSchemaField> prevByName = new LinkedHashMap<>();
    for (HoodieSchemaField f : previous.getFields()) {
      prevByName.put(f.name(), f);
    }
    Map<String, HoodieSchemaField> currByName = new LinkedHashMap<>();
    for (HoodieSchemaField f : current.getFields()) {
      currByName.put(f.name(), f);
    }

    for (HoodieSchemaField currField : current.getFields()) {
      HoodieSchemaField prevField = prevByName.get(currField.name());
      if (prevField == null) {
        diff.added.add(currField.name() + ":" + typeOf(currField.schema()));
      } else if (!schemaEquals(prevField.schema(), currField.schema())) {
        diff.typeChanges.put(currField.name(),
            new TypeChange(typeOf(prevField.schema()), typeOf(currField.schema())));
      }
    }
    for (HoodieSchemaField prevField : previous.getFields()) {
      if (!currByName.containsKey(prevField.name())) {
        diff.removed.add(prevField.name() + ":" + typeOf(prevField.schema()));
      }
    }
    return diff;
  }

  private static Compatibility classify(HoodieSchema previous, HoodieSchema current) {
    // Standard Avro reader/writer compatibility. The semantic question is "can a reader using the
    // current schema decode data written under the previous schema?". If yes → COMPATIBLE.
    try {
      SchemaPairCompatibility result =
          HoodieSchemaCompatibilityChecker.checkReaderWriterCompatibility(current, previous, false);
      switch (result.getType()) {
        case COMPATIBLE:
          return Compatibility.COMPATIBLE;
        case INCOMPATIBLE:
        default:
          return new Compatibility("INCOMPATIBLE", result.getDescription());
      }
    } catch (Exception e) {
      // Compatibility check should never throw, but guard against any odd input shape so logging
      // never breaks ingestion.
      return new Compatibility("UNKNOWN", "compat check failed: " + e.getClass().getSimpleName());
    }
  }

  private static String summariseTopLevel(HoodieSchema schema) {
    if (!schema.hasFields()) {
      return typeOf(schema);
    }
    List<String> parts = new ArrayList<>();
    for (HoodieSchemaField f : schema.getFields()) {
      parts.add(f.name() + ":" + typeOf(f.schema()));
    }
    return parts.toString();
  }

  private static String typeOf(HoodieSchema s) {
    if (s == null) {
      return NULL_PLACEHOLDER;
    }
    // Compact rendering: include the type name; for unions, list non-null branches; for records,
    // list the record name. Avoids dumping the full schema JSON in the diff line.
    switch (s.getType()) {
      case UNION:
        List<HoodieSchema> branches = s.getTypes();
        List<String> nonNull = new ArrayList<>();
        boolean hasNull = false;
        for (HoodieSchema b : branches) {
          if (b.getType().name().equals("NULL")) {
            hasNull = true;
          } else {
            nonNull.add(typeOf(b));
          }
        }
        if (hasNull && nonNull.size() == 1) {
          return "?" + nonNull.get(0);
        }
        return "union" + nonNull;
      case RECORD:
        return "record(" + s.getName() + ")";
      case ARRAY:
        return "array";
      case MAP:
        return "map";
      default:
        return s.getType().name().toLowerCase();
    }
  }

  private static boolean schemaEquals(HoodieSchema a, HoodieSchema b) {
    // Use Hudi's HoodieSchema equality, which respects type structure. Avoids relying on the more
    // expensive toString-based comparison.
    return Objects.equals(a, b);
  }

  // -------- inner types --------

  private static final class SchemaDiff {
    final List<String> added = new ArrayList<>();
    final List<String> removed = new ArrayList<>();
    final Map<String, TypeChange> typeChanges = new LinkedHashMap<>();

    boolean isEmpty() {
      return added.isEmpty() && removed.isEmpty() && typeChanges.isEmpty();
    }
  }

  private static final class TypeChange {
    final String oldType;
    final String newType;

    TypeChange(String oldType, String newType) {
      this.oldType = oldType;
      this.newType = newType;
    }

    @Override
    public String toString() {
      return oldType + "->" + newType;
    }
  }

  private static final class Compatibility {
    static final Compatibility COMPATIBLE = new Compatibility("COMPATIBLE", "");

    final String label;
    final String detail;

    Compatibility(String label, String detail) {
      this.label = label;
      this.detail = detail;
    }
  }

  /** Test-only accessor so unit tests can drive the diff logic without hitting SLF4J. */
  static String renderDiffForTest(HoodieSchema previous, HoodieSchema current) {
    SchemaDiff diff = computeTopLevelDiff(previous, current);
    return "added=" + diff.added + ", removed=" + diff.removed + ", type_changes=" + diff.typeChanges;
  }

  /** Test-only accessor for compatibility classification. */
  static String classifyForTest(HoodieSchema previous, HoodieSchema current) {
    return classify(previous, current).label;
  }
}
