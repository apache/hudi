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

import java.util.Locale;

/**
 * Position-change selector for column-level schema mutations.
 *
 * <p>HoodieSchema-side replacement for
 * {@code TableChange.ColumnPositionChange.ColumnPositionType} — same semantics,
 * decoupled from the {@code internal.schema} package so callers can consume the
 * evolution API without depending on the legacy type system. Callers that still
 * need the legacy type can convert via {@link #toLegacy()}.</p>
 *
 * <ul>
 *   <li>{@link #FIRST} — move the column to the first position within its enclosing struct.
 *   <li>{@link #BEFORE} — place the column immediately before a reference column in the same struct.
 *   <li>{@link #AFTER} — place the column immediately after a reference column in the same struct.
 *   <li>{@link #NO_OPERATION} — leave the column at its current position.
 * </ul>
 */
public enum ColumnPositionType {
  FIRST,
  BEFORE,
  AFTER,
  NO_OPERATION;

  public static ColumnPositionType fromValue(String value) {
    switch (value.toLowerCase(Locale.ROOT)) {
      case "first":
        return FIRST;
      case "before":
        return BEFORE;
      case "after":
        return AFTER;
      case "no_operation":
        return NO_OPERATION;
      default:
        throw new IllegalArgumentException(
            String.format("only support first/before/after but found: %s", value));
    }
  }

  /**
   * Converts to the legacy {@code internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType}
   * for delegation into the existing applier. This bridge exists during the
   * InternalSchema → HoodieSchema migration and will be removed once the applier is
   * rewritten on pure HoodieSchema (Phase 5).
   */
  public org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType toLegacy() {
    switch (this) {
      case FIRST:
        return org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.FIRST;
      case BEFORE:
        return org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.BEFORE;
      case AFTER:
        return org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.AFTER;
      case NO_OPERATION:
      default:
        return org.apache.hudi.internal.schema.action.TableChange.ColumnPositionChange.ColumnPositionType.NO_OPERATION;
    }
  }
}
