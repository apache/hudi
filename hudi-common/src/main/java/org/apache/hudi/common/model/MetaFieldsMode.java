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

package org.apache.hudi.common.model;

import org.apache.hudi.common.util.StringUtils;

/**
 * Which of Hudi's meta columns are physically populated on disk.
 *
 * <p>Selective modes exist so that tables that opt out of the default {@code populate.meta.fields=true}
 * can still keep the two columns that matter for downstream operations without paying for the other
 * three:
 *
 * <ul>
 *   <li>{@code _hoodie_commit_time} — required for incremental queries.</li>
 *   <li>{@code _hoodie_file_name} — useful for file-level pruning / investigation lookups.</li>
 * </ul>
 *
 * <p>The remaining three meta columns ({@code _hoodie_commit_seqno}, {@code _hoodie_record_key},
 * {@code _hoodie_partition_path}) are all-or-nothing — either populate every meta column ({@link #ALL})
 * or none of them beyond the two selectable ones. If you need any of the remaining columns, set
 * {@code hoodie.populate.meta.fields=true}.
 *
 * <p>This enum is the single source of truth for meta-column population. The legacy boolean
 * {@code hoodie.populate.meta.fields} is deprecated and consulted only when
 * {@code hoodie.meta.fields.mode} is absent, so that tables written before the mode property
 * existed keep their behavior:
 *
 * <ul>
 *   <li>{@code populate.meta.fields=true} (or absent) → {@link #ALL} — today's default.</li>
 *   <li>{@code populate.meta.fields=false} → {@link #NONE}.</li>
 * </ul>
 *
 * <p>On-disk representation: the enum {@link #name()} is persisted in {@code hoodie.properties}
 * under the property {@code hoodie.meta.fields.mode}.
 */
public enum MetaFieldsMode {
  /**
   * All five Hudi meta columns are populated — today's default.
   */
  ALL(true, true),

  /**
   * No Hudi meta columns are populated. Incremental queries are unsupported. File-level pruning
   * that depends on {@code _hoodie_file_name} is unsupported.
   */
  NONE(false, false),

  /**
   * Only {@code _hoodie_commit_time} is populated. Incremental queries remain functional; other
   * meta columns stay null on disk.
   */
  COMMIT_TIME_ONLY(true, false),

  /**
   * Only {@code _hoodie_file_name} is populated. Useful for file-level lookups and debugging;
   * incremental queries are unsupported.
   */
  FILE_NAME_ONLY(false, true),

  /**
   * Both {@code _hoodie_commit_time} and {@code _hoodie_file_name} are populated.
   */
  COMMIT_TIME_AND_FILE_NAME(true, true);

  private final boolean commitTimePopulated;
  private final boolean fileNamePopulated;

  MetaFieldsMode(boolean commitTimePopulated, boolean fileNamePopulated) {
    this.commitTimePopulated = commitTimePopulated;
    this.fileNamePopulated = fileNamePopulated;
  }

  public boolean isCommitTimePopulated() {
    return commitTimePopulated;
  }

  public boolean isFileNamePopulated() {
    return fileNamePopulated;
  }

  /**
   * @return true when all five meta columns are populated (i.e. this is {@link #ALL}). Selective
   * modes never populate {@code _hoodie_record_key}, {@code _hoodie_partition_path}, or
   * {@code _hoodie_commit_seqno}.
   */
  public boolean isRecordKeyPopulated() {
    return this == ALL;
  }

  /**
   * Resolve the effective mode. {@code hoodie.meta.fields.mode} is the source of truth; the
   * deprecated {@code hoodie.populate.meta.fields} boolean is a fallback for tables written before
   * the mode property existed. Precedence:
   *
   * <ul>
   *   <li>non-empty mode → the parsed enum value (the legacy boolean is not consulted).</li>
   *   <li>null/empty mode + {@code populateMetaFields=false} → {@link #NONE}.</li>
   *   <li>null/empty mode + {@code populateMetaFields=true} → {@link #ALL}.</li>
   * </ul>
   *
   * @param rawMode             raw {@code hoodie.meta.fields.mode} value; may be null or empty.
   * @param legacyPopulateMetaFields value of the deprecated {@code hoodie.populate.meta.fields}.
   * @throws IllegalArgumentException when the raw mode value does not match any enum value. This
   *         includes the pre-enum comma-separated format — callers that upgrade an old table must
   *         migrate the value through the hudi-cli.
   */
  public static MetaFieldsMode resolve(String rawMode, boolean legacyPopulateMetaFields) {
    if (StringUtils.isNullOrEmpty(rawMode)) {
      return legacyPopulateMetaFields ? ALL : NONE;
    }
    return parse(rawMode);
  }

  /**
   * Parse a raw {@code hoodie.meta.fields.mode} value into an enum constant, with a message that
   * lists the allowed values. Prefer this over {@link #valueOf(String)} for user-supplied input.
   */
  public static MetaFieldsMode parse(String rawMode) {
    try {
      return MetaFieldsMode.valueOf(rawMode.trim());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(
          "Unsupported value '%s' for hoodie.meta.fields.mode. Allowed values: %s, %s, %s, %s, %s.",
          rawMode, ALL, NONE, COMMIT_TIME_ONLY, FILE_NAME_ONLY, COMMIT_TIME_AND_FILE_NAME), e);
    }
  }

  /**
   * @return the equivalent value of the deprecated {@code hoodie.populate.meta.fields} boolean, so
   * that call sites not yet migrated to this enum keep observing consistent behavior.
   */
  public boolean toLegacyPopulateMetaFields() {
    return this == ALL;
  }
}
