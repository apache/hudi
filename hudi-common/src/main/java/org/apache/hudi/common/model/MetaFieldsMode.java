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
 * <p>Mapping to the legacy {@code hoodie.populate.meta.fields} boolean:
 *
 * <ul>
 *   <li>{@link #ALL} corresponds to {@code populate.meta.fields=true} — today's default.</li>
 *   <li>Every other value corresponds to {@code populate.meta.fields=false} plus a selective opt-in.</li>
 * </ul>
 *
 * <p>On-disk representation: the enum {@link #name()} is persisted in {@code hoodie.properties}
 * under the property {@code hoodie.meta.fields.mode}. For backward compatibility, older tables that
 * predate this property fall back to {@link #ALL} or {@link #NONE} based on the legacy boolean.
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
   * Auto-derive a mode from the legacy {@code hoodie.populate.meta.fields} boolean and the raw
   * {@code hoodie.meta.fields.mode} property value. Precedence:
   *
   * <ul>
   *   <li>{@code populateMetaFields=true} → {@link #ALL} (the mode property is ignored).</li>
   *   <li>{@code populateMetaFields=false} + null/empty mode → {@link #NONE}.</li>
   *   <li>{@code populateMetaFields=false} + non-empty mode → the parsed enum value.</li>
   * </ul>
   *
   * <p>Throws {@link IllegalArgumentException} when the raw mode value does not match any enum
   * value. This includes the pre-enum comma-separated format — callers that upgrade an old table
   * must migrate the value through the hudi-cli.
   */
  public static MetaFieldsMode fromConfig(boolean populateMetaFields, String rawMode) {
    if (populateMetaFields) {
      return ALL;
    }
    if (StringUtils.isNullOrEmpty(rawMode)) {
      return NONE;
    }
    try {
      return MetaFieldsMode.valueOf(rawMode.trim());
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(
          "Unsupported value '%s' for hoodie.meta.fields.mode. Allowed values: %s, %s, %s, %s.",
          rawMode, COMMIT_TIME_ONLY, FILE_NAME_ONLY, COMMIT_TIME_AND_FILE_NAME, NONE), e);
    }
  }
}
