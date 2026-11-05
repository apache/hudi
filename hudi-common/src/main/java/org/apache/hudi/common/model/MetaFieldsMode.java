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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.StringUtils;

import lombok.Getter;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

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
 * <p>This enum is the single source of truth for meta-column population. When the mode property is
 * absent, the {@code resolve} overloads fall back to the deprecated {@code
 * hoodie.populate.meta.fields} boolean so that tables written before the mode property existed keep
 * their behavior:
 *
 * <ul>
 *   <li>{@code populate.meta.fields=true} (or absent) → {@link #ALL} — today's default.</li>
 *   <li>{@code populate.meta.fields=false} → {@link #NONE}.</li>
 * </ul>
 *
 * <p>On-disk representation: the enum {@link #name()} is persisted in {@code hoodie.properties}
 * under the property {@code hoodie.meta.fields.mode}.
 */
@Getter
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

  /**
   * @return true when all five meta columns are populated (i.e. this is {@link #ALL}). Selective
   * modes never populate {@code _hoodie_record_key}, {@code _hoodie_partition_path}, or
   * {@code _hoodie_commit_seqno}.
   */
  public boolean isRecordKeyPopulated() {
    return this == ALL;
  }

  /**
   * @return true for the modes that populate some but not all meta columns, i.e. everything except
   * {@link #ALL} and {@link #NONE}.
   *
   * <p>These are the modes the deprecated {@code hoodie.populate.meta.fields} boolean cannot
   * express, so they are what callers gate on when a code path only understands all-or-nothing meta
   * fields — writer engines not yet wired for selective population, table versions that predate the
   * mode property, and validation that must not let a two-state writer speak for a five-state table.
   */
  public boolean isSelective() {
    return this != ALL && this != NONE;
  }

  /**
   * Resolve the configured meta-fields mode, falling back to the deprecated population boolean.
   *
   * <p>When {@link HoodieTableConfig#META_FIELDS_MODE} is set, it is always authoritative. Legacy
   * configs without the mode resolve to {@link #ALL} or {@link #NONE} from
   * {@link HoodieTableConfig#POPULATE_META_FIELDS} without mutating the config.
   */
  public static MetaFieldsMode resolve(HoodieConfig config) {
    String rawMode = config.getString(HoodieTableConfig.META_FIELDS_MODE);
    if (StringUtils.isNullOrEmpty(rawMode)) {
      return config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS) ? ALL : NONE;
    }
    return parse(rawMode);
  }

  /**
   * Resolve the configured meta-fields mode from raw properties, falling back to the deprecated
   * population boolean without mutating the properties.
   */
  public static MetaFieldsMode resolve(Properties props) {
    String rawMode = props.getProperty(HoodieTableConfig.META_FIELDS_MODE.key());
    if (StringUtils.isNullOrEmpty(rawMode)) {
      return Boolean.parseBoolean(props.getProperty(
          HoodieTableConfig.POPULATE_META_FIELDS.key(),
          HoodieTableConfig.POPULATE_META_FIELDS.defaultValue().toString()))
          ? ALL : NONE;
    }
    return parse(rawMode);
  }

  /**
   * Resolve the configured meta-fields mode from a string map, falling back to the deprecated
   * population boolean without mutating the map.
   */
  public static MetaFieldsMode resolve(Map<String, String> propsMap) {
    String rawMode = propsMap.get(HoodieTableConfig.META_FIELDS_MODE.key());
    if (StringUtils.isNullOrEmpty(rawMode)) {
      return Boolean.parseBoolean(propsMap.getOrDefault(
          HoodieTableConfig.POPULATE_META_FIELDS.key(),
          HoodieTableConfig.POPULATE_META_FIELDS.defaultValue().toString()))
          ? ALL : NONE;
    }
    return parse(rawMode);
  }

  /**
   * Parse a raw {@code hoodie.meta.fields.mode} value into an enum constant, with a message that
   * lists the allowed values.
   */
  private static MetaFieldsMode parse(String rawMode) {
    try {
      // Case-insensitive: users hand-editing hoodie.properties or passing write options should not
      // have to match the enum's casing exactly.
      return MetaFieldsMode.valueOf(rawMode.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(String.format(
          "Unsupported value '%s' for hoodie.meta.fields.mode. Allowed values: %s.",
          rawMode, Arrays.stream(values()).map(Enum::name).collect(Collectors.joining(", "))), e);
    }
  }

  /**
   * @return the equivalent value of the deprecated {@code hoodie.populate.meta.fields} boolean, so
   * that call sites not yet migrated to this enum keep observing consistent behavior.
   */
  public boolean toLegacyPopulateMetaFields() {
    return this == ALL;
  }

  public static MetaFieldsMode fromLegacyPopulateMetaFields(Boolean populateMetaFields) {
    return populateMetaFields == null || populateMetaFields ? MetaFieldsMode.ALL : MetaFieldsMode.NONE;
  }

  /**
   * @return true when this mode populates at least one meta column that {@code other} does not.
   *
   * <p>Meta-field population is a physical-storage decision baked into files at write time, so it
   * can never be widened for an existing table: earlier commits would be missing columns that later
   * commits have, and readers cannot tell the two apart. Every transition that adds a column is
   * therefore rejected — {@code NONE -> COMMIT_TIME_ONLY} and
   * {@code FILE_NAME_ONLY -> COMMIT_TIME_AND_FILE_NAME} just as much as {@code NONE -> ALL}.
   *
   * <p>Narrowing is not flagged here, because the two callers treat it differently:
   *
   * <ul>
   *   <li>A writer must match the table exactly. It cannot narrow either — the mode is a table
   *       property and runtime validation rejects any mismatch.</li>
   *   <li>The sanctioned mutation paths — hudi-cli and upgrade — may narrow, and only narrow.
   *       Dropping a meta column leaves earlier files carrying values nothing reads, which is
   *       recoverable; adding one leaves later files claiming a column earlier files lack, which is
   *       not. A caller changing the mode must therefore reject any transition where the new mode
   *       {@code isWiderThan} the old.</li>
   * </ul>
   */
  public boolean isWiderThan(MetaFieldsMode other) {
    if (other == null) {
      return false;
    }
    return (commitTimePopulated && !other.commitTimePopulated)
        || (fileNamePopulated && !other.fileNamePopulated)
        || (isRecordKeyPopulated() && !other.isRecordKeyPopulated());
  }
}
