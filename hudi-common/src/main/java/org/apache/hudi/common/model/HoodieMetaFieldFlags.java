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
import org.apache.hudi.exception.HoodieException;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

/**
 * Encapsulates which individual meta fields should be populated during writes.
 * Provides named accessor methods for better readability compared to boolean array indexing.
 *
 * <p>The flags correspond to the 5 standard Hudi meta columns in {@link HoodieRecord#HOODIE_META_COLUMNS}:
 * commit_time, commit_seqno, record_key, partition_path, file_name.
 */
public class HoodieMetaFieldFlags implements Serializable {

  private static final long serialVersionUID = 1L;

  private static final HoodieMetaFieldFlags ALL_POPULATED = new HoodieMetaFieldFlags(true, true, true, true, true);
  private static final HoodieMetaFieldFlags NONE_POPULATED = new HoodieMetaFieldFlags(false, false, false, false, false);

  private final boolean commitTimePopulated;
  private final boolean commitSeqNoPopulated;
  private final boolean recordKeyPopulated;
  private final boolean partitionPathPopulated;
  private final boolean fileNamePopulated;

  private HoodieMetaFieldFlags(boolean commitTimePopulated, boolean commitSeqNoPopulated,
                               boolean recordKeyPopulated, boolean partitionPathPopulated,
                               boolean fileNamePopulated) {
    this.commitTimePopulated = commitTimePopulated;
    this.commitSeqNoPopulated = commitSeqNoPopulated;
    this.recordKeyPopulated = recordKeyPopulated;
    this.partitionPathPopulated = partitionPathPopulated;
    this.fileNamePopulated = fileNamePopulated;
  }

  /**
   * Returns an instance where all meta fields are populated.
   *
   * <p>Production code should obtain flags via {@link HoodieTableConfig#getHoodieMetaFieldFlags()}
   * so the on-disk persisted state is the single source of truth. This factory exists for
   * tests that mock {@code HoodieTableConfig} and need to stub the flags getter explicitly.
   */
  public static HoodieMetaFieldFlags allPopulated() {
    return ALL_POPULATED;
  }

  /**
   * Returns an instance where no meta fields are populated.
   *
   * <p>Package-private; see {@link #allPopulated()}.
   */
  static HoodieMetaFieldFlags nonePopulated() {
    return NONE_POPULATED;
  }

  /**
   * Creates an instance from a set of excluded field names.
   *
   * <p>Package-private; see {@link #allPopulated()}.
   *
   * @param excluded set of meta field names to exclude (e.g. "_hoodie_record_key")
   * @return HoodieMetaFieldFlags with excluded fields marked as not populated
   * @throws IllegalArgumentException if {@code excluded} contains names that are not in
   *         {@link HoodieRecord#HOODIE_META_COLUMNS}, so configuration typos fail fast
   *         rather than being silently ignored.
   */
  static HoodieMetaFieldFlags fromExcludedFields(Set<String> excluded) {
    if (excluded == null || excluded.isEmpty()) {
      return ALL_POPULATED;
    }
    Set<String> unknown = new HashSet<>(excluded);
    unknown.removeAll(HoodieRecord.HOODIE_META_COLUMNS);
    if (!unknown.isEmpty()) {
      throw new IllegalArgumentException(
          "Unknown meta field name(s) in exclusion list: " + unknown
              + ". Valid names are: " + HoodieRecord.HOODIE_META_COLUMNS);
    }
    return new HoodieMetaFieldFlags(
        !excluded.contains(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
        !excluded.contains(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD),
        !excluded.contains(HoodieRecord.RECORD_KEY_METADATA_FIELD),
        !excluded.contains(HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        !excluded.contains(HoodieRecord.FILENAME_METADATA_FIELD)
    );
  }

  /**
   * Resolves the flags from a {@link HoodieConfig} (typically the merged write
   * config or the table config). Reads {@link HoodieTableConfig#POPULATE_META_FIELDS}
   * and {@link HoodieTableConfig#META_FIELDS_EXCLUDE_LIST}; the exclusion list is
   * a comma-separated list of meta-field names with whitespace tolerated.
   *
   * <p><b>Internal only.</b> The intended (and only supported) caller is
   * {@link HoodieTableConfig#getHoodieMetaFieldFlags()}. All write- and read-side code
   * should reach the flags through {@code HoodieTableConfig.getHoodieMetaFieldFlags()}
   * so the persisted table state remains the single source of truth.
   */
  public static HoodieMetaFieldFlags fromConfig(HoodieConfig config) {
    boolean populateMetaFields = config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
    if (!populateMetaFields) {
      return NONE_POPULATED;
    }
    Set<String> excluded = parseExcludeList(config.getString(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST));
    return fromExcludedFields(excluded);
  }

  /**
   * Parses a comma-separated meta-field exclusion list into a set of trimmed names.
   * Returns an empty set for null or blank input. Used as the single parsing point
   * for {@link HoodieTableConfig#META_FIELDS_EXCLUDE_LIST} so callers do not duplicate
   * split/trim logic.
   */
  public static Set<String> parseExcludeList(String value) {
    Set<String> excluded = new HashSet<>();
    if (value == null || value.trim().isEmpty()) {
      return excluded;
    }
    for (String field : value.split(",")) {
      String trimmed = field.trim();
      if (!trimmed.isEmpty()) {
        excluded.add(trimmed);
      }
    }
    return excluded;
  }

  public boolean isCommitTimePopulated() {
    return commitTimePopulated;
  }

  public boolean isCommitSeqNoPopulated() {
    return commitSeqNoPopulated;
  }

  public boolean isRecordKeyPopulated() {
    return recordKeyPopulated;
  }

  public boolean isPartitionPathPopulated() {
    return partitionPathPopulated;
  }

  public boolean isFileNamePopulated() {
    return fileNamePopulated;
  }

  /**
   * @return {@code true} if a key generator is needed at read/write time to reconstruct the
   *         record key or partition path. The key generator derives both from source fields,
   *         so a single instance is needed if either {@code _hoodie_record_key} or
   *         {@code _hoodie_partition_path} is not populated on disk.
   */
  public boolean isKeyGeneratorRequired() {
    return !recordKeyPopulated || !partitionPathPopulated;
  }

  /**
   * Validates a transition from one (populateMetaFields, excludeList) state to another.
   * The set of valid states forms a lattice that can only move in one direction:
   *
   * <ul>
   *   <li>{@code populate=true,exclude=X} -> {@code populate=true,exclude=Y}: allowed iff
   *       {@code Y} is a superset of {@code X} (i.e. transitions can only widen the set
   *       of excluded fields, never narrow it). Reintroducing a previously-excluded field
   *       would lie to readers about historical files written while it was null.</li>
   *   <li>{@code populate=true,*} -> {@code populate=false}: always allowed. The exclude
   *       list is irrelevant once meta fields are disabled entirely.</li>
   *   <li>{@code populate=false} -> {@code populate=true}: rejected. Historical files
   *       have empty meta fields and cannot be retro-populated.</li>
   *   <li>{@code populate=false} -> {@code populate=false}: allowed (no-op).</li>
   * </ul>
   *
   * <p>Both exclude sets must contain only names from {@link HoodieRecord#HOODIE_META_COLUMNS};
   * an unknown name in {@code newExclude} is rejected so config typos fail fast.
   *
   * @throws HoodieException if the transition is not allowed
   * @throws IllegalArgumentException if {@code newExclude} contains an unknown meta-field name
   */
  public static void validateTransition(boolean oldPopulate, Set<String> oldExclude,
                                        boolean newPopulate, Set<String> newExclude) {
    Set<String> safeOldExclude = oldExclude == null ? new HashSet<>() : oldExclude;
    Set<String> safeNewExclude = newExclude == null ? new HashSet<>() : newExclude;

    // Fail fast on unknown names in the new exclude list (typos).
    Set<String> unknownNew = new HashSet<>(safeNewExclude);
    unknownNew.removeAll(HoodieRecord.HOODIE_META_COLUMNS);
    if (!unknownNew.isEmpty()) {
      throw new IllegalArgumentException(
          "Unknown meta field name(s) in exclusion list: " + new TreeSet<>(unknownNew)
              + ". Valid names are: " + HoodieRecord.HOODIE_META_COLUMNS);
    }

    if (!oldPopulate) {
      // Absorbing state: once populate=false, the only allowed transition is staying false.
      if (newPopulate) {
        throw new HoodieException(
            "Cannot transition " + HoodieTableConfig.POPULATE_META_FIELDS.key()
                + " from false to true. Historical files written with meta fields disabled "
                + "cannot be retro-populated, which would corrupt reads that rely on meta "
                + "field availability.");
      }
      return;
    }

    if (!newPopulate) {
      // populate=true -> populate=false is always allowed; the prior exclude list becomes
      // irrelevant once meta fields are disabled entirely.
      return;
    }

    // populate=true -> populate=true: new exclude set must be a (non-strict) superset of old.
    Set<String> removed = new HashSet<>(safeOldExclude);
    removed.removeAll(safeNewExclude);
    if (!removed.isEmpty()) {
      throw new HoodieException(
          "Cannot remove field(s) " + new TreeSet<>(removed) + " from "
              + HoodieTableConfig.META_FIELDS_EXCLUDE_LIST.key()
              + ". The exclude list is monotonic - fields can only be added, never removed, "
              + "because historical files written while a field was excluded contain null for "
              + "that field and cannot be retro-populated. Current exclude list: "
              + new TreeSet<>(safeOldExclude) + ", attempted: " + new TreeSet<>(safeNewExclude) + ".");
    }
  }

  /**
   * @return true if at least one of the 5 meta fields is populated. Equivalent to the
   *         long-standing {@code populateMetaFields} cardinal switch: false only when
   *         meta fields are entirely disabled (or every field is in the exclusion list).
   */
  public boolean isAnyPopulated() {
    return commitTimePopulated || commitSeqNoPopulated || recordKeyPopulated
        || partitionPathPopulated || fileNamePopulated;
  }

  /**
   * @return {@code commitTime} if {@link #isCommitTimePopulated()}, else {@code null}.
   */
  public String getCommitTime(String commitTime) {
    return commitTimePopulated ? commitTime : null;
  }

  /**
   * @return {@code commitSeqNo} if {@link #isCommitSeqNoPopulated()}, else {@code null}.
   */
  public String getCommitSeqNo(String commitSeqNo) {
    return commitSeqNoPopulated ? commitSeqNo : null;
  }

  /**
   * @return {@code recordKey} if {@link #isRecordKeyPopulated()}, else {@code null}.
   */
  public String getRecordKey(String recordKey) {
    return recordKeyPopulated ? recordKey : null;
  }

  /**
   * @return {@code partitionPath} if {@link #isPartitionPathPopulated()}, else {@code null}.
   */
  public String getPartitionPath(String partitionPath) {
    return partitionPathPopulated ? partitionPath : null;
  }

  /**
   * @return {@code fileName} if {@link #isFileNamePopulated()}, else {@code null}.
   */
  public String getFileName(String fileName) {
    return fileNamePopulated ? fileName : null;
  }
}
