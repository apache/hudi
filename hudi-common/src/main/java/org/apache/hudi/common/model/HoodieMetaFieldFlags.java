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

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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

  private final boolean instantTimePopulated;
  private final boolean commitSeqNoPopulated;
  private final boolean recordKeyPopulated;
  private final boolean partitionPathPopulated;
  private final boolean fileNamePopulated;

  private HoodieMetaFieldFlags(boolean instantTimePopulated, boolean commitSeqNoPopulated,
                               boolean recordKeyPopulated, boolean partitionPathPopulated,
                               boolean fileNamePopulated) {
    this.instantTimePopulated = instantTimePopulated;
    this.commitSeqNoPopulated = commitSeqNoPopulated;
    this.recordKeyPopulated = recordKeyPopulated;
    this.partitionPathPopulated = partitionPathPopulated;
    this.fileNamePopulated = fileNamePopulated;
  }

  /**
   * Returns an instance where all meta fields are populated.
   */
  public static HoodieMetaFieldFlags allPopulated() {
    return ALL_POPULATED;
  }

  /**
   * Returns an instance where no meta fields are populated.
   */
  public static HoodieMetaFieldFlags nonePopulated() {
    return NONE_POPULATED;
  }

  /**
   * Creates an instance from a set of excluded field names, using the standard
   * {@link HoodieRecord#HOODIE_META_COLUMNS} ordering.
   *
   * @param excluded set of meta field names to exclude (e.g. "_hoodie_record_key")
   * @return HoodieMetaFieldFlags with excluded fields marked as not populated
   */
  public static HoodieMetaFieldFlags fromExcludedFields(Set<String> excluded) {
    if (excluded == null || excluded.isEmpty()) {
      return ALL_POPULATED;
    }
    List<String> metaColumns = HoodieRecord.HOODIE_META_COLUMNS;
    return new HoodieMetaFieldFlags(
        !excluded.contains(metaColumns.get(0)),
        !excluded.contains(metaColumns.get(1)),
        !excluded.contains(metaColumns.get(2)),
        !excluded.contains(metaColumns.get(3)),
        !excluded.contains(metaColumns.get(4))
    );
  }

  /**
   * Resolves the flags from a {@link HoodieConfig} (typically the merged write
   * config or the table config). Reads {@link HoodieTableConfig#POPULATE_META_FIELDS}
   * and {@link HoodieTableConfig#META_FIELDS_EXCLUDE_LIST}; the exclusion list is
   * a comma-separated list of meta-field names with whitespace tolerated.
   */
  public static HoodieMetaFieldFlags fromConfig(HoodieConfig config) {
    boolean populateMetaFields = config.getBooleanOrDefault(HoodieTableConfig.POPULATE_META_FIELDS);
    if (!populateMetaFields) {
      return NONE_POPULATED;
    }
    String value = config.getString(HoodieTableConfig.META_FIELDS_EXCLUDE_LIST);
    if (value == null || value.trim().isEmpty()) {
      return ALL_POPULATED;
    }
    Set<String> excluded = new HashSet<>();
    for (String field : value.split(",")) {
      String trimmed = field.trim();
      if (!trimmed.isEmpty()) {
        excluded.add(trimmed);
      }
    }
    return fromExcludedFields(excluded);
  }

  public boolean isInstantTimePopulated() {
    return instantTimePopulated;
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
}
