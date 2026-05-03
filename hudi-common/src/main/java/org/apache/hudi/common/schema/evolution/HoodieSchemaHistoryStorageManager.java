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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.internal.schema.io.FileBasedInternalSchemaStorageManager;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.util.List;

/**
 * HoodieSchema-shaped façade around the {@code .hoodie/.schema/} schema-history
 * storage layer. Delegates to {@link FileBasedInternalSchemaStorageManager} so the
 * on-disk paths, file naming, and JSON layout are all preserved verbatim — a hard
 * requirement for backward compatibility with tables created by prior Hudi versions.
 *
 * <p>String-based methods ({@link #persistHistorySchemaStr},
 * {@link #getHistorySchemaStr}, {@link #cleanOldFiles}) pass through unchanged.
 * Only {@link #getSchemaByKey(String)} crosses the schema-type boundary and uses
 * {@link HoodieSchemaInternalSchemaBridge} to return a HoodieSchema with field ids
 * preserved.</p>
 */
public class HoodieSchemaHistoryStorageManager {

  /**
   * Subdirectory name under {@code .hoodie/} that holds the schema-history files.
   * Matches {@link FileBasedInternalSchemaStorageManager#SCHEMA_NAME} verbatim.
   */
  public static final String SCHEMA_NAME = FileBasedInternalSchemaStorageManager.SCHEMA_NAME;

  private final FileBasedInternalSchemaStorageManager delegate;

  public HoodieSchemaHistoryStorageManager(HoodieStorage storage, StoragePath baseTablePath) {
    this.delegate = new FileBasedInternalSchemaStorageManager(storage, baseTablePath);
  }

  public HoodieSchemaHistoryStorageManager(HoodieTableMetaClient metaClient) {
    this.delegate = new FileBasedInternalSchemaStorageManager(metaClient);
  }

  /**
   * Persists a history-schema JSON blob keyed by the commit instant time. The
   * input JSON is expected to be in the layout produced by
   * {@link HoodieSchemaSerDe#toJsonHistory} or
   * {@link HoodieSchemaSerDe#inheritHistory}.
   */
  public void persistHistorySchemaStr(String instantTime, String historySchemaStr) {
    delegate.persistHistorySchemaStr(instantTime, historySchemaStr);
  }

  /**
   * Removes schema-history files whose instant times are not in the valid-commits
   * set. Safety mechanism for archival cleanup.
   */
  public void cleanOldFiles(List<String> validateCommits) {
    delegate.cleanOldFiles(validateCommits);
  }

  /**
   * Returns the most recent history-schema JSON blob across all commits, or empty
   * string if no schema history exists.
   */
  public String getHistorySchemaStr() {
    return delegate.getHistorySchemaStr();
  }

  /**
   * Same as {@link #getHistorySchemaStr()} but only considers files whose
   * instant time appears in {@code validCommits}.
   */
  public String getHistorySchemaStrByGivenValidCommits(List<String> validCommits) {
    return delegate.getHistorySchemaStrByGivenValidCommits(validCommits);
  }

  /**
   * Resolves a single schema by its version id (typically a commit-instant
   * timestamp). Returns empty if no schema with that version is in the history.
   */
  @SuppressWarnings("unchecked")
  public Option<HoodieSchema> getSchemaByKey(String versionId) {
    Option<InternalSchema> internal = delegate.getSchemaByKey(versionId);
    if (!internal.isPresent()) {
      return Option.empty();
    }
    InternalSchema is = internal.get();
    String recordName = (is.getRecord() != null && is.getRecord().name() != null)
        ? is.getRecord().name() : "hoodieSchema";
    return Option.of(HoodieSchemaInternalSchemaBridge.toHoodieSchema(is, recordName));
  }
}
