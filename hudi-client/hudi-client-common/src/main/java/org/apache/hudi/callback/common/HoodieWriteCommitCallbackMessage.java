/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.callback.common;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.callback.util.HoodieWriteCommitCallbackUtil;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.util.Lazy;
import org.apache.hudi.common.util.Option;

import lombok.AccessLevel;
import lombok.Getter;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Base callback message, which contains commitTime and tableName only for now.
 */
@Getter
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public class HoodieWriteCommitCallbackMessage implements Serializable {

  private static final long serialVersionUID = -3033643980627719561L;

  /**
   * CommitTime for one batch write, this is required.
   */
  private final String commitTime;

  /**
   * Table name this batch commit to.
   */
  private final String tableName;

  /**
   * BathPath the table located.
   */
  private final String basePath;

  /**
   * Statistics about Hoodie write operation.
   */
  private final List<HoodieWriteStat> hoodieWriteStat;

  /**
   * Action Type of the commit.
   */
  private final Option<String> commitActionType;

  /**
   * Extra metadata in the commit.
   */
  private final Option<Map<String, String>> extraMetadata;

  /**
   * Previous base file paths keyed by fileId, derived from {@link #hoodieWriteStat} and the
   * {@link BaseFileOnlyView} handed over by the write client, so that callback
   * implementations don't have to rebuild a view themselves. Empty for inserts and for
   * callers that don't supply a view.
   *
   * <p>Holds the resolved map once {@link #getPrevFilePaths()} has run, and stays null until
   * then. Not transient: this is the copy that crosses Java serialization, which is why
   * {@link #writeObject} forces resolution before writing. Excluded from the generated
   * getters so it is published only through {@link #getPrevFilePaths()}.
   */
  @Getter(AccessLevel.NONE)
  private volatile Map<String, PrevFilePaths> prevFilePaths;

  /**
   * Resolves {@link #prevFilePaths} on demand. Resolution is deferred until the first
   * {@link #getPrevFilePaths()} call, so a callback that never reads the previous paths pays
   * nothing (no FileSystemView access at all). Transient because it captures a
   * FileSystemView supplier, which is not serializable: on a deserialized instance this is
   * null and the already-resolved {@link #prevFilePaths} is used instead. Excluded from the
   * generated getters so the {@link Lazy} wrapper never leaks into JSON.
   */
  @Getter(AccessLevel.NONE)
  private final transient Lazy<Map<String, PrevFilePaths>> prevFilePathsResolver;

  /**
   * Free-form context that producers can attach for downstream callback consumers.
   * The OSS write client populates this as empty; specialized callsites or wrappers
   * may populate it with whatever context their callbacks need.
   */
  private final Map<String, String> extraContext;

  public HoodieWriteCommitCallbackMessage(String commitTime,
                                          String tableName,
                                          String basePath,
                                          List<HoodieWriteStat> hoodieWriteStat,
                                          Option<String> commitActionType,
                                          Option<Map<String, String>> extraMetadata,
                                          Supplier<BaseFileOnlyView> fsViewSupplier,
                                          Map<String, String> extraContext) {
    this.commitTime = commitTime;
    this.tableName = tableName;
    this.basePath = basePath;
    this.hoodieWriteStat = hoodieWriteStat;
    this.commitActionType = commitActionType;
    this.extraMetadata = extraMetadata;
    this.prevFilePathsResolver = Lazy.lazily(() -> HoodieWriteCommitCallbackUtil.resolvePrevFilePaths(
        hoodieWriteStat, fsViewSupplier == null ? null : fsViewSupplier.get()));
    this.extraContext = extraContext;
  }

  public HoodieWriteCommitCallbackMessage(String commitTime,
                                          String tableName,
                                          String basePath,
                                          List<HoodieWriteStat> hoodieWriteStat) {
    this(commitTime, tableName, basePath, hoodieWriteStat, Option.empty(), Option.empty(),
        null, Collections.emptyMap());
  }

  public HoodieWriteCommitCallbackMessage(String commitTime,
                                          String tableName,
                                          String basePath,
                                          List<HoodieWriteStat> hoodieWriteStat,
                                          Option<String> commitActionType,
                                          Option<Map<String, String>> extraMetadata) {
    this(commitTime, tableName, basePath, hoodieWriteStat, commitActionType, extraMetadata,
        null, Collections.emptyMap());
  }

  /**
   * Returns the previous base file paths keyed by fileId, resolving them from the file-system
   * view on first access and memoizing the result. A consumer that never calls this triggers
   * no FileSystemView lookup. Never null: empty when no view was supplied and when the commit
   * only inserted.
   */
  public Map<String, PrevFilePaths> getPrevFilePaths() {
    Map<String, PrevFilePaths> paths = prevFilePaths;
    if (paths == null) {
      // The resolver is null only on an instance restored from Java serialization, and there
      // the resolved map has already been read back into prevFilePaths (see writeObject).
      paths = prevFilePathsResolver == null ? Collections.emptyMap() : prevFilePathsResolver.get();
      prevFilePaths = paths;
    }
    return paths;
  }

  /**
   * A {@link BaseFileOnlyView} cannot cross a serialization boundary, so materialize the
   * paths at the last possible moment and let the resolved map travel in their place.
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    getPrevFilePaths();
    out.defaultWriteObject();
  }

  /**
   * Container for previously-existing file paths associated with a single fileId in a
   * commit. {@link #baseFilePath} is the base file the new write replaces, and
   * {@link #bootstrapBaseFilePath} is the bootstrap-source file the previous
   * base file referenced (null for non-bootstrap tables).
   */
  @Getter
  public static class PrevFilePaths implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String baseFilePath;
    private final String bootstrapBaseFilePath;

    public PrevFilePaths(String baseFilePath, String bootstrapBaseFilePath) {
      this.baseFilePath = baseFilePath;
      this.bootstrapBaseFilePath = bootstrapBaseFilePath;
    }
  }
}
