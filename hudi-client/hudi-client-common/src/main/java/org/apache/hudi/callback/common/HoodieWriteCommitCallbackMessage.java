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
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.common.util.Option;

import lombok.AccessLevel;
import lombok.Getter;

import java.io.IOException;
import java.io.ObjectInputStream;
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
   * Previous base file paths keyed by fileId, resolved lazily. Populated by the write
   * client from the cached FileSystemView so that callback implementations don't have to
   * rebuild a view. Empty for inserts and for callers that don't pre-resolve.
   *
   * <p>Resolution is deferred until the first {@link #getPrevFilePaths()} call: a callback
   * that never reads the previous paths pays nothing (no FileSystemView access). Transient
   * because the initializer may capture a FileSystemView (not serializable); the resolved
   * map is what crosses Java serialization (see {@link #writeObject}/{@link #readObject}).
   * Excluded from the generated getters so the {@link Lazy} wrapper never leaks into JSON.
   */
  @Getter(AccessLevel.NONE)
  private transient Lazy<Map<String, PrevFilePaths>> prevFilePaths;

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
                                          Supplier<Map<String, PrevFilePaths>> prevFilePathsSupplier,
                                          Map<String, String> extraContext) {
    this.commitTime = commitTime;
    this.tableName = tableName;
    this.basePath = basePath;
    this.hoodieWriteStat = hoodieWriteStat;
    this.commitActionType = commitActionType;
    this.extraMetadata = extraMetadata;
    this.prevFilePaths = Lazy.lazily(
        prevFilePathsSupplier != null ? prevFilePathsSupplier : Collections::emptyMap);
    this.extraContext = extraContext;
  }

  public HoodieWriteCommitCallbackMessage(String commitTime,
                                          String tableName,
                                          String basePath,
                                          List<HoodieWriteStat> hoodieWriteStat) {
    this(commitTime, tableName, basePath, hoodieWriteStat, Option.empty(), Option.empty(),
        Collections::emptyMap, Collections.emptyMap());
  }

  public HoodieWriteCommitCallbackMessage(String commitTime,
                                          String tableName,
                                          String basePath,
                                          List<HoodieWriteStat> hoodieWriteStat,
                                          Option<String> commitActionType,
                                          Option<Map<String, String>> extraMetadata) {
    this(commitTime, tableName, basePath, hoodieWriteStat, commitActionType, extraMetadata,
        Collections::emptyMap, Collections.emptyMap());
  }

  /**
   * Returns the previous base file paths keyed by fileId, resolving them on first access.
   * A consumer that never calls this triggers no FileSystemView lookup. Never null.
   */
  public Map<String, PrevFilePaths> getPrevFilePaths() {
    return prevFilePaths.get();
  }

  /**
   * Force lazy resolution before serialization so the resolved map (not the transient
   * {@link Lazy} holder, which is not serializable) is what gets written.
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeObject(getPrevFilePaths());
  }

  @SuppressWarnings("unchecked")
  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    prevFilePaths = Lazy.eagerly((Map<String, PrevFilePaths>) in.readObject());
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
