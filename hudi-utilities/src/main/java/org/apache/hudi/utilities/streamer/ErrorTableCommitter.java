/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.Option;

import org.apache.spark.api.java.JavaRDD;

import java.util.Objects;

/**
 * Commits the error-table side of a HoodieStreamer commit.
 *
 * <p>Two paths exist, mirroring the original {@code HoodieStreamerWriteStatusValidator} behavior:</p>
 * <ul>
 *   <li><b>Unified write path</b> ({@code isErrorTableWriteUnificationEnabled=true}): commit the
 *       error-table write statuses produced alongside the data-table write.</li>
 *   <li><b>Legacy path</b>: invoke {@link BaseErrorTableWriter#upsertAndCommit(String, Option)}
 *       which performs both the upsert and the commit internally.</li>
 * </ul>
 *
 * <p>This helper performs the commit and reports success/failure. It deliberately does <i>not</i>
 * handle the {@code ROLLBACK_COMMIT} / {@code LOG_ERROR} failure strategies — that policy decision
 * lives in {@code StreamSync.writeToSinkAndDoMetaSync()}, which understands the surrounding
 * orchestration. Extracted from {@code HoodieStreamerWriteStatusValidator} as part of #18750.</p>
 */
public final class ErrorTableCommitter {

  private ErrorTableCommitter() {
  }

  /**
   * Commit the error-table writes for the given instant.
   *
   * @param errorTableWriter                    The configured error-table writer. Must not be null.
   * @param errorTableWriteStatusRDDOpt         Optional error-table write status RDD, populated when
   *                                            unification is enabled. Must not be null
   *                                            ({@link Option#empty()} if no RDD).
   * @param isErrorTableWriteUnificationEnabled Whether unified-write mode is enabled.
   * @param instantTime                         Instant being committed.
   * @param latestCommittedInstant              Optional latest completed instant, passed to legacy
   *                                            {@code upsertAndCommit}. Must not be null
   *                                            ({@link Option#empty()} if none).
   * @return {@code true} if the error-table commit succeeded (or was a no-op);
   *         {@code false} if it failed and the caller must apply a failure-policy action.
   */
  public static boolean commit(BaseErrorTableWriter<?> errorTableWriter,
                               Option<JavaRDD<WriteStatus>> errorTableWriteStatusRDDOpt,
                               boolean isErrorTableWriteUnificationEnabled,
                               String instantTime,
                               Option<String> latestCommittedInstant) {
    Objects.requireNonNull(errorTableWriter, "errorTableWriter");
    Objects.requireNonNull(errorTableWriteStatusRDDOpt, "errorTableWriteStatusRDDOpt");
    Objects.requireNonNull(instantTime, "instantTime");
    Objects.requireNonNull(latestCommittedInstant, "latestCommittedInstant");

    if (isErrorTableWriteUnificationEnabled) {
      // In unification mode the error-table writes were produced upstream by the unified write
      // path. Commit them here; nothing to do when the optional RDD is absent (true no-op).
      if (errorTableWriteStatusRDDOpt.isPresent()) {
        return errorTableWriter.commit(errorTableWriteStatusRDDOpt.get());
      }
      return true;
    }
    // Legacy path: writer performs both upsert and commit internally.
    return errorTableWriter.upsertAndCommit(instantTime, latestCommittedInstant);
  }
}
