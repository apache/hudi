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

/**
 * Commits the error-table side effect for a HoodieStreamer commit.
 *
 * <p>Two paths exist, mirroring the original {@code HoodieStreamerWriteStatusValidator} behavior:</p>
 * <ul>
 *   <li><b>Unified write path</b> ({@code isErrorTableWriteUnificationEnabled=true}): commit the
 *       error-table write statuses produced alongside the data-table write.</li>
 *   <li><b>Legacy path</b>: invoke {@link BaseErrorTableWriter#upsertAndCommit(String, Option)}
 *       which performs both the upsert and the commit internally.</li>
 * </ul>
 *
 * <p>This helper is intentionally side-effect-only and returns a boolean success. Failure-policy
 * handling (ROLLBACK_COMMIT vs LOG_ERROR) is the caller's responsibility — see
 * {@code StreamSync.writeToSinkAndDoMetaSync()}. Extracted from
 * {@code HoodieStreamerWriteStatusValidator} as part of issue #18750.</p>
 */
public final class ErrorTableCommitter {

  private ErrorTableCommitter() {
  }

  /**
   * Commit the error-table writes for the given instant.
   *
   * @param errorTableWriter                    The configured error-table writer.
   * @param errorTableWriteStatusRDDOpt         Optional error-table write status RDD, populated when
   *                                            unification is enabled.
   * @param isErrorTableWriteUnificationEnabled Whether unified-write mode is enabled.
   * @param instantTime                         Instant being committed.
   * @param latestCommittedInstant              Optional latest completed instant, passed to legacy
   *                                            {@code upsertAndCommit}.
   * @return {@code true} if the error-table commit succeeded (or was a no-op);
   *         {@code false} if it failed and the caller must apply a failure-policy action.
   */
  public static boolean commit(BaseErrorTableWriter<?> errorTableWriter,
                               Option<JavaRDD<WriteStatus>> errorTableWriteStatusRDDOpt,
                               boolean isErrorTableWriteUnificationEnabled,
                               String instantTime,
                               Option<String> latestCommittedInstant) {
    if (isErrorTableWriteUnificationEnabled) {
      // In unification mode, the error-table writes were already produced upstream by the
      // unified write path. We only need to commit them here; nothing to commit when the
      // optional RDD is absent (true no-op).
      if (errorTableWriteStatusRDDOpt.isPresent()) {
        return errorTableWriter.commit(errorTableWriteStatusRDDOpt.get());
      }
      return true;
    }
    // Legacy path: writer performs both upsert and commit internally.
    return errorTableWriter.upsertAndCommit(instantTime, latestCommittedInstant);
  }
}
