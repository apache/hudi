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

package org.apache.hudi.utilities.util;

import java.io.Serializable;

/**
 * The one storage read this pass makes: open a log file and report what its first block says.
 *
 * <p>An interface rather than a direct call to {@link LogFileInstantReader} so that the rules in
 * the FS-MDT diff dimension stay exercisable without a filesystem, and so that a caller that has
 * already resolved its log files can say so. It is not optional: defaulting it would silently leave
 * every log file with an unknown block type, so a rollback marker could be adjudicated like any
 * other log file. A caller with genuinely nothing to read passes {@link #noLogFilesToRead()} and
 * says so out loud.
 *
 * <p>An implementation <b>must not throw</b>. It reports failure by returning an unresolved
 * {@link LogFileInstantReader.Result}, which is what {@link LogFileInstantReader#read} already
 * does; this class guards against a throw anyway, and treats it the same way.
 *
 * <p><b>Extends {@link Serializable} deliberately.</b> A Java lambda targeting a non-serializable
 * interface produces a non-serializable instance, and the implementations here are lambdas
 * ({@code FsMdtDiffDimension#logFileReaderFor}). Today they are built task-side and so never cross
 * a closure boundary, which makes the omission harmless -- but nothing enforces that placement, and
 * moving the construction to the driver would fail with a {@code NotSerializableException} only on
 * a cluster, after passing every local test. {@code StoragePathFilter} carries the same declaration
 * for the same reason.
 */
@FunctionalInterface
public interface LogFileReader extends Serializable {

  /**
   * @param partitionPath relative partition path the file was listed in
   * @param fileName      the bare log-file name, never a path
   * @return what the first log block says; never {@code null}
   */
  LogFileInstantReader.Result read(String partitionPath, String fileName);

  /**
   * A reader for a caller with no log files to open. Every log file it is asked about comes back
   * {@code UNREADABLE}, so <b>no log file is adjudicated at all</b>: a log file is adjudicated on
   * the instant its first block header names and gets no fallback onto the instant its record
   * arrived with. That is the fail-closed direction: an {@code FS_EXCLUSIVE_LOG_FILE} record
   * carries an instant of its own, so a fallback onto it would adjudicate the file with an unknown
   * block type, on the path this pass exists to close.
   *
   * <p>Base files are unaffected: they are never opened and their instant comes from their name.
   */
  static LogFileReader noLogFilesToRead() {
    return (partitionPath, fileName) ->
        LogFileInstantReader.Result.unresolved(MismatchRecord.InstantUnresolvedReason.UNREADABLE);
  }
}
