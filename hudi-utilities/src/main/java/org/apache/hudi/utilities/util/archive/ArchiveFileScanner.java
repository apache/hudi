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

package org.apache.hudi.utilities.util.archive;

import org.apache.hudi.common.util.Option;

import org.apache.avro.generic.GenericRecord;

import java.util.function.BiConsumer;

/**
 * Reads archive files: cheaply for their instant range, fully for their records.
 *
 * <p>Implementations must not throw for storage or decode failures. Those are reported through the
 * return values, because a search that aborts on one damaged file in an old archive is less useful
 * than one that reports which instants it could not vouch for. Exceptions thrown by the caller's
 * {@code onHit} consumer are a different matter and may propagate.
 */
public interface ArchiveFileScanner {

  /**
   * Determines the range of instant times a file covers, without reading the whole file.
   *
   * @return the file's closed instant range, or {@code Option.empty()} when it could not be
   *         established -- which the searcher reads as "assume it covers everything and scan it",
   *         never as "it covers nothing"
   */
  Option<InstantRange> probe(ArchiveFile file);

  /**
   * Reads every record in a file, handing over the ones being searched for.
   *
   * @param file     the file to read
   * @param coverage what {@link #probe} said this file covers, or {@link InstantRange#all()} when
   *                 the probe failed. Supplied so an implementation can report records falling
   *                 outside the probed range, which would mean the range-based pruning is unsound
   *                 for this table.
   * @param targets  consulted with {@link LiveTargets#isTarget} per record, and updated with
   *                 {@link LiveTargets#markFound} on a hit
   * @param onHit    invoked for every record whose instant is a target, including repeat records for
   *                 an instant already found in this file
   *
   * @return whether the whole file was read
   */
  ScanOutcome scan(ArchiveFile file,
                   InstantRange coverage,
                   LiveTargets targets,
                   BiConsumer<String, GenericRecord> onHit);
}
