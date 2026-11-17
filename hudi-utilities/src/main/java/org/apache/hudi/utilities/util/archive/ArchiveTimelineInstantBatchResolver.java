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

import java.util.Set;

/**
 * Resolves a whole batch of instants against a table's archived timeline in ONE pass.
 *
 * <p>This is the only interface in the validator that reads an archived timeline. Everything
 * downstream reads the {@link ArchivedInstantWorkingSet} it returns. A per-instant or per-file-group
 * archived read is what stopped this class of job finishing on the tables it exists for, so the
 * batch is the unit on purpose.
 *
 * <p>Implementations are layout-specific and chosen by
 * {@link ArchiveTimelineInstantBatchResolvers#forTable}: the v1 (pre table version 8) archive is a
 * set of {@code .commits_.archive.N} log files, the v2 archive is an LSM tree, and no single read
 * path serves both.
 *
 * @param <T> the projection each found instant carries
 */
public interface ArchiveTimelineInstantBatchResolver<T> {

  /**
   * Resolves every target in one pass.
   *
   * <p><b>Never throws.</b> Every failure -- an unlistable archive, a thrown scanner, a projector
   * that could not decode -- degrades to {@link InstantSearchState#INDETERMINATE} for the affected
   * targets, which refuses rather than condemns. A caller forced to guess at that point has no safe
   * guess available to it.
   *
   * <p>The guarantee covers every {@link Exception}. An {@link Error} -- an {@code OutOfMemoryError},
   * a {@code StackOverflowError} -- propagates by design rather than degrading to
   * {@code INDETERMINATE}: it is not a condition any caller could resolve into a verdict, and
   * swallowing it would hide a JVM-level failure behind an ordinary refusal.
   *
   * <p><b>One deliberate exception to "never throws":</b> {@link V2ArchiveTimelineInstantBatchResolver}
   * throws {@link UnsupportedOperationException} unconditionally, because the LSM archive cannot yet
   * distinguish a genuine absence from a manifest that resolved to nothing, and degrading to
   * {@code INDETERMINATE} would look like an ordinary, silent refusal rather than the missing
   * capability it actually is. The never-throws guarantee above holds for every implementation that
   * can actually read its layout. Callers must call
   * {@link ArchiveTimelineInstantBatchResolvers#readsV1Archive} before constructing or calling a
   * resolver at all, and refuse when it is {@code false}, rather than reach this exception.
   *
   * @param targetInstants the instants to resolve; an empty set yields an empty working set and
   *                       reads nothing
   */
  ArchivedInstantWorkingSet<T> warmUp(Set<String> targetInstants);
}
