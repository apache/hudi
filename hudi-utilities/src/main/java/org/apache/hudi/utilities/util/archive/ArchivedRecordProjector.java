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

import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * Turns an archived timeline record into whatever a caller actually needs from it, during the
 * single pass, so that nothing larger has to be retained.
 *
 * <p><b>Called on searcher worker threads</b>, concurrently, for records of different instants.
 * Implementations must be thread-safe. {@link #merge} is called under a per-instant lock, so it
 * never races with itself for the same instant.
 *
 * @param <T> the projection, which is what a {@link ArchivedInstantWorkingSet} carries for a FOUND
 *            instant. Keep it small: it is retained for every found instant of the batch.
 */
public interface ArchivedRecordProjector<T> {

  /**
   * @throws IOException when the record arrived but could not be understood. That makes its instant
   *         {@link InstantSearchState#INDETERMINATE} -- the instant demonstrably exists and could
   *         not be read -- and must never be reported as an empty projection, which reads
   *         downstream as an absence.
   */
  T project(GenericRecord record) throws IOException;

  /**
   * Combines two projections of the SAME instant.
   *
   * <p>One instant arrives as several records -- the v6 archive persists a requested, an inflight
   * and a completed entry and only one of them carries write stats -- so projections must merge
   * rather than overwrite. <b>Must not depend on the order the records arrived in</b>, because the
   * pass is multi-threaded and that order is not defined.
   */
  T merge(T first, T second);
}
