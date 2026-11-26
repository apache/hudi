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

package org.apache.hudi.utilities.eventlog;

/**
 * The Hudi operation a stage belongs to: the level above {@link HudiPhase}.
 *
 * <p>The model is two-level, {@code operation -> phase -> stage}, because a phase alone is
 * ambiguous. A {@code WRITE} phase inside an upsert is not the same thing as the write inside a
 * compaction, and forty repetitions of the rollback phases is one restore rather than forty
 * rollbacks. Advice from a downstream optimizer differs by operation, so the operation has to be
 * reported alongside the phase.
 *
 * <p>Table services have a different stage shape from write operations: mostly a planning job
 * followed by an execution job, rather than the index/profile/write/commit pipeline. That structural
 * difference is what makes the sequence signature usable when the module name does not say outright
 * which operation is running.
 */
public enum HudiOperation {

  /**
   * Any of bulk_insert, insert, upsert, delete, insert_overwrite, insert_overwrite_table or
   * delete_partition. These are deliberately not distinguished from stages alone: they share the
   * same phase pipeline, and only the module name occasionally disambiguates them.
   */
  WRITE_OPERATION,

  /** Base-file compaction: plan generation followed by file-slice compaction. */
  COMPACTION,

  /** Log compaction, which merges log blocks without producing a new base file. */
  LOG_COMPACTION,

  /** Clustering: plan generation followed by one or more execution jobs. */
  CLUSTERING,

  /** Cleaner planning and execution. */
  CLEANING,

  /** Timeline archival. */
  ARCHIVAL,

  /** A single rollback of one instant. */
  ROLLBACK,

  /** A restore, which is N rollbacks driven by one restore instant. */
  RESTORE,

  /** Savepoint creation. */
  SAVEPOINT,

  /** Bootstrap of an existing non-Hudi table. */
  BOOTSTRAP,

  /** Building or updating a metadata table index partition. */
  INDEXING,

  /** A HoodieStreamer sync round: source read, optional transformation, then a write operation. */
  STREAMER_SYNC,

  /** Neither the module name nor the phase sequence resolved an operation. */
  UNKNOWN;

  /** Which signal produced an operation, so a reader can judge how much to trust it. */
  public enum Source {
    /** The module half of the job description named the operation. */
    MODULE,

    /** The operation was inferred from the sequence of phases in the job. */
    SEQUENCE,

    /** Nothing resolved; the operation is {@link HudiOperation#UNKNOWN}. */
    NONE;

    /** Lower-case form used in JSON, matching the documented {@code operationSource} values. */
    public String toJson() {
      return name().toLowerCase(java.util.Locale.ROOT);
    }
  }
}
