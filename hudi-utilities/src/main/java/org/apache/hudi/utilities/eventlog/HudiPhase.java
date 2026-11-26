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
 * The coarse bucket of a Hudi operation that a Spark job's time falls into.
 *
 * <p>These buckets are deliberately few. The question worth answering is "where did the time go",
 * at a granularity a user can act on: <em>60% of the run is index tagging</em>, or <em>compaction
 * planning dominates execution</em>. Finer attribution than that costs complexity and risks being
 * confidently wrong, so the finer label a description resolved to is carried alongside as a
 * sub-phase rather than fragmenting this list. The verbatim job description is always reported too,
 * as the escape hatch.
 *
 * <p>A write operation has five buckets: {@link #SOURCE_READ_AND_TRANSFORM},
 * {@link #DEDUP_AND_INDEX_TAGGING}, {@link #DATA_TABLE_WRITE}, {@link #MARKER_RECONCILIATION} and
 * {@link #METADATA_TABLE_WRITE}. The table services -- compaction, clustering, cleaning, rollback --
 * each have {@link #PLANNING} and {@link #EXECUTION}. Because those two names recur across every
 * table service, {@link HudiOperation} is what disambiguates them, which is why the operation
 * dimension is reported alongside.
 *
 * <p><b>Several bucket names state an attribution limit rather than hiding it.</b> Hudi leaves some
 * steps untagged and Spark evaluates lazily, so their cost is forced into a neighbouring stage and
 * cannot be separated from an event log. Where that is known to happen the bucket is named for
 * everything it really contains, instead of naming one step and footnoting the rest.
 */
public enum HudiPhase {

  /**
   * Source read, user transformation and record creation, which cannot be separated.
   *
   * <p>{@code StreamSync} tags only the source fetch and the emptiness check; applying the
   * transformer is untagged and {@code HoodieStreamerUtils} sets no job status at all. Spark forces
   * all three into whichever stage materialises the records, so the bucket is named for all three.
   */
  SOURCE_READ_AND_TRANSFORM,

  /**
   * Deduplication, index tagging and workload profiling, which Hudi runs as one job.
   *
   * <p>{@code BaseWriteHelper} tags only the {@code Tagging:} step and {@code deduplicateRecords}
   * runs untagged immediately before it. {@code Building workload profile:} is likewise the single
   * description Hudi sets over the whole lazy chain -- dedup, tagging and profiling all execute
   * beneath it -- so all three belong to this bucket. Which component actually ran in a given stage
   * is recorded as the sub-phase, refined from the Spark stage name.
   */
  DEDUP_AND_INDEX_TAGGING,

  /**
   * Writing to the data table, including the small-file probe that feeds it, and collecting and
   * committing the resulting write stats.
   *
   * <p>Small-file probing is folded in because it is mostly listing and driver work; when it is
   * individually large it stays visible through the sub-phase.
   */
  DATA_TABLE_WRITE,

  /** Marker creation, marker reconciliation, and deleting files not reconciled against markers. */
  MARKER_RECONCILIATION,

  /** Writing to and committing the metadata table, including its index partitions. */
  METADATA_TABLE_WRITE,

  /**
   * The planning half of a table service: choosing what to compact, cluster, clean or roll back.
   * Which service this is comes from {@link HudiOperation}, not from this value.
   */
  PLANNING,

  /**
   * The execution half of a table service: doing the compaction, clustering, cleaning or rollback.
   * Which service this is comes from {@link HudiOperation}, not from this value.
   */
  EXECUTION,

  /** Timeline archival. Driver-dominated, so expected to be absent or trivial in a Spark log. */
  ARCHIVAL,

  /** The job description did not match any known Hudi activity, or there was no description. */
  UNKNOWN
}
