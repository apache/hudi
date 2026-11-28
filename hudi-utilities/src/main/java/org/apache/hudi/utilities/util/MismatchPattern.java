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

/**
 * The shapes a latest-file-slice comparison can disagree in, at <b>file</b> granularity.
 *
 * <p>Every pattern names the side that holds the file: {@code FS_} for a file the storage listing
 * reports and the metadata table does not, {@code MDT_} for one the metadata table records and
 * storage no longer has. The one exception is
 * {@link #BASE_FILE_ATTRIBUTE_MISMATCH}, where both sides hold the same file name and disagree
 * about what it contains.
 *
 * <p>The three grains are deliberate and are not collapsed into one another. A caller acting on
 * these findings needs to know whether an entire file group is unaccounted for, whether the two
 * sides merely disagree about which slice is current, or whether one file inside an otherwise
 * agreed slice is missing — the remedies differ, and so does the risk of getting them wrong.
 */
public enum MismatchPattern {

  /** File group present only in the storage listing; reported for every slice of it on storage. */
  FS_EXCLUSIVE_FILE_GROUP(MismatchSide.FS),

  /** File group present only in the metadata table. */
  MDT_EXCLUSIVE_FILE_GROUP(MismatchSide.MDT),

  /**
   * The file group exists on both sides, but the storage listing's latest slice has a base instant
   * the metadata table's latest slice does not — the metadata table is behind, or has lost the
   * newer slice.
   */
  FS_EXCLUSIVE_FILE_SLICE(MismatchSide.FS),

  /** Mirror of {@link #FS_EXCLUSIVE_FILE_SLICE}: the metadata table's latest slice is the newer. */
  MDT_EXCLUSIVE_FILE_SLICE(MismatchSide.MDT),

  /** The two sides agree on the slice, and only storage has a base file for it. */
  FS_EXCLUSIVE_BASE_FILE(MismatchSide.FS),

  /** The two sides agree on the slice, and only the metadata table has a base file for it. */
  MDT_EXCLUSIVE_BASE_FILE(MismatchSide.MDT),

  /**
   * Same base file name on both sides, differing {@code fileId} or length. Nothing is missing —
   * what one side records about the file is wrong.
   */
  BASE_FILE_ATTRIBUTE_MISMATCH(MismatchSide.BOTH),

  /** The two sides agree on the slice, and only storage has this log file. */
  FS_EXCLUSIVE_LOG_FILE(MismatchSide.FS),

  /** The two sides agree on the slice, and only the metadata table has this log file. */
  MDT_EXCLUSIVE_LOG_FILE(MismatchSide.MDT),

  /**
   * Storage holds a file that something is off about, and <b>the timeline has not decided</b>.
   *
   * <p>Written by both validation modes, to the dangling report. Fail-fast produces nothing else —
   * it checks the invariant and stops. Exhaustive mode adjudicates these afterwards, on the driver,
   * and whatever it proves unreferenced is re-emitted as {@link #DANGLING_FILES}.
   *
   * <p>The name carries the epistemic state on purpose. A category called "dangling" that might
   * merely be suspected is the shape of mistake that deletes live data, so the suspicion is in the
   * name rather than one indirection away in a verdict field.
   *
   * <p>Why a file became a suspect is {@link MismatchRecord#getSuspectReason()}: a slice holding
   * more than one base file, or a log file the timeline does not obviously vouch for.
   *
   * <p><b>Never appears in the adjudicated report.</b>
   */
  DANGLING_FILE_SUSPECTS(MismatchSide.FS),

  /**
   * Storage holds a file that is <b>proven</b> unreferenced: either no completed instant exists for
   * it on the active or archived timeline, or the commit it names completed and does not record it.
   *
   * <p>Only ever written to the adjudicated report, and only by the reconciliation pass. Its
   * contract is that a consumer may act on it without re-checking — the mitigation job backs these
   * up and deletes them without a second opinion. Anything the timeline could not settle is
   * <em>absent</em> from that report rather than present with a cautious verdict, precisely so that
   * contract stays true.
   */
  DANGLING_FILES(MismatchSide.FS);

  private final MismatchSide side;

  MismatchPattern(MismatchSide side) {
    this.side = side;
  }

  /** Which side holds the file this pattern is about. */
  public MismatchSide getSide() {
    return side;
  }
}
