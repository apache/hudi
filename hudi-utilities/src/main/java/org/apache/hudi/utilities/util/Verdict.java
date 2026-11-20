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
 * What the reconciliation pass concluded about one file.
 *
 * <p>Only {@link #SANCTIONED} and {@link #NOT_SETTLED} pass. Everything else fails the run, and
 * everything else requires positive proof: a verdict reached because a question could not be
 * answered is a bug, not a finding. See the design's governing safety property.
 */
public enum Verdict {
  /** Explained. Not a finding. */
  SANCTIONED(false),
  /**
   * The question could not be answered. Reported so an operator can see how much of a run was
   * unadjudicable, never acted on, and never fatal.
   */
  NOT_SETTLED(false),
  /** Storage holds a file nothing references. A delete authorisation. */
  DANGLING(true),
  /** The metadata table records something storage contradicts. */
  MDT_CORRUPTION(true),
  /** The metadata table names a file storage no longer has, and no clean or rollback explains it. */
  OUTBOUND_DELETION(true),
  /**
   * A state a run must never observe. Reached from two places: an instant completed at
   * {@code T_prev} that is pending or absent at {@code T_aft}, and an instant that was absent or
   * {@code REQUESTED} at {@code T_prev}, is {@code REQUESTED} at {@code T_aft}, and wrote a file no
   * rollback accounts for. The causes are a timeline edited outside Hudi's write paths, the
   * validator's own two reads being wrong, or a rollback or restore of a completed instant that
   * overlapped the run. That last one is a supported write path, so this verdict does not by itself
   * mean the table or the code is broken.
   */
  CODE_BUG(true);

  private final boolean validationFailure;

  Verdict(boolean validationFailure) {
    this.validationFailure = validationFailure;
  }

  public boolean isValidationFailure() {
    return validationFailure;
  }
}
