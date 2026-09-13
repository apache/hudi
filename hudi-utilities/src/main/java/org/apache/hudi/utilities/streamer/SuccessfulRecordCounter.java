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

import java.util.List;
import java.util.Objects;

/**
 * Computes record counts for a HoodieStreamer commit, summing across the data-table
 * write statuses and (optionally) the error-table write statuses when error-table
 * write unification is enabled.
 *
 * <p>Extracted from {@code HoodieStreamerWriteStatusValidator} (issue #18750) so the
 * counting logic can be invoked from the explicit pre-commit orchestration in
 * {@code StreamSync} without going through the {@code WriteStatusValidator} callback.</p>
 */
public final class SuccessfulRecordCounter {

  private SuccessfulRecordCounter() {
  }

  /**
   * Compute total / errored / successful record counts from a pre-collected list of write statuses.
   *
   * @param dataTableWriteStatuses           Pre-collected data-table write statuses. Must not be null.
   * @param errorTableWriteStatuses            Error-table write statuses collected before the error table
   *                                           commit, or empty when none were written
   *                                         when unification is enabled. Must not be null
   *                                         ({@link Option#empty()} when no error table).
   * @param isErrorTableWriteUnificationEnabled Whether error-table records contribute to the totals.
   * @return immutable {@link Counts} snapshot.
   */
  public static Counts compute(List<WriteStatus> dataTableWriteStatuses,
                               Option<List<WriteStatus>> errorTableWriteStatuses,
                               boolean isErrorTableWriteUnificationEnabled) {
    Objects.requireNonNull(dataTableWriteStatuses, "dataTableWriteStatuses");
    Objects.requireNonNull(errorTableWriteStatuses, "errorTableWriteStatuses");
    long totalRecords = 0L;
    long totalErrorRecords = 0L;
    for (WriteStatus ws : dataTableWriteStatuses) {
      totalRecords += ws.getTotalRecords();
      totalErrorRecords += ws.getTotalErrorRecords();
    }
    if (isErrorTableWriteUnificationEnabled && errorTableWriteStatuses.isPresent()) {
      for (WriteStatus ws : errorTableWriteStatuses.get()) {
        totalRecords += ws.getTotalRecords();
        totalErrorRecords += ws.getTotalErrorRecords();
      }
    }
    return new Counts(totalRecords, totalErrorRecords);
  }

  /** Immutable count snapshot. */
  public static final class Counts {
    public static final Counts ZERO = new Counts(0L, 0L);

    private final long totalRecords;
    private final long totalErrorRecords;

    public Counts(long totalRecords, long totalErrorRecords) {
      this.totalRecords = totalRecords;
      this.totalErrorRecords = totalErrorRecords;
    }

    public long getTotalRecords() {
      return totalRecords;
    }

    public long getTotalErrorRecords() {
      return totalErrorRecords;
    }

    public long getTotalSuccessfulRecords() {
      return totalRecords - totalErrorRecords;
    }

    public boolean hasErrors() {
      return totalErrorRecords > 0;
    }
  }
}
