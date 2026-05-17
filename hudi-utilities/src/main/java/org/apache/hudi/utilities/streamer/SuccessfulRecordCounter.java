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

import java.util.List;

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
   * Compute total / errored / successful record counts for a commit.
   *
   * @param dataTableWriteStatuses           Pre-collected write statuses from the data-table write.
   * @param errorTableWriteStatusRDDOpt      Optional error-table write status RDD; only consulted
   *                                         when unification is enabled.
   * @param isErrorTableWriteUnificationEnabled Whether error-table records contribute to the totals.
   * @return immutable {@link Counts} snapshot.
   */
  public static Counts compute(List<WriteStatus> dataTableWriteStatuses,
                               Option<JavaRDD<WriteStatus>> errorTableWriteStatusRDDOpt,
                               boolean isErrorTableWriteUnificationEnabled) {
    long totalRecords = 0L;
    long totalErroredRecords = 0L;
    for (WriteStatus ws : dataTableWriteStatuses) {
      totalRecords += ws.getTotalRecords();
      totalErroredRecords += ws.getTotalErrorRecords();
    }
    if (isErrorTableWriteUnificationEnabled && errorTableWriteStatusRDDOpt.isPresent()) {
      JavaRDD<WriteStatus> errorRdd = errorTableWriteStatusRDDOpt.get();
      totalRecords += errorRdd.mapToDouble(WriteStatus::getTotalRecords).sum().longValue();
      totalErroredRecords += errorRdd.mapToDouble(WriteStatus::getTotalErrorRecords).sum().longValue();
    }
    return new Counts(totalRecords, totalErroredRecords);
  }

  /** Immutable count snapshot. */
  public static final class Counts {
    private final long totalRecords;
    private final long totalErroredRecords;

    public Counts(long totalRecords, long totalErroredRecords) {
      this.totalRecords = totalRecords;
      this.totalErroredRecords = totalErroredRecords;
    }

    public long getTotalRecords() {
      return totalRecords;
    }

    public long getTotalErroredRecords() {
      return totalErroredRecords;
    }

    public long getTotalSuccessfulRecords() {
      return totalRecords - totalErroredRecords;
    }

    public boolean hasErrors() {
      return totalErroredRecords > 0;
    }
  }
}
