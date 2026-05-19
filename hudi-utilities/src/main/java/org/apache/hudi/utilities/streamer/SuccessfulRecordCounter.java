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
   * @param errorTableWriteStatusRDDOpt      Optional error-table write status RDD; only consulted
   *                                         when unification is enabled. Must not be null
   *                                         ({@link Option#empty()} when no error table).
   * @param isErrorTableWriteUnificationEnabled Whether error-table records contribute to the totals.
   * @return immutable {@link Counts} snapshot.
   */
  public static Counts compute(List<WriteStatus> dataTableWriteStatuses,
                               Option<JavaRDD<WriteStatus>> errorTableWriteStatusRDDOpt,
                               boolean isErrorTableWriteUnificationEnabled) {
    Objects.requireNonNull(dataTableWriteStatuses, "dataTableWriteStatuses");
    Objects.requireNonNull(errorTableWriteStatusRDDOpt, "errorTableWriteStatusRDDOpt");

    long totalRecords = 0L;
    long totalErroredRecords = 0L;
    for (WriteStatus ws : dataTableWriteStatuses) {
      totalRecords += ws.getTotalRecords();
      totalErroredRecords += ws.getTotalErrorRecords();
    }
    if (isErrorTableWriteUnificationEnabled && errorTableWriteStatusRDDOpt.isPresent()) {
      JavaRDD<WriteStatus> errorRdd = errorTableWriteStatusRDDOpt.get();
      totalRecords += sumLong(errorRdd, WriteStatusLongExtractor.TOTAL_RECORDS);
      totalErroredRecords += sumLong(errorRdd, WriteStatusLongExtractor.TOTAL_ERROR_RECORDS);
    }
    return new Counts(totalRecords, totalErroredRecords);
  }

  /**
   * Lossless long sum over an RDD. Avoids the precision loss of {@code mapToDouble().sum()},
   * which silently rounds counts above 2^53 (about 9 quadrillion).
   */
  private static long sumLong(JavaRDD<WriteStatus> rdd, WriteStatusLongExtractor extractor) {
    return rdd.map(extractor::extract).fold(0L, Long::sum);
  }

  private enum WriteStatusLongExtractor {
    TOTAL_RECORDS {
      @Override
      long extract(WriteStatus ws) {
        return ws.getTotalRecords();
      }
    },
    TOTAL_ERROR_RECORDS {
      @Override
      long extract(WriteStatus ws) {
        return ws.getTotalErrorRecords();
      }
    };

    abstract long extract(WriteStatus ws);
  }

  /** Immutable count snapshot. */
  public static final class Counts {
    public static final Counts ZERO = new Counts(0L, 0L);

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
