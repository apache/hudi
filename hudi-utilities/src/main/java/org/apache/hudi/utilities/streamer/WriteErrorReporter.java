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

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Logs the first N errored write statuses so the operator can triage a failed commit.
 *
 * <p>Two overloads are provided so callers don't trigger a redundant Spark action:</p>
 * <ul>
 *   <li>{@link #logTopErrors(List)} — operate on a pre-collected list. Use when the caller
 *       already has the write statuses on the driver.</li>
 *   <li>{@link #logTopErrors(JavaRDD)} — operate on the RDD via {@code filter().take()}.
 *       Use when no validators were configured and the list wasn't materialized. Requires
 *       the RDD to be cached if other actions will follow, otherwise the DAG re-evaluates.</li>
 * </ul>
 *
 * <p>Extracted from {@code HoodieStreamerWriteStatusValidator} (#18750).</p>
 */
@Slf4j
public final class WriteErrorReporter {

  private static final int DEFAULT_MAX_ERRORS = 100;

  private WriteErrorReporter() {
  }

  public static void logTopErrors(JavaRDD<WriteStatus> writeStatusRDD) {
    logTopErrors(writeStatusRDD, DEFAULT_MAX_ERRORS);
  }

  public static void logTopErrors(List<WriteStatus> writeStatuses) {
    logTopErrors(writeStatuses, DEFAULT_MAX_ERRORS);
  }

  /**
   * Log up to {@code maxErrors} errored write statuses from the given RDD. Each errored status's
   * global error is logged at ERROR; per-key errors are logged at TRACE. The header line is INFO.
   * No-op when the RDD is null or {@code maxErrors <= 0}.
   */
  public static void logTopErrors(JavaRDD<WriteStatus> writeStatusRDD, int maxErrors) {
    if (writeStatusRDD == null || maxErrors <= 0) {
      return;
    }
    log.info("Printing out the top {} errored write statuses", maxErrors);
    writeStatusRDD.filter(WriteStatus::hasErrors).take(maxErrors).forEach(WriteErrorReporter::logOne);
  }

  /**
   * Log up to {@code maxErrors} errored write statuses from a pre-collected list. Same logging
   * shape as the RDD overload but avoids a Spark action when the caller already has the list.
   */
  public static void logTopErrors(List<WriteStatus> writeStatuses, int maxErrors) {
    if (writeStatuses == null || maxErrors <= 0) {
      return;
    }
    log.info("Printing out the top {} errored write statuses", maxErrors);
    writeStatuses.stream()
        .filter(WriteStatus::hasErrors)
        .limit(maxErrors)
        .forEach(WriteErrorReporter::logOne);
  }

  private static void logOne(WriteStatus writeStatus) {
    log.error("Global error: {}", writeStatus.getGlobalError());
    if (!writeStatus.getErrors().isEmpty()) {
      writeStatus.getErrors().forEach((k, v) -> log.trace("Error for key {} : {}", k, v));
    }
  }
}
