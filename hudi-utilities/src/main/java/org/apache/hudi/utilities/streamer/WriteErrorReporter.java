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

import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs the first N errored write statuses so the operator can triage a failed commit.
 *
 * <p>Extracted from {@code HoodieStreamerWriteStatusValidator} (issue #18750) so the
 * top-errors dump can be invoked from the pre-commit orchestration in {@code StreamSync}
 * without going through the {@code WriteStatusValidator} callback.</p>
 */
public final class WriteErrorReporter {

  private static final Logger LOG = LoggerFactory.getLogger(WriteErrorReporter.class);
  private static final int DEFAULT_MAX_ERRORS = 100;

  private WriteErrorReporter() {
  }

  public static void logTopErrors(JavaRDD<WriteStatus> writeStatusRDD) {
    logTopErrors(writeStatusRDD, DEFAULT_MAX_ERRORS);
  }

  /**
   * Log up to {@code maxErrors} errored write statuses from the given RDD.
   * Each errored status's global error is logged at ERROR; per-key errors are logged at TRACE.
   */
  public static void logTopErrors(JavaRDD<WriteStatus> writeStatusRDD, int maxErrors) {
    if (writeStatusRDD == null || maxErrors <= 0) {
      return;
    }
    LOG.error("Printing out the top {} errors", maxErrors);
    writeStatusRDD.filter(WriteStatus::hasErrors).take(maxErrors).forEach(writeStatus -> {
      LOG.error("Global error {}", writeStatus.getGlobalError());
      if (!writeStatus.getErrors().isEmpty()) {
        writeStatus.getErrors().forEach((k, v) -> LOG.trace("Error for key {} : {}", k, v));
      }
    });
  }
}
