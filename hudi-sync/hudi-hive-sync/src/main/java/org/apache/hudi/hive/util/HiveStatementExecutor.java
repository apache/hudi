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

package org.apache.hudi.hive.util;

import org.apache.hudi.hive.HoodieHiveSyncException;

import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;

/**
 * Runs HiveQL through a Hive {@link Driver}.
 */
public final class HiveStatementExecutor {

  /**
   * Batched partition statements can run to many kilobytes; the error only needs enough of the
   * statement to identify it.
   */
  static final int MAX_STATEMENT_LENGTH_IN_ERROR = 1000;

  private HiveStatementExecutor() {
  }

  /**
   * Runs the statement and throws if Hive rejects it. {@link Driver#run(String)} reports compile
   * and execution failures (a partition spec that names a non-partition column, a DDL task the
   * metastore refuses) through a non-zero response code rather than by throwing, so a caller that
   * only catches exceptions goes on as though the statement had been applied.
   */
  public static void executeOrThrow(Driver driver, String sql) throws CommandNeedRetryException {
    CommandProcessorResponse response = driver.run(sql);
    if (response.getResponseCode() != 0) {
      throw new HoodieHiveSyncException(String.format(
          "Hive rejected the statement with response code %d (SQLState %s): %s%nStatement: %s",
          response.getResponseCode(), response.getSQLState(), response.getErrorMessage(), abbreviate(sql)),
          response.getException());
    }
  }

  private static String abbreviate(String sql) {
    if (sql.length() <= MAX_STATEMENT_LENGTH_IN_ERROR) {
      return sql;
    }
    return sql.substring(0, MAX_STATEMENT_LENGTH_IN_ERROR) + "... (" + sql.length() + " characters)";
  }
}
