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

package org.apache.hudi.client.transaction;

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieSchemaEvolutionConflictException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;

/**
 * Strategy interface for schema conflict resolution with multiple writers.
 * Users can provide pluggable implementations for different kinds of strategies to resolve conflicts when multiple
 * writers are mutating the schema of hudi table.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.EVOLVING)
public interface SchemaConflictResolutionStrategy {

  /**
   * Resolves schema conflicts when multiple writers are mutating the schema of the Hudi table concurrently.
   * NOTE: WE ASSUME the meta client of the table is already reloaded with the latest timeline before this method call.
   *
   * @param table                        Hoodie table
   * @param config                       Hoodie write config
   * @param lastCompletedTxnOwnerInstant Last completed instant when the current transaction started
   * @param currTxnOwnerInstant          Instant of the current transaction
   * @return Resolved schema if hoodie.write.concurrency.schema.conflict.resolution.enable is turned on and the resolution is successful; Option.empty otherwise.
   * @throws HoodieWriteConflictException if schema conflicts cannot be resolved.
   */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  Option<HoodieSchema> resolveConcurrentSchemaEvolution(
      HoodieTable table,
      HoodieWriteConfig config,
      Option<HoodieInstant> lastCompletedTxnOwnerInstant,
      Option<HoodieInstant> currTxnOwnerInstant);

  static void throwConcurrentSchemaEvolutionException(
      Option<HoodieSchema> tableSchemaAtTxnStart, Option<HoodieSchema> tableSchemaAtTxnValidation, HoodieSchema writerSchemaOfTxn,
      Option<HoodieInstant> lastCompletedTxnOwnerInstant,
      Option<HoodieInstant> currTxnOwnerInstant) throws HoodieWriteConflictException {
    String errMsg = String.format(
        "Detected incompatible concurrent schema evolution. Schema when transaction starts: %s, "
            + "schema when transaction enters validation phase: %s tableSchemaAtTxnValidation, "
            + "schema the transaction tries to commit with: %s. lastCompletedTxnOwnerInstant is %s "
            + " and currTxnOwnerInstant is %s.",
        tableSchemaAtTxnStart.isPresent() ? tableSchemaAtTxnStart : "Not exists as no commited txn at that time",
        tableSchemaAtTxnValidation.isPresent() ? tableSchemaAtTxnValidation : "Not exists",
        writerSchemaOfTxn,
        lastCompletedTxnOwnerInstant.isPresent() ? lastCompletedTxnOwnerInstant : "Not exists",
        currTxnOwnerInstant.isPresent() ? currTxnOwnerInstant : "Not exists");

    throw new HoodieSchemaEvolutionConflictException(errMsg);
  }
}
