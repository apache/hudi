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

import org.apache.hudi.avro.AvroSchemaComparatorForSchemaEvolution;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;

import static org.apache.hudi.client.transaction.SchemaConflictResolutionStrategy.throwConcurrentSchemaEvolutionException;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;

/**
 * The implementation of SchemaConflictResolutionStrategy that detects incompatible
 * schema evolution from multiple writers
 */
public class SimpleSchemaConflictResolutionStrategy implements SchemaConflictResolutionStrategy {

  @Override
  public Option<Schema> resolveConcurrentSchemaEvolution(
      HoodieTable table,
      HoodieWriteConfig config,
      Option<HoodieInstant> lastCompletedTxnOwnerInstant,
      Option<HoodieInstant> currTxnOwnerInstant) {

    // If this is compaction table service, skip schema evolution check as it does not evolve schema.
    if (!currTxnOwnerInstant.isPresent()
        || currTxnOwnerInstant.get().getAction().equals(COMPACTION_ACTION)
        || (currTxnOwnerInstant.get().getAction().equals(REPLACE_COMMIT_ACTION)
        && ClusteringUtils.isClusteringInstant(table.getMetaClient().getActiveTimeline(), currTxnOwnerInstant.get(), table.getMetaClient().getInstantGenerator()))) {
      return Option.empty();
    }

    Schema writerSchemaOfTxn = new Schema.Parser().parse(config.getWriteSchema());
    // Only consider instants that change the table schema
    HoodieTimeline schemaEvolutionTimeline = table.getMetaClient().getSchemaEvolutionTimeline();
    // If lastCompletedInstantsAtTxnStart is null it means no committed txn when the current one starts.
    HoodieInstant lastCompletedInstantsAtTxnStart = lastCompletedTxnOwnerInstant.isPresent()
        ? schemaEvolutionTimeline.findInstantsBeforeOrEquals(
        lastCompletedTxnOwnerInstant.get().requestedTime()).lastInstant().orElseGet(() -> null)
        : null;
    // If lastCompletedInstantsAtTxnValidation is null there are 2 possibilities:
    // - No committed txn at validation starts
    // - [Almost impossible, so we ignore it] there is a commited txn, yet it is archived which cannot be found
    // in the active timeline.
    HoodieInstant lastCompletedInstantsAtTxnValidation = schemaEvolutionTimeline.lastInstant().orElse(null);

    // Please refer to RFC 82 for details of the case numbers.
    // Case 1:
    // We (curr txn) are the first to commit ever on this table, no conflict could happen.
    if (lastCompletedInstantsAtTxnValidation == null) {
      // Implies lastCompletedInstantsAtTxnStart is null as well.
      return Option.of(writerSchemaOfTxn);
    }

    // Optional optimization: if no concurrent writes happen at all, no conflict could happen.
    if (lastCompletedInstantsAtTxnValidation.equals(lastCompletedInstantsAtTxnStart)) {
      return Option.of(writerSchemaOfTxn);
    }

    // Case 2, 4, 7: Both writers try to evolve to the same schema or neither evolves schema.
    TableSchemaResolver schemaResolver = getSchemaResolver(table);
    Schema tableSchemaAtTxnValidation = getTableSchemaAtInstant(schemaResolver, lastCompletedInstantsAtTxnValidation);
    if (AvroSchemaComparatorForSchemaEvolution.schemaEquals(writerSchemaOfTxn, tableSchemaAtTxnValidation)) {
      return Option.of(writerSchemaOfTxn);
    }

    // Case 3:
    // We (curr txn) are the second to commit, and there is one commit that is done concurrently after this commit has started.
    // txn 1:    |-----read write-----|validate & commit|
    // curr txn: -----------------|--------read write--------|--validate & commit--|
    // lastCompletedInstantsAtTxnValidation != null is implied.
    // Populate configs regardless of what's the case we are trying to handle.
    if (lastCompletedInstantsAtTxnStart == null) {
      // If they don't share the same schema, we simply abort as a naive way of handling without considering
      // that they might be potentially compatible.
      throwConcurrentSchemaEvolutionException(
          Option.empty(), tableSchemaAtTxnValidation, writerSchemaOfTxn, lastCompletedTxnOwnerInstant, currTxnOwnerInstant);
    }

    // the transaction Compatible case 5
    // Table schema has not changed from the start of the transaction till the pre-commit validation
    Schema tableSchemaAtTxnStart = getTableSchemaAtInstant(schemaResolver, lastCompletedInstantsAtTxnStart);
    if (AvroSchemaComparatorForSchemaEvolution.schemaEquals(tableSchemaAtTxnStart, tableSchemaAtTxnValidation)) {
      return Option.of(writerSchemaOfTxn);
    }

    // Case 6: Current txn does not evolve schema, the tableSchema we saw at validation phase
    // might be an evolved one, use it.
    if (AvroSchemaComparatorForSchemaEvolution.schemaEquals(writerSchemaOfTxn, tableSchemaAtTxnStart)) {
      return Option.of(tableSchemaAtTxnValidation);
    }

    // Incompatible case 8: Initial table schema is S1, there is a concurrent txn evolves schema to S2,
    // current writer schema is S3.
    // Before the curr txn started, there are commited txn, with optional txn that commited during the
    // read-write phase of the curr txn (they can lead to concurrently schema evolution along with the curr txn).
    // table schema: ----------------S1----------------------------S2-------------------------
    // txn 1(S1):     |validate & commit|
    // txn 2(S2):     --------|-----read write-------|validate & commit|
    // curr txn(S3):  --------------------------|--------read write--------|--validate X
    throwConcurrentSchemaEvolutionException(
        Option.of(tableSchemaAtTxnStart), tableSchemaAtTxnValidation, writerSchemaOfTxn,
        lastCompletedTxnOwnerInstant, currTxnOwnerInstant);
    // Not reachable
    return Option.empty();
  }

  TableSchemaResolver getSchemaResolver(HoodieTable table) {
    return new TableSchemaResolver(table.getMetaClient());
  }

  private static Schema getTableSchemaAtInstant(TableSchemaResolver schemaResolver, HoodieInstant instant) {
    try {
      return schemaResolver.getTableAvroSchema(instant, false);
    } catch (Exception e) {
      throw new HoodieException("Unable to get table schema", e);
    }
  }
}
