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
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.avro.HoodieAvroUtils.isSchemaNull;
import static org.apache.hudi.client.transaction.SchemaConflictResolutionStrategy.throwConcurrentSchemaEvolutionException;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMPACTION_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.compareTimestamps;

/**
 * The implementation of SchemaConflictResolutionStrategy that detects incompatible
 * schema evolution from multiple writers
 */
public class SimpleSchemaConflictResolutionStrategy implements SchemaConflictResolutionStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(SimpleSchemaConflictResolutionStrategy.class);

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
        && ClusteringUtils.isClusteringInstant(table.getMetaClient().getActiveTimeline(), currTxnOwnerInstant.get()))) {
      return Option.empty();
    }

    // Guard against unrecognized cases where writers do not come with a writer schema.
    if (StringUtils.isNullOrEmpty(config.getWriteSchema())) {
      LOG.warn(StringUtils.join("Writer config does not come with a valid writer schema. Writer config: ",
          config.toString(), ". Owner instant: ", currTxnOwnerInstant.get().toString()));
      return Option.empty();
    }

    Schema writerSchemaOfTxn = new Schema.Parser().parse(config.getWriteSchema());
    // If a writer does not come with a meaningful schema, skip the schema resolution.
    if (isSchemaNull(writerSchemaOfTxn)) {
      return getTableSchemaAtInstant(
          table.getMetaClient().getSchemaEvolutionTimelineInReverseOrder(),
          getSchemaResolver(table),
          currTxnOwnerInstant.get(),
          table,
          new ConcurrentHashMap<>(),
          new ConcurrentHashMap<>());
    }

    // Fast path: We can tell there is no schema conflict by just comparing the instants without involving table/writer schema comparison.
    HoodieTimeline reverseOrderTimeline = table.getMetaClient().getSchemaEvolutionTimelineInReverseOrder();

    // schema and writer schema.
    HoodieInstant lastCompletedInstantAtTxnStart = lastCompletedTxnOwnerInstant.isPresent()
        ? getInstantInTimelineImmediatelyPriorToTimestamp(lastCompletedTxnOwnerInstant.get().getTimestamp(), reverseOrderTimeline).orElse(null)
        : null;
    // If lastCompletedInstantAtTxnValidation is null there are 2 possibilities:
    // - No committed txn at validation starts
    // - [Almost impossible, so we ignore it] there is a commited txn, yet it is archived which cannot be found
    // in the active timeline.
    HoodieInstant lastCompletedInstantAtTxnValidation = reverseOrderTimeline.firstInstant().orElse(null);
    // Please refer to RFC 82 for details of the case numbers.
    // Case 1:
    // We (curr txn) are the first to commit ever on this table, no conflict could happen.
    if (lastCompletedInstantAtTxnValidation == null) {
      // Implies lastCompletedInstantAtTxnStart is null as well.
      return Option.of(writerSchemaOfTxn);
    }

    // Optional optimization: if no concurrent writes happen at all, no conflict could happen.
    if (lastCompletedInstantAtTxnValidation.equals(lastCompletedInstantAtTxnStart)) {
      return Option.of(writerSchemaOfTxn);
    }

    // Cache expensive computation for reusing over getTableSchemaAtInstant calls.
    ConcurrentHashMap<HoodieInstant, Boolean> isClusteringInstantMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<HoodieInstant, Boolean> instantContainsValidSchemaMap = new ConcurrentHashMap<>();

    TableSchemaResolver resolver = getSchemaResolver(table);
    Option<Schema> tableSchemaAtTxnValidation = getTableSchemaAtInstant(
        reverseOrderTimeline, resolver, lastCompletedInstantAtTxnValidation, table, instantContainsValidSchemaMap, isClusteringInstantMap);
    // If table schema is not defined, it's still case 1. There can be cases where there are commits but they didn't
    // write any data.
    if (!tableSchemaAtTxnValidation.isPresent()) {
      return Option.of(writerSchemaOfTxn);
    }
    // Case 2, 4, 7: Both writers try to evolve to the same schema or neither evolves schema.
    boolean writerSchemaIsCurrentTableSchema = AvroSchemaComparatorForSchemaEvolution.schemaEquals(writerSchemaOfTxn, tableSchemaAtTxnValidation.get());
    if (writerSchemaIsCurrentTableSchema) {
      return Option.of(writerSchemaOfTxn);
    }

    // Case 3:
    // We (curr txn) are the second to commit, and there is one commit that is done concurrently after this commit has started.
    // txn 1:    |-----read write-----|validate & commit|
    // curr txn: -----------------|--------read write--------|--validate & commit--|
    // lastCompletedInstantAtTxnValidation != null is implied.
    // Populate configs regardless of what's the case we are trying to handle.
    if (lastCompletedInstantAtTxnStart == null) {
      // If they don't share the same schema, we simply abort as a naive way of handling without considering
      // that they might be potentially compatible.
      throwConcurrentSchemaEvolutionException(
          Option.empty(), tableSchemaAtTxnValidation, writerSchemaOfTxn, lastCompletedTxnOwnerInstant, currTxnOwnerInstant);
    }
    Option<Schema> tableSchemaAtTxnStart = getTableSchemaAtInstant(
        reverseOrderTimeline, resolver, lastCompletedInstantAtTxnStart, table, instantContainsValidSchemaMap, isClusteringInstantMap);
    // If no table schema is defined, fall back to case 3.
    if (!tableSchemaAtTxnStart.isPresent()) {
      throwConcurrentSchemaEvolutionException(
          Option.empty(), tableSchemaAtTxnValidation, writerSchemaOfTxn, lastCompletedTxnOwnerInstant, currTxnOwnerInstant);
    }

    // Case 5:
    // Table schema has not changed from the start of the transaction till the pre-commit validation
    // If table schema parsing failed we will blindly go with writer schema. use option.empty
    if (AvroSchemaComparatorForSchemaEvolution.schemaEquals(tableSchemaAtTxnStart.get(), tableSchemaAtTxnValidation.get())) {
      return Option.of(writerSchemaOfTxn);
    }

    // Case 6: Current txn does not evolve schema, the tableSchema we saw at validation phase
    // might be an evolved one, use it.
    if (AvroSchemaComparatorForSchemaEvolution.schemaEquals(writerSchemaOfTxn, tableSchemaAtTxnStart.get())) {
      return tableSchemaAtTxnValidation;
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
        tableSchemaAtTxnStart, tableSchemaAtTxnValidation, writerSchemaOfTxn,
        lastCompletedTxnOwnerInstant, currTxnOwnerInstant);
    // Not reachable
    return Option.empty();
  }

  private Option<HoodieInstant> getInstantInTimelineImmediatelyPriorToTimestamp(
      String timestamp, HoodieTimeline reverseOrderTimeline) {
    return Option.fromJavaOptional(reverseOrderTimeline.getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getTimestamp(), LESSER_THAN_OR_EQUALS, timestamp))
        .findFirst());
  }

  TableSchemaResolver getSchemaResolver(HoodieTable table) {
    return new TableSchemaResolver(table.getMetaClient());
  }

  private static Option<Schema> getTableSchemaAtInstant(
      HoodieTimeline reversedTimeline, TableSchemaResolver schemaResolver, @Nonnull HoodieInstant instant, HoodieTable table,
      ConcurrentHashMap<HoodieInstant, Boolean> instantContainsValidSchemaMap,
      ConcurrentHashMap<HoodieInstant, Boolean> isClusteringInstantMap) {
    // To find the table schema given an instant time, need to walk backwards from the latest instant in
    // the timeline finding a completed instant containing a valid schema.
    Option<HoodieInstant> instantWithTableSchema = Option.fromJavaOptional(reversedTimeline.getInstantsAsStream()
        // only pay attention to instants comes no later than the given instant
        .filter(s -> compareTimestamps(s.getTimestamp(), LESSER_THAN_OR_EQUALS, instant.getTimestamp()))
        // ignore clustering table service instants as it does not contain the table schema.
        // isClusteringInstantMap is used as a cache to store result of the expensive computation as this function
        // is used for multiple times.
        .filter(s -> !s.getAction().equals(REPLACE_COMMIT_ACTION)
            || !isClusteringInstantMap.computeIfAbsent(s, s1 -> ClusteringUtils.isClusteringInstant(reversedTimeline, s1)))
        // Make sure the commit metadata has a valid schema inside. Same caching the result for expensive operation.
        .filter(s -> {
          instantContainsValidSchemaMap.computeIfAbsent(s, s1 -> {
            try {
              return !StringUtils.isNullOrEmpty(
                  HoodieCommitMetadata.fromBytes(reversedTimeline.getInstantDetails(s1).get(), HoodieCommitMetadata.class)
                  .getMetadata(HoodieCommitMetadata.SCHEMA_KEY));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
          return instantContainsValidSchemaMap.get(s);
        }).findFirst());

    if (instantWithTableSchema.isPresent()) {
      // If there is a qualified instant, parse the table schema out of it.
      try {
        return Option.of(schemaResolver.getTableAvroSchema(instantWithTableSchema.get(), false));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      // If there isn't one, 2 possible cases:
      // - It is a newly created table which only contains instant that does not write data, table schema is undefined
      //   at that time - we will try reading the table creation schema if there is one.
      // - The table have data and valid commits (non-empty table), but they are all archived and can't be found in
      //   active timeline. At this point we don't have enough context for detecting schema conflicts. This we cannot handle,
      //   only log a warning.
      // The second case can only happen for streams who have not been running SimpleSchemaConflictResolutionStrategy logic
      // since their creation time. Before the schema conflict resolution change went in, there can be writers commiting with
      // a null schema. After this change, all non table service writers using this strategy class are guaranteed to have the
      // latest table schema at their validation phase in their commit metadata.
      LOG.warn(String.format("Cannot find a qualified instant to extract table schema, SimpleSchemaConflictResolutionStrategy "
          + "might not be able to detect and prevent incompatible concurrent schema evolution from happening if this is an "
          + "non-empty table. Table base path: %s.", table.getMetaClient().getBasePathV2()));
      return schemaResolver.getTableCreateSchemaWithMetadata(false);
    }
  }
}
