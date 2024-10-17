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

package org.apache.hudi.client.utils;

import org.apache.hudi.client.transaction.ConcurrentOperation;
import org.apache.hudi.client.transaction.ConflictResolutionStrategy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TransactionUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TransactionUtils.class);

  /**
   * Resolve any write conflicts when committing data.
   *
   * @param table
   * @param currentTxnOwnerInstant
   * @param thisCommitMetadata
   * @param config
   * @param lastCompletedTxnOwnerInstant
   * @param pendingInstants
   *
   * @return
   * @throws HoodieWriteConflictException
   */
  public static Option<HoodieCommitMetadata> resolveWriteConflictIfAny(
      final HoodieTable table,
      final Option<HoodieInstant> currentTxnOwnerInstant,
      final Option<HoodieCommitMetadata> thisCommitMetadata,
      final HoodieWriteConfig config,
      Option<HoodieInstant> lastCompletedTxnOwnerInstant,
      boolean reloadActiveTimeline,
      Set<String> pendingInstants) throws HoodieWriteConflictException {
    WriteOperationType operationType = thisCommitMetadata.map(HoodieCommitMetadata::getOperationType).orElse(null);
    if (config.needResolveWriteConflict(operationType)) {
      // deal with pendingInstants
      Stream<HoodieInstant> completedInstantsDuringCurrentWriteOperation = getCompletedInstantsDuringCurrentWriteOperation(table.getMetaClient(), pendingInstants);
      ConflictResolutionStrategy resolutionStrategy = config.getWriteConflictResolutionStrategy();
      if (reloadActiveTimeline) {
        table.getMetaClient().reloadActiveTimeline();
      }
      Stream<HoodieInstant> instantStream = Stream.concat(resolutionStrategy.getCandidateInstants(
          table.getMetaClient(), currentTxnOwnerInstant.get(), lastCompletedTxnOwnerInstant),
              completedInstantsDuringCurrentWriteOperation);

      final ConcurrentOperation thisOperation = new ConcurrentOperation(currentTxnOwnerInstant.get(), thisCommitMetadata.orElseGet(HoodieCommitMetadata::new));
      instantStream.forEach(instant -> {
        try {
          ConcurrentOperation otherOperation = new ConcurrentOperation(instant, table.getMetaClient());
          if (resolutionStrategy.hasConflict(thisOperation, otherOperation)) {
            LOG.info("Conflict encountered between current instant = " + thisOperation + " and instant = "
                + otherOperation + ", attempting to resolve it...");
            resolutionStrategy.resolveConflict(table, thisOperation, otherOperation);
          }
        } catch (IOException io) {
          throw new HoodieWriteConflictException("Unable to resolve conflict, if present", io);
        }
      });
      LOG.info("Successfully resolved conflicts, if any");

      // Resolve schema.
      Schema schemaOfCommitMetadata = resolveConcurrentSchemaEvolution(
          table, config, lastCompletedTxnOwnerInstant, new TableSchemaResolver(table.getMetaClient()));
      if (thisCommitMetadata.isPresent()) {
        thisCommitMetadata.get().addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schemaOfCommitMetadata.toString());
      }
      thisOperation.getCommitMetadataOption().get().addMetadata(
          HoodieCommitMetadata.SCHEMA_KEY, schemaOfCommitMetadata.toString());
      return thisOperation.getCommitMetadataOption();
    }
    return thisCommitMetadata;
  }

  /**
   * Resolve concurrent schema evolution. If it is resolvable, return the schema to be set in the commit metadata.
   * Otherwise, throw a {@link HoodieWriteConflictException}.
   *
   * @param table The Hoodie table.
   * @param config The Hoodie write configuration.
   * @param lastCompletedTxnOwnerInstant The last completed transaction owner instant.
   * @param schemaResolver The table schema resolver.
   * @throws HoodieWriteConflictException If there is a concurrent schema evolution.
   */
  static Schema resolveConcurrentSchemaEvolution(
      HoodieTable table,
      HoodieWriteConfig config,
      Option<HoodieInstant> lastCompletedTxnOwnerInstant,
      TableSchemaResolver schemaResolver) {
    HoodieInstant lastCompletedInstantsAtTxnStart = lastCompletedTxnOwnerInstant.isPresent() ? lastCompletedTxnOwnerInstant.get() : null;
    HoodieInstant lastCompletedInstantsAtTxnValidation = table.getMetaClient().reloadActiveTimeline().getCommitsTimeline().filterCompletedInstants()
        .lastInstant()
        .orElse(null);
    // Populate configs regardless of what's the case we are trying to handle.
    Schema schemaOfTxn = new Schema.Parser().parse(config.getWriteSchema());
    Schema schemaAtTxnStart = null;
    Schema schemaAtTxnValidation = null;
    try {
      if (lastCompletedInstantsAtTxnStart != null) {
        schemaAtTxnStart = schemaResolver.getTableAvroSchema(lastCompletedInstantsAtTxnStart, false);
      }
      if (lastCompletedInstantsAtTxnValidation != null) {
        schemaAtTxnValidation = schemaResolver.getTableAvroSchema(lastCompletedInstantsAtTxnValidation, false);
      }
    } catch (Exception e) {
      throw new HoodieWriteConflictException("Unable to resolve conflict, if present", e);
    }

    // Case 1:
    // We (curr txn) are the first to commit ever on this table, no conflict could happen.
    // We should use the current writer schema in commit metadata.
    // curr txn: |--read write--|--validate & commit--|
    if (lastCompletedInstantsAtTxnValidation == null) {
      // Implies lastCompletedInstantsAtTxnStart is null as well.
      return schemaOfTxn;
    }

    // Case 2:
    // We (curr txn) are the second to commit, and the first one commits during the read write phase.
    // txn 1:    |-----read write-----|validate & commit|
    // curr txn: -----------------|--------read write--------|--validate & commit--|
    // lastCompletedInstantsAtTxnValidation != null is implied.
    if (lastCompletedInstantsAtTxnStart == null) {
      // If they don't share the same schema, we simply abort as a naive way of handling without considering
      // that they might be potentially compatible.
      if (!schemaOfTxn.equals(schemaAtTxnValidation)) {
        String concurrentSchemaEvolutionError = String.format(
            "Detect concurrent schema evolution. Schema when transaction starts: %s, "
                + "schema when transaction enters validation phase %s schemaAtTxnValidation, "
                + "schema the transaction tries to commit with %s",
            "Not exists as no commited txn at that time", schemaAtTxnValidation, schemaOfTxn);
        throw new HoodieWriteConflictException(concurrentSchemaEvolutionError);
      }
      // No schema evolution here as both txn uses the same schema.
      return schemaOfTxn;
    }

    // Case 3
    // Before the curr txn started, there are commited txn, with optional txn that commited during the read-write phase
    // of the curr txn (they can lead to concurrently schema evolution along with the curr txn).
    // txn 1:            |validate & commit|
    // txn 2 (optional): --------|-----read write-------|validate & commit|
    // curr txn:         --------------------------|--------read write--------|--validate & commit--|

    // We only abort if all 3 txn above has different schemas.
    // Please note, we allow 2 special cases as they naturally work:
    // - txn 1 commited with schema s1, txn 2 use s2, curr txn uses s1 - reader path will resolve it properly
    // - txn 2 and curr txn uses the same schema.
    if (!schemaAtTxnStart.equals(schemaOfTxn)
        && !schemaAtTxnStart.equals(schemaAtTxnValidation)
        && !schemaOfTxn.equals(schemaAtTxnValidation)) {
      String concurrentSchemaEvolutionError = String.format(
          "Detect concurrent schema evolution. Schema when transaction starts: %s, "
              + "schema when transaction enters validation phase %s schemaAtTxnValidation, "
              + "schema the transaction tries to commit with %s", schemaAtTxnStart, schemaAtTxnValidation, schemaOfTxn);
      throw new HoodieWriteConflictException(concurrentSchemaEvolutionError);
    }

    // Compatible cases:

    // txn1 use schema s1, no txn2, curr txn use s1 => curr txn should tag s1 in its commit metadata.
    // txn1 use schema s1, no txn2, curr txn use s2 => curr txn should tag s2 in its commit metadata.
    if (lastCompletedInstantsAtTxnStart.equals(lastCompletedInstantsAtTxnValidation)) {
      return schemaOfTxn;
    }

    // txn1 use schema s1, txn2 use s1, curr txn use s1 => curr txn should tag s1 in its commit metadata.
    // txn1 use schema s1, txn2 use s1, curr txn use s2 => curr txn should tag s2 in its commit metadata.
    if (schemaAtTxnStart.equals(schemaAtTxnValidation)) {
      return schemaOfTxn;
    }

    // txn1 use schema s1, txn2 use s2, curr txn use s1 => curr txn should tag s2 in its commit metadata.
    // txn1 use schema s1, txn2 use s2, curr txn use s2 => curr txn should tag s2 in its commit metadata.
    return schemaAtTxnValidation;
  }

  /**
   * Get the last completed transaction hoodie instant and {@link HoodieCommitMetadata#getExtraMetadata()}.
   *
   * @param metaClient
   * @return
   */
  public static Option<Pair<HoodieInstant, Map<String, String>>> getLastCompletedTxnInstantAndMetadata(
      HoodieTableMetaClient metaClient) {
    Option<HoodieInstant> hoodieInstantOption = metaClient.getActiveTimeline().getCommitsTimeline()
        .filterCompletedInstants().lastInstant();
    return getHoodieInstantAndMetaDataPair(metaClient, hoodieInstantOption);
  }

  private static Option<Pair<HoodieInstant, Map<String, String>>> getHoodieInstantAndMetaDataPair(HoodieTableMetaClient metaClient, Option<HoodieInstant> hoodieInstantOption) {
    try {
      if (hoodieInstantOption.isPresent()) {
        HoodieCommitMetadata commitMetadata = TimelineUtils.getCommitMetadata(hoodieInstantOption.get(), metaClient.getActiveTimeline());
        return Option.of(Pair.of(hoodieInstantOption.get(), commitMetadata.getExtraMetadata()));
      } else {
        return Option.empty();
      }
    } catch (IOException io) {
      throw new HoodieIOException("Unable to read metadata for instant " + hoodieInstantOption.get(), io);
    }
  }

  /**
   * Get InflightAndRequest instants.
   *
   * @param metaClient
   * @return
   */
  public static Set<String> getInflightAndRequestedInstants(HoodieTableMetaClient metaClient) {
    // collect InflightAndRequest instants for deltaCommit/commit/compaction/clustering
    Set<String> timelineActions = CollectionUtils
        .createImmutableSet(HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.CLUSTERING_ACTION, HoodieTimeline.COMPACTION_ACTION, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.COMMIT_ACTION);
    return metaClient
        .getActiveTimeline()
        .getTimelineOfActions(timelineActions)
        .filterInflightsAndRequested()
        .getInstantsAsStream()
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toSet());
  }

  public static Stream<HoodieInstant> getCompletedInstantsDuringCurrentWriteOperation(HoodieTableMetaClient metaClient, Set<String> pendingInstants) {
    // deal with pendingInstants
    // some pending instants maybe finished during current write operation,
    // we should check the conflict of those pending operation
    return metaClient
        .reloadActiveTimeline()
        .getCommitsTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .filter(f -> pendingInstants.contains(f.getTimestamp()));
  }
}
