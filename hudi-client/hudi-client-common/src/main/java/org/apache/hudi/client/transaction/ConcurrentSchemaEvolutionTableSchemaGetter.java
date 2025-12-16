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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineLayout;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.util.Lazy;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static org.apache.hudi.avro.AvroSchemaUtils.appendFieldsToSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.containsFieldInSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Helper class to read table schema. ONLY USE IT FOR SimpleConcurrentFileWritesConflictResolutionStrategy.
 */
@Slf4j
class ConcurrentSchemaEvolutionTableSchemaGetter {

  protected final HoodieTableMetaClient metaClient;

  private final Lazy<ConcurrentHashMap<HoodieInstant, Schema>> tableSchemaCache;

  private Option<HoodieInstant> latestCommitWithValidSchema = Option.empty();

  @VisibleForTesting
  public ConcurrentHashMap<HoodieInstant, Schema> getTableSchemaCache() {
    return tableSchemaCache.get();
  }

  public ConcurrentSchemaEvolutionTableSchemaGetter(HoodieTableMetaClient metaClient) {
    this.metaClient = metaClient;
    // Unbounded sized map. Should replace with some caching library.
    this.tableSchemaCache = Lazy.lazily(ConcurrentHashMap::new);
  }

  /**
   * Handles partition column logic for a given schema.
   *
   * @param schema the input schema to process
   * @return the processed schema with partition columns handled appropriately
   */
  private Schema handlePartitionColumnsIfNeeded(Schema schema) {
    if (metaClient.getTableConfig().shouldDropPartitionColumns()) {
      return metaClient.getTableConfig().getPartitionFields()
          .map(partitionFields -> appendPartitionColumns(schema, Option.ofNullable(partitionFields)))
          .or(() -> Option.of(schema))
          .get();
    }
    return schema;
  }

  public Option<Schema> getTableAvroSchemaIfPresent(boolean includeMetadataFields, Option<HoodieInstant> instant) {
    return getTableAvroSchemaFromTimelineWithCache(instant) // Get table schema from schema evolution timeline.
        .or(this::getTableCreateSchemaWithoutMetaField) // Fall back: read create schema from table config.
        .map(tableSchema -> includeMetadataFields ? HoodieAvroUtils.addMetadataFields(tableSchema, false) : HoodieAvroUtils.removeMetadataFields(tableSchema))
        .map(this::handlePartitionColumnsIfNeeded);
  }

  private Option<Schema> getTableCreateSchemaWithoutMetaField() {
    return metaClient.getTableConfig().getTableCreateSchema();
  }

  private void setCachedLatestCommitWithValidSchema(Option<HoodieInstant> instantOption) {
    latestCommitWithValidSchema = instantOption;
  }

  private Option<HoodieInstant> getCachedLatestCommitWithValidSchema() {
    return latestCommitWithValidSchema;
  }

  @VisibleForTesting
  Option<Schema> getTableAvroSchemaFromTimelineWithCache(Option<HoodieInstant> instantTime) {
    return getTableAvroSchemaFromTimelineWithCache(computeSchemaEvolutionTimelineInReverseOrder(), instantTime);
  }

  // [HUDI-9112] simplify the logic
  Option<Schema> getTableAvroSchemaFromTimelineWithCache(Stream<HoodieInstant> reversedTimelineStream, Option<HoodieInstant> instantTime) {
    // If instantTime is empty it means read the latest one. In that case, get the cached instant if there is one.
    boolean fetchFromLastValidCommit = instantTime.isEmpty();
    Option<HoodieInstant> targetInstant = instantTime.or(getCachedLatestCommitWithValidSchema());
    Schema cachedTableSchema = null;

    // Try cache first if there is a target instant to fetch for.
    if (!targetInstant.isEmpty()) {
      cachedTableSchema = tableSchemaCache.get().getOrDefault(targetInstant.get(), null);
    }

    // Cache miss on either latestCommitWithValidSchema or commitMetadataCache. Compute the result.
    if (cachedTableSchema == null) {
      Option<Pair<HoodieInstant, Schema>> instantWithSchema = getLastCommitMetadataWithValidSchemaFromTimeline(reversedTimelineStream, targetInstant);
      if (instantWithSchema.isPresent()) {
        targetInstant = Option.of(instantWithSchema.get().getLeft());
        cachedTableSchema = instantWithSchema.get().getRight();
      }
    }

    // After computation, update the cache for the instant and commit metadata.
    if (fetchFromLastValidCommit) {
      setCachedLatestCommitWithValidSchema(targetInstant);
    }
    if (cachedTableSchema != null) {
      // We save cache for 2 cases
      // - input specifies a specific instant, we update the cache for that instant, which is instantTime.
      // - input is empty implying fetch the latest schema, update the cache for the
      //   latest valid commit which is targetInstant
      if (instantTime.isPresent()) {
        tableSchemaCache.get().putIfAbsent(instantTime.get(), cachedTableSchema);
      }
      if (targetInstant.isPresent()) {
        tableSchemaCache.get().putIfAbsent(targetInstant.get(), cachedTableSchema);
      }
    }

    // Finally process the computation results and return.
    return cachedTableSchema == null ? Option.empty() : Option.of(cachedTableSchema);
  }

  @VisibleForTesting
  Option<Pair<HoodieInstant, Schema>> getLastCommitMetadataWithValidSchemaFromTimeline(Stream<HoodieInstant> reversedTimelineStream, Option<HoodieInstant> instant) {
    // To find the table schema given an instant time, need to walk backwards from the latest instant in
    // the timeline finding a completed instant containing a valid schema.
    ConcurrentHashMap<HoodieInstant, Schema> tableSchemaAtInstant = new ConcurrentHashMap<>();
    Option<HoodieInstant> instantWithTableSchema = Option.fromJavaOptional(reversedTimelineStream
        // If a completion time is specified, find the first eligible instant in the schema evolution timeline.
        // Should switch to completion time based.
        .filter(s -> instant.isEmpty() || compareTimestamps(s.getCompletionTime(), LESSER_THAN_OR_EQUALS, instant.get().getCompletionTime()))
        // Make sure the commit metadata has a valid schema inside. Same caching the result for expensive operation.
        .filter(s -> {
          try {
            // If we processed the instant before, do not parse the commit metadata again.
            if (tableSchemaCache.get().containsKey(s)) {
              tableSchemaAtInstant.putIfAbsent(s, tableSchemaCache.get().get(s));
              return true;
            }
            HoodieCommitMetadata metadata = metaClient.getActiveTimeline().readCommitMetadata(s);
            String schemaStr = metadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY);
            boolean isValidSchemaStr = !StringUtils.isNullOrEmpty(schemaStr);
            if (isValidSchemaStr) {
              tableSchemaAtInstant.putIfAbsent(s, new Schema.Parser().parse(schemaStr));
            }
            return isValidSchemaStr;
          } catch (IOException e) {
            log.warn("Failed to parse commit metadata for instant {} ", s, e);
          }
          return false;
        })
        .findFirst());

    if (instantWithTableSchema.isEmpty()) {
      return Option.empty();
    }
    return Option.of(Pair.of(instantWithTableSchema.get(), tableSchemaAtInstant.get(instantWithTableSchema.get())));
  }

  public static Schema appendPartitionColumns(Schema dataSchema, Option<String[]> partitionFields) {
    // In cases when {@link DROP_PARTITION_COLUMNS} config is set true, partition columns
    // won't be persisted w/in the data files, and therefore we need to append such columns
    // when schema is parsed from data files
    //
    // Here we append partition columns with {@code StringType} as the data type
    if (!partitionFields.isPresent() || partitionFields.get().length == 0) {
      return dataSchema;
    }

    boolean hasPartitionColNotInSchema = Arrays.stream(partitionFields.get()).anyMatch(pf -> !containsFieldInSchema(dataSchema, pf));
    boolean hasPartitionColInSchema = Arrays.stream(partitionFields.get()).anyMatch(pf -> containsFieldInSchema(dataSchema, pf));
    if (hasPartitionColNotInSchema && hasPartitionColInSchema) {
      throw new HoodieSchemaException("Partition columns could not be partially contained w/in the data schema");
    }

    if (hasPartitionColNotInSchema) {
      // when hasPartitionColNotInSchema is true and hasPartitionColInSchema is false, all partition columns
      // are not in originSchema. So we create and add them.
      List<Field> newFields = new ArrayList<>();
      for (String partitionField : partitionFields.get()) {
        newFields.add(new Schema.Field(
            partitionField, createNullableSchema(Schema.Type.STRING), "", JsonProperties.NULL_VALUE));
      }
      return appendFieldsToSchema(dataSchema, newFields);
    }

    return dataSchema;
  }

  /**
   * Get timeline in REVERSE order that only contains completed instants which POTENTIALLY evolve the table schema.
   * For types of instants that are included and not reflecting table schema at their instant completion time please refer
   * comments inside the code.
   */
  public Stream<HoodieInstant> computeSchemaEvolutionTimelineInReverseOrder() {
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    Stream<HoodieInstant> timelineStream = timeline.getInstantsAsStream();
    final Set<String> actions;
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE: {
        actions = new HashSet<>(Arrays.asList(COMMIT_ACTION, REPLACE_COMMIT_ACTION));
        break;
      }
      case MERGE_ON_READ: {
        actions = new HashSet<>(Arrays.asList(DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION));
        break;
      }
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }

    // We only care committed instant when it comes to table schema.
    TimelineLayout timelineLayout = metaClient.getTimelineLayout();
    // Table schema getter is completion time based ordering.
    Comparator<HoodieInstant> reversedComparator = timelineLayout.getInstantComparator().completionTimeOrderedComparator().reversed();

    // The timeline still contains DELTA_COMMIT_ACTION/COMMIT_ACTION which might not contain a valid schema
    // field in their commit metadata.
    // Since the operations of filtering them out are expensive, we should do on-demand stream based
    // filtering when we actually need the table schema.
    Stream<HoodieInstant> reversedTimelineWithTableSchema = timelineStream
        // Only focuses on those who could potentially evolve the table schema.
        .filter(instant -> actions.contains(instant.getAction()))
        // Further filtering out clustering operations as it does not evolve table schema.
        .filter(instant -> !ClusteringUtils.isClusteringInstant(timeline, instant, metaClient.getInstantGenerator()))
        .filter(HoodieInstant::isCompleted)
        // We reverse the order as the operation against this timeline would be very efficient if
        // we always start from the tail.
        .sorted(reversedComparator);
    return reversedTimelineWithTableSchema;
  }
}
