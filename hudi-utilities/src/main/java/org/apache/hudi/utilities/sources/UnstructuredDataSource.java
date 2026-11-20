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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.blob.BlobSourceSetup;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.CloudObjectIncrCheckpoint;
import org.apache.hudi.utilities.sources.helpers.CloudObjectsSelectorCommon;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;
import org.apache.hudi.utilities.sources.helpers.QueryInfo;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredCloud;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredMode;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredObjectMaterializer;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSchemaContract;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSchemaGuard;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.checkRequiredConfigProperties;
import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH;
import static org.apache.hudi.utilities.config.HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getHollowCommitHandleMode;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getMissingCheckpointStrategy;

/**
 * The source of every unstructured stream: raw unstructured bytes of the objects a cloud events Hudi table announces, selected
 * incrementally with requested-time checkpoints, for one of two jobs and one of two clouds.
 * <ul>
 *   <li>The job is {@code hoodie.unstructured.source.mode} ({@link UnstructuredMode}): {@code blob} writes the blob table through
 *   the quarantine transformer, {@code embedding} the embedding table through the embedding transformer. The streamer runs that
 *   transformer in every round ({@link UnstructuredSourceContract}), with the job's own {@code --transformer-class} entries after it,
 *   and refuses the other job's transformer.</li>
 *   <li>The cloud is {@code hoodie.unstructured.source.cloud} ({@link UnstructuredCloud}): {@code s3} reads the events table of the
 *   S3 events source, {@code gcs} the one of the GCS events source. Only the events table's key and size columns and the selector
 *   type differ; the selection, the status call and the bytes are read the same way on both.</li>
 * </ul>
 * Small objects become inline blob rows; large objects carry pending references whose path stays null: the blob job's copy
 * transformer copies the bytes and fills it, and the embedding job's transformer reads the object from its
 * {@code source_file_path}. An object whose status cannot be read, a missing object included, or whose inline bytes cannot be
 * read fails the round.
 *
 * <p>The job's requirements are its contract's ({@link UnstructuredSourceContract}): the settings it runs with, which the streamer
 * merges into a copy of the job's properties before anything reads them, the transformers and the schema check. The source is
 * built from those merged properties and reads none of the settings. Construction validates the job's other properties
 * ({@link BlobSourceSetup#validate}) without changing them or reading the events table.
 *
 * <p>A user's target schema (the stream's schema provider) must be compatible with the columns the job writes
 * ({@link UnstructuredSchemaContract}): it is checked when the source is built and again at the start of every round, before the
 * round reads anything or starts its instant, since a provider may change its schema between rounds.
 */
@Slf4j
public class UnstructuredDataSource extends HoodieIncrSource {

  private final UnstructuredMode mode;
  private final UnstructuredCloud cloud;
  private final String srcPath;
  private final int numInstantsPerFetch;
  private final IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy;
  private final QueryRunner queryRunner;
  private final CloudDataFetcher cloudDataFetcher;
  private final Option<SchemaProvider> schemaProvider;
  private final Option<SnapshotLoadQuerySplitter> snapshotLoadQuerySplitter;

  public UnstructuredDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                                SchemaProvider schemaProvider, HoodieIngestionMetrics metrics) {
    this(props, sparkContext, sparkSession, metrics, new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public UnstructuredDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                                HoodieIngestionMetrics metrics, StreamContext streamContext) {
    this(validate(props), sparkContext, sparkSession, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, sparkContext, sparkSession, metrics, new CloudObjectsSelectorCommon(props),
            new UnstructuredObjectMaterializer(props, sparkContext)),
        streamContext);
  }

  /** Builds the source with the given query runner and fetcher, which tests replace. */
  UnstructuredDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession, QueryRunner queryRunner,
                         CloudDataFetcher cloudDataFetcher, StreamContext streamContext) {
    super(props, sparkContext, sparkSession, streamContext);
    this.mode = UnstructuredMode.from(props);
    this.cloud = UnstructuredCloud.from(props);
    checkRequiredConfigProperties(props, Collections.singletonList(HOODIE_SRC_BASE_PATH));
    this.srcPath = getStringWithAltKeys(props, HOODIE_SRC_BASE_PATH);
    this.numInstantsPerFetch = getIntWithAltKeys(props, NUM_INSTANTS_PER_FETCH);
    this.missingCheckpointStrategy = getMissingCheckpointStrategy(props);
    this.queryRunner = queryRunner;
    this.cloudDataFetcher = cloudDataFetcher;
    this.schemaProvider = Option.ofNullable(streamContext.getSchemaProvider());
    this.snapshotLoadQuerySplitter = SnapshotLoadQuerySplitter.getInstance(props);
    checkUserTargetSchema();
  }

  /**
   * Refuses a user target schema that is not compatible with the columns the job writes ({@link UnstructuredSchemaContract}). A
   * stream with no schema provider, or one whose target schema is unset or the null schema, has none.
   */
  private void checkUserTargetSchema() {
    if (!schemaProvider.isPresent()) {
      return;
    }
    HoodieSchema target = schemaProvider.get().getTargetHoodieSchema();
    if (target == null || target.getType() == HoodieSchemaType.NULL) {
      return;
    }
    UnstructuredSchemaContract.checkTarget(mode, UnstructuredSchemaContract.contract(mode, props, Option.empty()), target);
  }

  /**
   * The job's checks ({@link UnstructuredSourceContract#validateJob}), before anything else of the source is built; the streamer has
   * already run them with the merge. Returns {@code props} unchanged.
   */
  private static TypedProperties validate(TypedProperties props) {
    return UnstructuredSourceContract.validateJob(props);
  }

  /** Refuses an existing table with a different record key before the writer's stock table checks. */
  public static void validate(UnstructuredMode mode, String basePath, HoodieTableConfig table) {
    String expected = String.join(",", mode.keyColumns());
    String stored = table.getString(HoodieTableConfig.RECORDKEY_FIELDS);
    if (stored == null || !expected.equals(UnstructuredSourceContract.fields(stored))) {
      throw new IllegalArgumentException(name(mode) + " requires the table's " + HoodieTableConfig.RECORDKEY_FIELDS.key() + "=" + expected
          + ": the table at " + basePath + " stores " + (stored == null ? "none" : stored) + "; write this job to a new table");
    }
  }

  /** Plans the job's chain without reading source data, before constructing the source. */
  public static void validate(UnstructuredMode mode, JavaSparkContext jsc, SparkSession spark, List<String> chain,
                              TypedProperties props, Option<HoodieSchema> targetSchema, Option<StructType> tableSchema) {
    UnstructuredSchemaContract.checkChainPlan(mode, mode.requiredTransformer(), UnstructuredSchemaGuard.class, chain,
        jsc, spark, props, targetSchema, tableSchema);
  }

  /** The name every refusal of {@code mode}'s job carries, such as {@code UnstructuredDataSource (mode blob)}. */
  public static String name(UnstructuredMode mode) {
    return UnstructuredDataSource.class.getSimpleName() + " (mode " + mode.configValue() + ")";
  }

  /** The job this source serves. */
  public UnstructuredMode mode() {
    return mode;
  }

  /** The cloud whose events table this source reads. */
  public UnstructuredCloud cloud() {
    return cloud;
  }

  @Override
  protected Option<Checkpoint> translateCheckpoint(Option<Checkpoint> lastCheckpoint) {
    if (lastCheckpoint.isPresent()) {
      ValidationUtils.checkArgument(lastCheckpoint.get() instanceof StreamerCheckpointV1,
          "For UnstructuredDataSource, only StreamerCheckpointV1, i.e., requested time-based "
              + "checkpoint, is supported. Checkpoint provided is: " + lastCheckpoint.get());
    }
    return lastCheckpoint;
  }

  /**
   * Checks the user's target schema again, as the provider gives it now, before any data is read; then selects the round's
   * objects from the events table with the cloud's key and size columns and reads them. Caught up, the round is empty and its checkpoint is the end
   * instant on both clouds.
   */
  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    checkUserTargetSchema();
    CloudObjectIncrCheckpoint cloudObjectIncrCheckpoint = CloudObjectIncrCheckpoint.fromString(lastCheckpoint);
    HollowCommitHandling handlingMode = getHollowCommitHandleMode(props);
    QueryInfo queryInfo =
        IncrSourceHelper.generateQueryInfo(
            sparkContext, srcPath, numInstantsPerFetch,
            Option.of(new StreamerCheckpointV1(cloudObjectIncrCheckpoint.getCommit())),
            missingCheckpointStrategy, handlingMode,
            HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            cloud.keyColumn(), cloud.sizeColumn(), true,
            Option.ofNullable(cloudObjectIncrCheckpoint.getKey()), metricsOption);
    log.info("Querying {} events with:{}, queryInfo:{}", cloud.configValue(), cloudObjectIncrCheckpoint, queryInfo);

    if (isNullOrEmpty(cloudObjectIncrCheckpoint.getKey()) && queryInfo.areStartAndEndInstantsEqual()) {
      log.info("Already caught up. No new data to process");
      return Pair.of(Option.empty(), new StreamerCheckpointV1(queryInfo.getEndInstant()));
    }
    return cloudDataFetcher.fetchPartitionedSource(cloud.selectorType(), cloudObjectIncrCheckpoint, this.sourceProfileSupplier,
        queryRunner.run(queryInfo, snapshotLoadQuerySplitter), this.schemaProvider, sourceLimit);
  }

  /** Logs the committed checkpoint after a round commits, with the job and the cloud. */
  @Override
  public void onCommit(String lastCkptStr) {
    super.onCommit(lastCkptStr);
    log.info("{} round committed at checkpoint {} (cloud {})", name(mode), lastCkptStr, cloud.configValue());
  }
}
