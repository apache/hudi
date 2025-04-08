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

import org.apache.hudi.ApiMaturityLevel;
import org.apache.hudi.PublicAPIClass;
import org.apache.hudi.PublicAPIMethod;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.CheckpointUtils;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Either;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.callback.SourceCommitCallback;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.SourceProfileSupplier;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

import static org.apache.hudi.common.table.checkpoint.CheckpointUtils.shouldTargetCheckpointV2;
import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD;
import static org.apache.hudi.config.HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE;
import static org.apache.hudi.config.HoodieWriteConfig.WRITE_TABLE_VERSION;

/**
 * Represents a source from which we can tail data. Assumes a constructor that takes properties.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class Source<T> implements SourceCommitCallback, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(Source.class);

  public enum SourceType {
    JSON, AVRO, ROW, PROTO
  }

  protected transient TypedProperties props;
  protected transient JavaSparkContext sparkContext;
  protected transient SparkSession sparkSession;
  protected transient Option<SourceProfileSupplier> sourceProfileSupplier;
  protected int writeTableVersion;
  private transient SchemaProvider overriddenSchemaProvider;

  private final SourceType sourceType;
  private final StorageLevel storageLevel;
  protected final boolean persistRdd;
  private Either<Dataset<Row>, JavaRDD<?>> cachedSourceRdd = null;

  protected Source(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    this(props, sparkContext, sparkSession, schemaProvider, SourceType.AVRO);
  }

  protected Source(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider, SourceType sourceType) {
    this(props, sparkContext, sparkSession, sourceType, new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  protected Source(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession, SourceType sourceType, StreamContext streamContext) {
    this.props = props;
    this.sparkContext = sparkContext;
    this.sparkSession = sparkSession;
    this.overriddenSchemaProvider = streamContext.getSchemaProvider();
    this.sourceType = sourceType;
    this.sourceProfileSupplier = streamContext.getSourceProfileSupplier();
    this.storageLevel = StorageLevel.fromString(ConfigUtils.getStringWithAltKeys(props, TAGGED_RECORD_STORAGE_LEVEL_VALUE, true));
    this.persistRdd = ConfigUtils.getBooleanWithAltKeys(props, ERROR_TABLE_PERSIST_SOURCE_RDD);
    this.writeTableVersion = ConfigUtils.getIntWithAltKeys(props, WRITE_TABLE_VERSION);
  }

  @Deprecated
  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  protected abstract InputBatch<T> fetchNewData(Option<String> lastCkptStr, long sourceLimit);

  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  protected InputBatch<T> readFromCheckpoint(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    LOG.warn("In Hudi 1.0+, the checkpoint based on Hudi timeline is changed. "
        + "If your Source implementation relies on request time as the checkpoint, "
        + "you may consider migrating to completion time-based checkpoint by overriding "
        + "Source#translateCheckpoint and Source#fetchNewDataFromCheckpoint");
    return fetchNewData(
        lastCheckpoint.isPresent()
            ? Option.of(lastCheckpoint.get().getCheckpointKey()) : Option.empty(),
        sourceLimit);
  }

  /**
   * The second phase of checkpoint resolution - Checkpoint version translation.
   * After the checkpoint value is decided based on the existing configurations at
   * org.apache.hudi.utilities.streamer.StreamerCheckpointUtils#resolveWhatCheckpointToResume,
   *
   * For most of the data sources the there is no difference between checkpoint V1 and V2, it's
   * merely changing the wrapper class.
   *
   * Check child class method overrides to see special case handling.
   * */
  @PublicAPIMethod(maturity = ApiMaturityLevel.EVOLVING)
  protected Option<Checkpoint> translateCheckpoint(Option<Checkpoint> lastCheckpoint) {
    if (lastCheckpoint.isEmpty()) {
      return Option.empty();
    }
    if (CheckpointUtils.shouldTargetCheckpointV2(writeTableVersion, getClass().getName())) {
      // V2 -> V2
      if (lastCheckpoint.get() instanceof StreamerCheckpointV2) {
        return lastCheckpoint;
      }
      // V1 -> V2
      if (lastCheckpoint.get() instanceof StreamerCheckpointV1) {
        StreamerCheckpointV2 newCheckpoint = new StreamerCheckpointV2(lastCheckpoint.get());
        newCheckpoint.addV1Props();
        return Option.of(newCheckpoint);
      }
    } else {
      // V2 -> V1
      if (lastCheckpoint.get() instanceof StreamerCheckpointV2) {
        return Option.of(new StreamerCheckpointV1(lastCheckpoint.get()));
      }
      // V1 -> V1
      if (lastCheckpoint.get() instanceof StreamerCheckpointV1) {
        return lastCheckpoint;
      }
    }
    throw new UnsupportedOperationException("Unsupported checkpoint type: " + lastCheckpoint.get());
  }

  public void assertCheckpointVersion(Option<Checkpoint> lastCheckpoint, Option<Checkpoint> lastCheckpointTranslated, Checkpoint checkpoint) {
    if (checkpoint != null) {
      boolean shouldBeV2Checkpoint = shouldTargetCheckpointV2(writeTableVersion, getClass().getName());
      String errorMessage = String.format(
          "Data source should return checkpoint version V%s. The checkpoint resumed in the iteration is %s, whose translated version is %s. "
              + "The checkpoint returned after the iteration %s.",
          shouldBeV2Checkpoint ? "2" : "1",
          lastCheckpoint.isEmpty() ? "null" : lastCheckpointTranslated.get(),
          lastCheckpointTranslated.isEmpty() ? "null" : lastCheckpointTranslated.get(),
          checkpoint);
      if (shouldBeV2Checkpoint && !(checkpoint instanceof StreamerCheckpointV2)) {
        throw new IllegalStateException(errorMessage);
      }
      if (!shouldBeV2Checkpoint && !(checkpoint instanceof StreamerCheckpointV1)) {
        throw new IllegalStateException(errorMessage);
      }
    }
  }

  /**
   * Main API called by Hoodie Streamer to fetch records.
   *
   * @param lastCheckpoint Last Checkpoint
   * @param sourceLimit Source Limit
   * @return
   */
  public final InputBatch<T> fetchNext(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    Option<Checkpoint> lastCheckpointTranslated = translateCheckpoint(lastCheckpoint);
    InputBatch<T> batch = readFromCheckpoint(lastCheckpointTranslated, sourceLimit);
    batch.getBatch().ifPresent(this::persist);
    // If overriddenSchemaProvider is passed in CLI, use it
    return overriddenSchemaProvider == null ? batch
        : new InputBatch<>(batch.getBatch(), batch.getCheckpointForNextBatch(), overriddenSchemaProvider);
  }

  public SourceType getSourceType() {
    return sourceType;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  private synchronized void persist(T data) {
    boolean isSparkRdd = data.getClass().isAssignableFrom(Dataset.class) || data.getClass().isAssignableFrom(JavaRDD.class);
    if (allowSourcePersist() && isSparkRdd) {
      if (data.getClass().isAssignableFrom(Dataset.class)) {
        Dataset<Row> df = (Dataset<Row>) data;
        cachedSourceRdd = Either.left(df);
        df.persist(storageLevel);
      } else {
        JavaRDD<?> javaRDD = (JavaRDD<?>) data;
        cachedSourceRdd = Either.right(javaRDD);
        javaRDD.persist(storageLevel);
      }
    }
  }

  protected boolean allowSourcePersist() {
    return persistRdd;
  }

  @Override
  public void releaseResources() {
    if (cachedSourceRdd != null && cachedSourceRdd.isLeft()) {
      cachedSourceRdd.asLeft().unpersist();
    } else if (cachedSourceRdd != null && cachedSourceRdd.isRight()) {
      cachedSourceRdd.asRight().unpersist();
    }
  }
}
