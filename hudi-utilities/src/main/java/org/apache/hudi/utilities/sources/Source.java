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

import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_PERSIST_SOURCE_RDD;
import static org.apache.hudi.config.HoodieWriteConfig.TAGGED_RECORD_STORAGE_LEVEL_VALUE;
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
  private final transient SchemaProvider overriddenSchemaProvider;
  private final SourceType sourceType;
  private final StorageLevel storageLevel;
  private final boolean persistRdd;
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
  }

  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  protected abstract InputBatch<T> fetchNewData(Option<String> lastCkptStr, long sourceLimit);

  /**
   * Main API called by Hoodie Streamer to fetch records.
   *
   * @param lastCkptStr Last Checkpoint
   * @param sourceLimit Source Limit
   * @return
   */
  public final InputBatch<T> fetchNext(Option<String> lastCkptStr, long sourceLimit) {
    InputBatch<T> batch = fetchNewData(lastCkptStr, sourceLimit);
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
    if (persistRdd && isSparkRdd) {
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

  @Override
  public void releaseResources() {
    if (cachedSourceRdd != null && cachedSourceRdd.isLeft()) {
      cachedSourceRdd.asLeft().unpersist();
    } else if (cachedSourceRdd != null && cachedSourceRdd.isRight()) {
      cachedSourceRdd.asRight().unpersist();
    }
  }
}
