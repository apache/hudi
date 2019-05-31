/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.io.Serializable;
import java.util.Optional;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Represents a source from which we can tail data. Assumes a constructor that takes properties.
 */
public abstract class Source<T> implements Serializable {
  protected static volatile Logger log = LogManager.getLogger(Source.class);

  public enum SourceType {
    JSON,
    AVRO,
    ROW
  }

  protected transient TypedProperties props;
  protected transient JavaSparkContext sparkContext;
  protected transient SparkSession sparkSession;
  private transient SchemaProvider overriddenSchemaProvider;

  private final SourceType sourceType;

  protected Source(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    this(props, sparkContext, sparkSession, schemaProvider, SourceType.AVRO);
  }

  protected Source(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
      SchemaProvider schemaProvider, SourceType sourceType) {
    this.props = props;
    this.sparkContext = sparkContext;
    this.sparkSession = sparkSession;
    this.overriddenSchemaProvider = schemaProvider;
    this.sourceType = sourceType;
  }

  protected abstract InputBatch<T> fetchNewData(Optional<String> lastCkptStr, long sourceLimit);

  /**
   * Main API called by Hoodie Delta Streamer to fetch records
   * @param lastCkptStr Last Checkpoint
   * @param sourceLimit Source Limit
   * @return
   */
  public final InputBatch<T> fetchNext(Optional<String> lastCkptStr, long sourceLimit) {
    InputBatch<T> batch = fetchNewData(lastCkptStr, sourceLimit);
    // If overriddenSchemaProvider is passed in CLI, use it
    return overriddenSchemaProvider == null ? batch : new InputBatch<>(batch.getBatch(),
        batch.getCheckpointForNextBatch(), overriddenSchemaProvider);
  }

  public SourceType getSourceType() {
    return sourceType;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }
}
