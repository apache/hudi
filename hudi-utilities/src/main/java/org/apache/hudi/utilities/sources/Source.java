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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.callback.SourceCommitCallback;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Represents a source from which we can tail data. Assumes a constructor that takes properties.
 */
@PublicAPIClass(maturity = ApiMaturityLevel.STABLE)
public abstract class Source<T> implements SourceCommitCallback, Serializable {

  public enum SourceType {
    JSON, AVRO, ROW, PROTO
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

  @PublicAPIMethod(maturity = ApiMaturityLevel.STABLE)
  protected abstract InputBatch<T> fetchNewData(Option<String> lastCkptStr, long sourceLimit);

  /**
   * Main API called by Hoodie Delta Streamer to fetch records.
   * 
   * @param lastCkptStr Last Checkpoint
   * @param sourceLimit Source Limit
   * @return
   */
  public final InputBatch<T> fetchNext(Option<String> lastCkptStr, long sourceLimit) {
    InputBatch<T> batch = fetchNewData(lastCkptStr, sourceLimit);
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
}
/**
This class represents a source from which we can tail data, and assumes a constructor that takes properties.
        The class has a SourceType enum with four values: JSON, AVRO, ROW, and PROTO. This is used to indicate the type of data that is being sourced.
        The class has a constructor that takes several parameters, including TypedProperties, JavaSparkContext, SparkSession, SchemaProvider, and an optional SourceType.
        The constructor initializes these parameters, and the overriddenSchemaProvider is used to override the default schema provider if provided in the CLI.
        The class has an abstract method fetchNewData that must be implemented by subclasses.
        This method is used to fetch new data and return an InputBatch object, which contains the batch of data and the checkpoint for the next batch.
        The class also has a public method fetchNext, which calls fetchNewData and returns an InputBatch object. If the overriddenSchemaProvider is not null, it creates a new InputBatch object using the overridden schema provider.
        The class also has a getSourceType method to get the type of data being sourced, and a getSparkSession method to get the Spark session used by the source.
        Finally, the class implements the SourceCommitCallback interface and is serializable.
   */
