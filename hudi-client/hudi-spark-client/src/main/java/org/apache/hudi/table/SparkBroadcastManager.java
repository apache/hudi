/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.SparkFileFormatInternalRowReaderContext;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetReader;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * Broadcast variable management for Spark.
 */
public class SparkBroadcastManager extends EngineBroadcastManager {

  private static final Logger LOG = LoggerFactory.getLogger(SparkBroadcastManager.class);

  private final transient HoodieEngineContext context;

  protected Option<SparkParquetReader> parquetReaderOpt = Option.empty();
  protected Broadcast<SQLConf> sqlConfBroadcast;
  protected Broadcast<SparkParquetReader> parquetReaderBroadcast;
  protected Broadcast<SerializableConfiguration> configurationBroadcast;

  public SparkBroadcastManager(HoodieEngineContext context) {
    this.context = context;
  }

  @Override
  public void prepareAndBroadcast() {
    if (!(context instanceof HoodieSparkEngineContext)) {
      throw new HoodieIOException("Expected to be called using Engine's context and not local context");
    }

    HoodieSparkEngineContext hoodieSparkEngineContext = (HoodieSparkEngineContext) context;
    SQLConf sqlConf = hoodieSparkEngineContext.getSqlContext().sessionState().conf();
    JavaSparkContext jsc = hoodieSparkEngineContext.jsc();

    boolean returningBatch = sqlConf.parquetVectorizedReaderEnabled();
    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), Boolean.toString(returningBatch)));

    // Do broadcast.
    sqlConfBroadcast = jsc.broadcast(sqlConf);
    // new Configuration() is critical so that we don't run into ConcurrentModificatonException
    Configuration hadoopConf = new Configuration(jsc.hadoopConfiguration());
    hadoopConf.setBoolean(SQLConf.NESTED_SCHEMA_PRUNING_ENABLED().key(), false);
    hadoopConf.setBoolean(SQLConf.CASE_SENSITIVE().key(), false);
    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(SQLConf.PARQUET_BINARY_AS_STRING().key(), false);
    hadoopConf.setBoolean(SQLConf.PARQUET_INT96_AS_TIMESTAMP().key(), true);
    // Using string value of this conf to preserve compatibility across spark versions.
    hadoopConf.setBoolean("spark.sql.legacy.parquet.nanosAsLong", false);
    if (HoodieSparkUtils.gteqSpark3_4()) {
      // PARQUET_INFER_TIMESTAMP_NTZ_ENABLED is required from Spark 3.4.0 or above
      hadoopConf.setBoolean("spark.sql.parquet.inferTimestampNTZ.enabled", false);
    }
    StorageConfiguration config = new HadoopStorageConfiguration(hadoopConf).getInline();

    configurationBroadcast = jsc.broadcast(new SerializableConfiguration((Configuration) config.unwrap()));
    // Spark parquet reader has to be instantiated on the driver and broadcast to the executors
    parquetReaderOpt = Option.of(SparkAdapterSupport$.MODULE$.sparkAdapter().createParquetFileReader(
        false, sqlConfBroadcast.getValue(), options, configurationBroadcast.getValue().value()));
    parquetReaderBroadcast = jsc.broadcast(parquetReaderOpt.get());
  }

  @Override
  public Option<HoodieReaderContext> retrieveFileGroupReaderContext(StoragePath basePath) {
    if (parquetReaderBroadcast == null) {
      throw new HoodieException("Spark Parquet reader broadcast is not initialized.");
    }

    SparkParquetReader sparkParquetReader = parquetReaderBroadcast.getValue();
    if (sparkParquetReader != null) {
      List<Filter> filters = new ArrayList<>();
      return Option.of(new SparkFileFormatInternalRowReaderContext(
          sparkParquetReader,
          JavaConverters.asScalaBufferConverter(filters).asScala().toSeq(),
          JavaConverters.asScalaBufferConverter(filters).asScala().toSeq()));
    } else {
      throw new HoodieException("Cannot get the broadcast Spark Parquet reader.");
    }
  }

  @Override
  public Option<Configuration> retrieveStorageConfig() {
    return Option.of(configurationBroadcast.getValue().value());
  }
}