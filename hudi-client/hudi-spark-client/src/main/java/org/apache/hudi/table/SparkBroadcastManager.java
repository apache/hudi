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
import org.apache.hudi.client.utils.SparkInternalSchemaConverter;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.internal.schema.InternalSchema;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * Broadcast variable management for Spark.
 */
public class SparkBroadcastManager extends EngineBroadcastManager {

  private final transient HoodieEngineContext context;
  private final transient HoodieTableMetaClient metaClient;

  protected Option<SparkParquetReader> parquetReaderOpt = Option.empty();
  protected Broadcast<SQLConf> sqlConfBroadcast;
  protected Broadcast<SparkParquetReader> parquetReaderBroadcast;
  protected Broadcast<SerializableConfiguration> configurationBroadcast;

  public SparkBroadcastManager(HoodieEngineContext context, HoodieTableMetaClient metaClient) {
    this.context = context;
    this.metaClient = metaClient;
  }

  @Override
  public void prepareAndBroadcast() {
    if (!(context instanceof HoodieSparkEngineContext)) {
      throw new HoodieIOException("Expected to be called using Engine's context and not local context");
    }

    HoodieSparkEngineContext hoodieSparkEngineContext = (HoodieSparkEngineContext) context;
    SQLConf sqlConf = hoodieSparkEngineContext.getSqlContext().sessionState().conf();
    JavaSparkContext jsc = hoodieSparkEngineContext.jsc();

    // Prepare
    boolean returningBatch = sqlConf.parquetVectorizedReaderEnabled();
    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), Boolean.toString(returningBatch)));
    TableSchemaResolver resolver = new TableSchemaResolver(metaClient);
    InstantFileNameGenerator fileNameGenerator = metaClient.getTimelineLayout().getInstantFileNameGenerator();
    HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
    Map<String, String> schemaEvolutionConfigs =
        getSchemaEvolutionConfigs(resolver, timeline, fileNameGenerator, metaClient.getBasePath().toString());

    // Broadcast: SQLConf.
    sqlConfBroadcast = jsc.broadcast(sqlConf);
    // Broadcast: Configuration.
    Configuration configs = getHadoopConfiguration(jsc.hadoopConfiguration());
    addSchemaEvolutionConfigs(configs, schemaEvolutionConfigs);
    configurationBroadcast = jsc.broadcast(new SerializableConfiguration(configs));
    // Broadcast: ParquetReader.
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

  static Configuration getHadoopConfiguration(Configuration configuration) {
    // new Configuration() is critical so that we don't run into ConcurrentModificatonException
    Configuration hadoopConf = new Configuration(configuration);
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
    return (new HadoopStorageConfiguration(hadoopConf).getInline()).unwrap();
  }

  static Map<String, String> getSchemaEvolutionConfigs(TableSchemaResolver schemaResolver,
                                                       HoodieTimeline timeline,
                                                       InstantFileNameGenerator fileNameGenerator,
                                                       String basePath) {
    Option<InternalSchema> internalSchemaOpt = schemaResolver.getTableInternalSchemaFromCommitMetadata();
    Map<String, String> configs = new HashMap<>();
    if (internalSchemaOpt.isPresent()) {
      List<String> instantFiles = timeline.getInstants().stream().map(fileNameGenerator::getFileName).collect(Collectors.toList());
      configs.put(SparkInternalSchemaConverter.HOODIE_VALID_COMMITS_LIST, String.join(",", instantFiles));
      configs.put(SparkInternalSchemaConverter.HOODIE_TABLE_PATH, basePath);
    }
    return configs;
  }

  static void addSchemaEvolutionConfigs(Configuration configs, Map<String, String> schemaEvolutionConfigs) {
    for (Map.Entry<String, String> entry : schemaEvolutionConfigs.entrySet()) {
      configs.set(entry.getKey(), entry.getValue());
    }
  }
}