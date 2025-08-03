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

package org.apache.hudi.client.common;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.SparkFileFormatInternalRowReaderContext;
import org.apache.hudi.client.utils.SparkInternalSchemaConverter;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.SparkColumnarFileReader;
import org.apache.spark.sql.hudi.MultipleColumnarFileFormatReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.SerializableConfiguration;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * Factory that provides the {@link InternalRow} based {@link HoodieReaderContext} for reading data into the spark native format.
 */
class SparkReaderContextFactory implements ReaderContextFactory<InternalRow> {
  private final Broadcast<SparkColumnarFileReader> baseFileReaderBroadcast;
  private final Broadcast<SerializableConfiguration> configurationBroadcast;
  private final Broadcast<HoodieTableConfig> tableConfigBroadcast;

  SparkReaderContextFactory(HoodieSparkEngineContext hoodieSparkEngineContext, HoodieTableMetaClient metaClient) {
    this(hoodieSparkEngineContext, metaClient, new TableSchemaResolver(metaClient), SparkAdapterSupport$.MODULE$.sparkAdapter());
  }

  SparkReaderContextFactory(HoodieSparkEngineContext hoodieSparkEngineContext, HoodieTableMetaClient metaClient,
                            TableSchemaResolver resolver, SparkAdapter sparkAdapter) {
    SQLConf sqlConf = hoodieSparkEngineContext.getSqlContext().sessionState().conf();
    JavaSparkContext jsc = hoodieSparkEngineContext.jsc();

    // Prepare
    boolean returningBatch = sqlConf.parquetVectorizedReaderEnabled();
    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), Boolean.toString(returningBatch)));
    InstantFileNameGenerator fileNameGenerator = metaClient.getTimelineLayout().getInstantFileNameGenerator();
    HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedInstants();
    Map<String, String> schemaEvolutionConfigs =
        getSchemaEvolutionConfigs(resolver, timeline, fileNameGenerator, metaClient.getBasePath().toString());

    // Broadcast: SQLConf.
    // Broadcast: Configuration.
    Configuration configs = getHadoopConfiguration(jsc.hadoopConfiguration());
    schemaEvolutionConfigs.forEach(configs::set);
    configs.set(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE().key(), sqlConf.getConfString(SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE().key()));
    configs.set(SQLConf.PARQUET_WRITE_LEGACY_FORMAT().key(), sqlConf.getConfString(SQLConf.PARQUET_WRITE_LEGACY_FORMAT().key()));
    configurationBroadcast = jsc.broadcast(new SerializableConfiguration(configs));
    // Broadcast: BaseFilereader.
    if (metaClient.getTableConfig().isMultipleBaseFileFormatsEnabled()) {
      SparkColumnarFileReader parquetFileReader = sparkAdapter.createParquetFileReader(false, sqlConf, options, configs);
      SparkColumnarFileReader orcFileReader = getOrcFileReader(resolver, sqlConf, options, configs, sparkAdapter);
      baseFileReaderBroadcast = jsc.broadcast(new MultipleColumnarFileFormatReader(parquetFileReader, orcFileReader));
    } else if (metaClient.getTableConfig().getBaseFileFormat() == HoodieFileFormat.ORC) {
      SparkColumnarFileReader orcFileReader = getOrcFileReader(resolver, sqlConf, options, configs, sparkAdapter);
      baseFileReaderBroadcast = jsc.broadcast(orcFileReader);
    } else {
      baseFileReaderBroadcast = jsc.broadcast(
          sparkAdapter.createParquetFileReader(false, sqlConf, options, configs));
    }
    // Broadcast: TableConfig.
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    tableConfigBroadcast = jsc.broadcast(tableConfig);
  }

  @Override
  public HoodieReaderContext<InternalRow> getContext() {
    if (baseFileReaderBroadcast == null) {
      throw new HoodieException("Spark Parquet reader broadcast is not initialized.");
    }

    if (configurationBroadcast == null) {
      throw new HoodieException("Configuration broadcast is not initialized.");
    }

    if (tableConfigBroadcast == null) {
      throw new HoodieException("Table config broadcast is not initialized.");
    }

    SparkColumnarFileReader baseFileReader = baseFileReaderBroadcast.getValue();
    if (baseFileReader != null) {
      List<Filter> filters = Collections.emptyList();
      return new SparkFileFormatInternalRowReaderContext(
          baseFileReader,
          JavaConverters.asScalaBufferConverter(filters).asScala().toSeq(),
          JavaConverters.asScalaBufferConverter(filters).asScala().toSeq(),
          new HadoopStorageConfiguration(configurationBroadcast.getValue().value()),
          tableConfigBroadcast.getValue());
    } else {
      throw new HoodieException("Cannot get the broadcast Spark Parquet reader.");
    }
  }

  private static SparkColumnarFileReader getOrcFileReader(TableSchemaResolver resolver,
                                                          SQLConf sqlConf,
                                                          scala.collection.immutable.Map<String, String> options,
                                                          Configuration configs,
                                                          SparkAdapter sparkAdapter) {
    try {
      StructType dataSchema = AvroConversionUtils.convertAvroSchemaToStructType(resolver.getTableAvroSchema());
      return sparkAdapter.createOrcFileReader(false, sqlConf, options, configs, dataSchema);
    } catch (Exception e) {
      throw new HoodieException("Failed to broadcast ORC file reader", e);
    }
  }

  private static Configuration getHadoopConfiguration(Configuration configuration) {
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

  private static Map<String, String> getSchemaEvolutionConfigs(TableSchemaResolver schemaResolver,
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
}
