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

package org.apache.hudi.table;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.SparkFileFormatInternalRowReaderContext;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.execution.datasources.FileFormat;
import org.apache.spark.sql.execution.datasources.parquet.SparkParquetReader;
import org.apache.spark.sql.hudi.SparkAdapter;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.collection.JavaConverters;

public class SparkPartialUpdateReaderContext extends PartialUpdateReaderContext {

  private static final Logger LOG = LoggerFactory.getLogger(SparkPartialUpdateReaderContext.class);

  private final transient HoodieEngineContext context;

  protected Broadcast<SQLConf> sqlConfBroadcast;
  protected Option<SparkParquetReader> parquetReaderOpt = Option.empty();
  protected Broadcast<SparkParquetReader> parquetReaderBroadcast;
  protected Broadcast<SerializableConfiguration> configurationBroadcast;

  public SparkPartialUpdateReaderContext(HoodieEngineContext context) {
    this.context = context;
  }

  // Prepare broadcast variables.
  @Override
  public void prepareReaderContext() {
    // this needs to be fixed.
    if (!(context instanceof HoodieSparkEngineContext)) {
      throw new HoodieIOException("Expected to be called using Engine's context and not local context");
    }

    HoodieSparkEngineContext hoodieSparkEngineContext = (HoodieSparkEngineContext) context;
    SQLConf sqlConf = hoodieSparkEngineContext.getSqlContext().sessionState().conf();
    JavaSparkContext jsc = hoodieSparkEngineContext.jsc();

    // TODO: Confirm what is the correct way to set this config.
    boolean returningBatch = sqlConf.parquetVectorizedReaderEnabled();
    scala.collection.immutable.Map<String, String> options =
        scala.collection.immutable.Map$.MODULE$.<String, String>empty()
            .$plus(new Tuple2<>(FileFormat.OPTION_RETURNING_BATCH(), Boolean.toString(returningBatch)));

    // Do broadcast.
    SparkAdapter sparkAdapter = SparkAdapterSupport$.MODULE$.sparkAdapter();
    sqlConfBroadcast = jsc.broadcast(sqlConf);
    configurationBroadcast = jsc.broadcast(new SerializableConfiguration(jsc.hadoopConfiguration()));
    // TODO: Disable vectorization as of now. Assign it based on relevant settings.
    // TODO: Verify if we can construct the reader on the executor side if we has broadcast all necessary variables.
    parquetReaderOpt = Option.of(sparkAdapter.createParquetFileReader(
        false, sqlConfBroadcast.getValue(), options, configurationBroadcast.getValue().value()));
    parquetReaderBroadcast = jsc.broadcast(parquetReaderOpt.get());
  }

  @Override
  public Option<HoodieReaderContext> getReaderContext(StoragePath basePath) {
    if (parquetReaderBroadcast == null) {
      LOG.warn("ParquetReader is not broadcast; cannot use file group reader");
      return Option.empty();
    }

    // Reconstruct metaClient, since underlying properties may not be passed from driver to executors.
    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(new HadoopStorageConfiguration(configurationBroadcast.getValue().value())).build();
    SparkParquetReader sparkParquetReader = parquetReaderBroadcast.getValue();
    if (sparkParquetReader != null) {
      List<Filter> filters = new ArrayList<>();
      return Option.of(new SparkFileFormatInternalRowReaderContext(
          sparkParquetReader,
          // Need to verify this logic.
          metaClient.getTableConfig().getRecordKeyFields().get()[0],
          JavaConverters.asScalaBufferConverter(filters).asScala().toSeq(),
          JavaConverters.asScalaBufferConverter(filters).asScala().toSeq()));
    } else {
      LOG.warn("ParquetFileReader is null");
      return Option.empty();
    }
  }

  @Override
  public Option<Configuration> getStorageConfig() {
    return Option.of(configurationBroadcast.getValue().value());
  }

}
