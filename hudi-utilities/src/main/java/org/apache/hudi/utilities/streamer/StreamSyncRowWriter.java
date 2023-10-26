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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.HoodieSparkSqlWriter;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.commit.DatasetBulkInsertCommitActionExecutor;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.utilities.exception.HoodieStreamerException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class StreamSyncRowWriter extends StreamSync<Dataset<Row>> {
  public StreamSyncRowWriter(HoodieStreamer.Config cfg, SparkSession sparkSession, SchemaProvider schemaProvider,
                             TypedProperties props, HoodieSparkEngineContext hoodieSparkContext, FileSystem fs, Configuration conf,
                             Function<SparkRDDWriteClient, Boolean> onInitializingHoodieWriteClient)
      throws IOException {
    super(cfg, sparkSession, schemaProvider, props, hoodieSparkContext, fs, conf, onInitializingHoodieWriteClient);
  }

  @Override
  protected InputBatch<Dataset<Row>> fetchFromSourceWithTransformerAndUserSchemaProvider(Option<String> resumeCheckpointStr, boolean reconcileSchema, String instantTime) {
    InputBatch<Dataset<Row>> dataAndCheckpoint = fetchDataAndTransform(resumeCheckpointStr);
    if (dataAndCheckpoint == null) {
      return null;
    }
    schemaProvider = this.userProvidedSchemaProvider;
    return checkEmpty(new InputBatch<>(dataAndCheckpoint.getBatch(), dataAndCheckpoint.getCheckpointForNextBatch(), this.userProvidedSchemaProvider));
  }

  @Override
  protected InputBatch<Dataset<Row>> fetchFromSourceWithTransformerWithoutUserSchemaProvider(Option<String> resumeCheckpointStr, HoodieTableMetaClient metaClient,
                                                                                             boolean reconcileSchema, String instantTime) {
    InputBatch<Dataset<Row>> dataAndCheckpoint = fetchDataAndTransform(resumeCheckpointStr);
    if (dataAndCheckpoint == null) {
      return null;
    }
    schemaProvider = dataAndCheckpoint.getSchemaProvider();
    return checkEmpty(dataAndCheckpoint);
  }

  @Override
  protected InputBatch<Dataset<Row>> fetchFromSourceWithoutTransformer(Option<String> resumeCheckpointStr, HoodieTableMetaClient metaClient, String instantTime) {
    InputBatch<Dataset<Row>> dataAndCheckpoint = fetchNewDataInRowFormat(resumeCheckpointStr, cfg.sourceLimit);
    if (dataAndCheckpoint == null) {
      return null;
    }
    schemaProvider = dataAndCheckpoint.getSchemaProvider();
    return checkEmpty(dataAndCheckpoint);
  }

  @Override
  protected Pair<JavaRDD<WriteStatus>, Map<String, List<String>>> doWrite(String instantTime, InputBatch<Dataset<Row>> inputBatch) {
    Dataset<Row> df = inputBatch.getBatch().get();
    JavaRDD<WriteStatus> writeStatusRDD;
    switch (cfg.operation) {
      case INSERT:
      case UPSERT:
      case INSERT_OVERWRITE:
      case INSERT_OVERWRITE_TABLE:
      case DELETE_PARTITION:
        throw new HoodieStreamerException("Unsupported operation for row writer: " + cfg.operation);
      case BULK_INSERT:
        HoodieWriteConfig hoodieWriteConfig = HoodieSparkSqlWriter.getBulkInsertRowConfig(inputBatch.getSchemaProvider().getTargetSchema(),
            new HoodieConfig(HoodieStreamer.Config.getProps(fs, cfg)), cfg.targetBasePath, cfg.targetTableName);
        DatasetBulkInsertCommitActionExecutor executor = new DatasetBulkInsertCommitActionExecutor(hoodieWriteConfig, writeClient, instantTime, false);
        writeStatusRDD = executor.execute(df, !getPartitionColumns(props).isEmpty()).getWriteStatuses();
        break;
      default:
        throw new HoodieStreamerException("Unknown operation : " + cfg.operation);
    }

    return Pair.of(writeStatusRDD, Collections.emptyMap());
  }

  @Override
  protected boolean isEmpty(InputBatch<Dataset<Row>> inputBatch) {
    return inputBatch.getBatch().get().isEmpty();
  }

  @Override
  protected void setupWriteClient(InputBatch<Dataset<Row>> inputBatch) throws IOException {
    setupWriteClient(hoodieSparkContext.emptyRDD());
  }

  @Override
  protected void reInitWriteClient(Schema sourceSchema, Schema targetSchema, InputBatch<Dataset<Row>> inputBatch) throws IOException {
    reInitWriteClient(sourceSchema, targetSchema, hoodieSparkContext.emptyRDD());
  }

  private InputBatch<Dataset<Row>> checkEmpty(InputBatch<Dataset<Row>> inputBatch) {
    hoodieSparkContext.setJobStatus(this.getClass().getSimpleName(), "Checking if input is empty");
    if ((!inputBatch.getBatch().isPresent()) || (inputBatch.getBatch().get().isEmpty())) {
      LOG.info("No new data, perform empty commit.");
      return new InputBatch<>(Option.of(sparkSession.emptyDataFrame()), inputBatch.getCheckpointForNextBatch(), inputBatch.getSchemaProvider());
    }
    return inputBatch;
  }

}
