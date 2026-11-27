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

package org.apache.hudi.integration.test;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.RowSource;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class BatchRowSource extends RowSource {
  public static final String PROP_BATCH_ROW_SOURCE_PATH = "benchmark.input.source.path";
  private static final Logger LOG = LoggerFactory.getLogger(BatchRowSource.class);

  public BatchRowSource(TypedProperties props,
                        JavaSparkContext sparkContext,
                        SparkSession sparkSession,
                        SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  public BatchRowSource(TypedProperties props,
                        JavaSparkContext sparkContext,
                        SparkSession sparkSession,
                        StreamContext streamContext) {
    super(props, sparkContext, sparkSession, streamContext);
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr,
                                                              long sourceLimit) {
    String sourceDataPath = props.getString(PROP_BATCH_ROW_SOURCE_PATH);
    if (sourceDataPath == null) {
      throw new IllegalArgumentException(PROP_BATCH_ROW_SOURCE_PATH + " is not set.");
    }
    int roundNumber = lastCkptStr.isPresent() ? Integer.parseInt(lastCkptStr.get()) : 0;
    String path = sourceDataPath + "/" + roundNumber;
    StoragePath pathToFetch = new StoragePath(path);
    HoodieStorage storage = HoodieStorageUtils.getStorage(
        pathToFetch, new HadoopStorageConfiguration(sparkContext.hadoopConfiguration()));
    try {
      if (storage.exists(pathToFetch)) {
        return Pair.of(
            Option.of(sparkSession.read().parquet(path + "/*.parquet")),
            Integer.toString(roundNumber + 1));
      }
    } catch (IOException e) {
      LOG.warn("Error checking the following source path: {}", pathToFetch);
    }
    return Pair.of(Option.of(sparkSession.emptyDataFrame()), Integer.toString(roundNumber));
  }
}
