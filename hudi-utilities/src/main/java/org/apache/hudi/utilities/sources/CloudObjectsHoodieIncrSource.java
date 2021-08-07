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

import org.apache.hudi.DataSourceReadOptions;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Cloud Objects Hoodie Incr Source Class. {@link CloudObjectsHoodieIncrSource}.This source will use
 * the cloud files meta information form cloud meta hoodie table generate by CloudObjectsMetaSource.
 */
public class CloudObjectsHoodieIncrSource extends HoodieIncrSource {

  private static final Logger LOG = LogManager.getLogger(CloudObjectsHoodieIncrSource.class);

  public CloudObjectsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(
      Option<String> lastCkptStr, long sourceLimit) {

    DataSourceUtils.checkRequiredProperties(
        props, Collections.singletonList(Config.HOODIE_SRC_BASE_PATH));

    String srcPath = props.getString(Config.HOODIE_SRC_BASE_PATH);
    int numInstantsPerFetch =
        props.getInteger(Config.NUM_INSTANTS_PER_FETCH, Config.DEFAULT_NUM_INSTANTS_PER_FETCH);
    boolean readLatestOnMissingCkpt =
        props.getBoolean(
            Config.READ_LATEST_INSTANT_ON_MISSING_CKPT,
            Config.DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT);

    // Use begin Instant if set and non-empty
    Option<String> beginInstant =
        lastCkptStr.isPresent()
            ? lastCkptStr.get().isEmpty() ? Option.empty() : lastCkptStr
            : Option.empty();

    Pair<String, String> instantEndpts =
        IncrSourceHelper.calculateBeginAndEndInstants(
            sparkContext, srcPath, numInstantsPerFetch, beginInstant, readLatestOnMissingCkpt);

    if (instantEndpts.getKey().equals(instantEndpts.getValue())) {
      LOG.warn("Already caught up. Begin Checkpoint was :" + instantEndpts.getKey());
      return Pair.of(Option.empty(), instantEndpts.getKey());
    }

    // Do Incr pull. Set end instant if available
    DataFrameReader reader =
        sparkSession
            .read()
            .format("org.apache.hudi")
            .option(
                DataSourceReadOptions.QUERY_TYPE().key(),
                DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
            .option(
                DataSourceReadOptions.BEGIN_INSTANTTIME().key(), instantEndpts.getLeft())
            .option(
                DataSourceReadOptions.END_INSTANTTIME().key(), instantEndpts.getRight());

    Dataset<Row> source = reader.load(srcPath);

    // Extract distinct file keys from cloud meta hoodie table
    final List<Row> cloudMetaDf =
        source
            .filter("s3.object.size > 0")
            .select("s3.bucket.name", "s3.object.key")
            .distinct()
            .collectAsList();

    // Create S3 paths
    List<String> cloudFiles = new ArrayList<>();
    for (Row row : cloudMetaDf) {
      String bucket = row.getString(0);
      String key = row.getString(1);
      String filePath = "s3://" + bucket + "/" + key;
      cloudFiles.add(filePath);
    }
    String pathStr = String.join(",", cloudFiles);

    return Pair.of(Option.of(fromFiles(pathStr)), instantEndpts.getRight());
  }

  /**
   * Function to create Dataset from parquet files.
   */
  private Dataset<Row> fromFiles(String pathStr) {
    return sparkSession.read().parquet(pathStr.split(","));
  }
}
