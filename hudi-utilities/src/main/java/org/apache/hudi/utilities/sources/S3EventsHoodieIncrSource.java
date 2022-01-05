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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.DEFAULT_NUM_INSTANTS_PER_FETCH;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.DEFAULT_SOURCE_FILE_FORMAT;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.HOODIE_SRC_BASE_PATH;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.NUM_INSTANTS_PER_FETCH;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.READ_LATEST_INSTANT_ON_MISSING_CKPT;
import static org.apache.hudi.utilities.sources.HoodieIncrSource.Config.SOURCE_FILE_FORMAT;

/**
 * This source will use the S3 events meta information from hoodie table generate by {@link S3EventsSource}.
 */
public class S3EventsHoodieIncrSource extends HoodieIncrSource {

  private static final Logger LOG = LogManager.getLogger(S3EventsHoodieIncrSource.class);

  static class Config {
    // control whether we do existence check for files before consuming them
    static final String ENABLE_EXISTS_CHECK = "hoodie.deltastreamer.source.s3incr.check.file.exists";
    static final Boolean DEFAULT_ENABLE_EXISTS_CHECK = false;

    // control whether to filter the s3 objects starting with this prefix
    static final String S3_KEY_PREFIX = "hoodie.deltastreamer.source.s3incr.key.prefix";
    static final String S3_FS_PREFIX = "hoodie.deltastreamer.source.s3incr.fs.prefix";
  }

  public S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(HOODIE_SRC_BASE_PATH));
    String srcPath = props.getString(HOODIE_SRC_BASE_PATH);
    int numInstantsPerFetch = props.getInteger(NUM_INSTANTS_PER_FETCH, DEFAULT_NUM_INSTANTS_PER_FETCH);
    boolean readLatestOnMissingCkpt = props.getBoolean(
        READ_LATEST_INSTANT_ON_MISSING_CKPT, DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT);
    IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy = (props.containsKey(HoodieIncrSource.Config.MISSING_CHECKPOINT_STRATEGY))
        ? IncrSourceHelper.MissingCheckpointStrategy.valueOf(props.getString(HoodieIncrSource.Config.MISSING_CHECKPOINT_STRATEGY)) : null;
    if (readLatestOnMissingCkpt) {
      missingCheckpointStrategy = IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST;
    }

    // Use begin Instant if set and non-empty
    Option<String> beginInstant =
        lastCkptStr.isPresent()
            ? lastCkptStr.get().isEmpty() ? Option.empty() : lastCkptStr
            : Option.empty();

    Pair<String, String> instantEndpts =
        IncrSourceHelper.calculateBeginAndEndInstants(
            sparkContext, srcPath, numInstantsPerFetch, beginInstant, missingCheckpointStrategy);

    if (instantEndpts.getKey().equals(instantEndpts.getValue())) {
      LOG.warn("Already caught up. Begin Checkpoint was :" + instantEndpts.getKey());
      return Pair.of(Option.empty(), instantEndpts.getKey());
    }

    // Do incremental pull. Set end instant if available.
    DataFrameReader metaReader = sparkSession.read().format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), instantEndpts.getLeft())
        .option(DataSourceReadOptions.END_INSTANTTIME().key(), instantEndpts.getRight());
    Dataset<Row> source = metaReader.load(srcPath);
    
    if (source.isEmpty()) {
      return Pair.of(Option.empty(), instantEndpts.getRight());
    }

    String filter = "s3.object.size > 0";
    if (!StringUtils.isNullOrEmpty(props.getString(Config.S3_KEY_PREFIX))) {
      filter = filter + " and s3.object.key like '" + props.getString(Config.S3_KEY_PREFIX) + "%'";
    }

    String s3FS = props.getString(Config.S3_FS_PREFIX, "s3").toLowerCase();
    String s3Prefix = s3FS + "://";

    // Extract distinct file keys from s3 meta hoodie table
    final List<Row> cloudMetaDf = source
        .filter(filter)
        .select("s3.bucket.name", "s3.object.key")
        .distinct()
        .collectAsList();
    // Create S3 paths
    final boolean checkExists = props.getBoolean(Config.ENABLE_EXISTS_CHECK, Config.DEFAULT_ENABLE_EXISTS_CHECK);
    List<String> cloudFiles = new ArrayList<>();
    for (Row row : cloudMetaDf) {
      // construct file path, row index 0 refers to bucket and 1 refers to key
      String bucket = row.getString(0);
      String filePath = s3Prefix + bucket + "/" + row.getString(1);
      if (checkExists) {
        FileSystem fs = FSUtils.getFs(s3Prefix + bucket, sparkSession.sparkContext().hadoopConfiguration());
        try {
          if (fs.exists(new Path(filePath))) {
            cloudFiles.add(filePath);
          }
        } catch (IOException e) {
          LOG.error(String.format("Error while checking path exists for %s ", filePath), e);
        }
      } else {
        cloudFiles.add(filePath);
      }
    }
    String fileFormat = props.getString(SOURCE_FILE_FORMAT, DEFAULT_SOURCE_FILE_FORMAT);
    Option<Dataset<Row>> dataset = Option.empty();
    if (!cloudFiles.isEmpty()) {
      dataset = Option.of(sparkSession.read().format(fileFormat).load(cloudFiles.toArray(new String[0])));
    }
    return Pair.of(dataset, instantEndpts.getRight());
  }
}
