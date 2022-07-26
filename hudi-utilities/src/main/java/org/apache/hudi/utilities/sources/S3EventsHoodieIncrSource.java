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
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import com.esotericsoftware.minlog.Log;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
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
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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

    // control whether to ignore the s3 objects starting with this prefix
    static final String S3_IGNORE_KEY_PREFIX = "hoodie.deltastreamer.source.s3incr.ignore.key.prefix";
    // control whether to ignore the s3 objects with this substring
    static final String S3_IGNORE_KEY_SUBSTRING = "hoodie.deltastreamer.source.s3incr.ignore.key.substring";
    /**
     *{@value #SPARK_DATASOURCE_OPTIONS} is json string, passed to the reader while loading dataset.
     * Example delta streamer conf
     * - --hoodie-conf hoodie.deltastreamer.source.s3incr.spark.datasource.options={"header":"true","encoding":"UTF-8"}
     */
    static final String SPARK_DATASOURCE_OPTIONS = "hoodie.deltastreamer.source.s3incr.spark.datasource.options";
  }

  public S3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  private DataFrameReader getDataFrameReader(String fileFormat) {
    DataFrameReader dataFrameReader = sparkSession.read().format(fileFormat);
    if (!StringUtils.isNullOrEmpty(props.getString(Config.SPARK_DATASOURCE_OPTIONS, null))) {
      final ObjectMapper mapper = new ObjectMapper();
      Map<String, String> sparkOptionsMap = null;
      try {
        sparkOptionsMap = mapper.readValue(props.getString(Config.SPARK_DATASOURCE_OPTIONS), Map.class);
      } catch (IOException e) {
        throw new HoodieException(String.format("Failed to parse sparkOptions: %s", props.getString(Config.SPARK_DATASOURCE_OPTIONS)), e);
      }
      Log.info(String.format("sparkOptions loaded: %s", sparkOptionsMap));
      dataFrameReader = dataFrameReader.options(sparkOptionsMap);
    }
    return dataFrameReader;
  }

  public Pair<Option<Dataset<Row>>, String> fetchMetadata(Option<String> lastCkptStr, long sourceLimit) {
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

    Pair<String, Pair<String, String>> queryTypeAndInstantEndpts =
        IncrSourceHelper.calculateBeginAndEndInstants(
            sparkContext, srcPath, numInstantsPerFetch, beginInstant, missingCheckpointStrategy);

    if (queryTypeAndInstantEndpts.getValue().getKey().equals(queryTypeAndInstantEndpts.getValue().getValue())) {
      LOG.warn("Already caught up. Begin Checkpoint was :" + queryTypeAndInstantEndpts.getValue().getKey());
      return Pair.of(Option.empty(), queryTypeAndInstantEndpts.getRight().getRight());
    }

    Dataset<Row> source = null;
    // Do incremental pull. Set end instant if available.
    if (queryTypeAndInstantEndpts.getKey().equals(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())) {
      source = sparkSession.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), queryTypeAndInstantEndpts.getRight().getLeft())
          .option(DataSourceReadOptions.END_INSTANTTIME().key(), queryTypeAndInstantEndpts.getRight().getRight()).load(srcPath);
    } else {
      // if checkpoint is missing from source table, and if strategy is set to READ_UPTO_LATEST_COMMIT, we have to issue snapshot query
      source = sparkSession.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL()).load(srcPath)
          // add filtering so that only interested records are returned.
          .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
              queryTypeAndInstantEndpts.getRight().getLeft()));
    }
    return Pair.of(Option.of(source), queryTypeAndInstantEndpts.getRight().getRight());
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    Pair<Option<Dataset<Row>>, String> sourceMetadata = fetchMetadata(lastCkptStr, sourceLimit);
    if (!sourceMetadata.getKey().isPresent()) {
      return Pair.of(Option.empty(), sourceMetadata.getRight());
    }

    Dataset<Row> source = sourceMetadata.getKey().get();
    String fileFormat = props.getString(SOURCE_FILE_FORMAT, DEFAULT_SOURCE_FILE_FORMAT);
    String filter = "s3.object.size > 0";
    if (!StringUtils.isNullOrEmpty(props.getString(Config.S3_KEY_PREFIX, null))) {
      filter = filter + " and s3.object.key like '" + props.getString(Config.S3_KEY_PREFIX) + "%'";
    }
    if (!StringUtils.isNullOrEmpty(props.getString(Config.S3_IGNORE_KEY_PREFIX, null))) {
      filter = filter + " and s3.object.key not like '" + props.getString(Config.S3_IGNORE_KEY_PREFIX) + "%'";
    }
    if (!StringUtils.isNullOrEmpty(props.getString(Config.S3_IGNORE_KEY_SUBSTRING, null))) {
      filter = filter + " and s3.object.key not like '%" + props.getString(Config.S3_IGNORE_KEY_SUBSTRING) + "%'";
    }
    // add file format filtering by default
    filter = filter + " and s3.object.key like '%" + fileFormat + "%'";

    String s3FS = props.getString(Config.S3_FS_PREFIX, "s3").toLowerCase();
    String s3Prefix = s3FS + "://";

    // Create S3 paths
    final boolean checkExists = props.getBoolean(Config.ENABLE_EXISTS_CHECK, Config.DEFAULT_ENABLE_EXISTS_CHECK);
    SerializableConfiguration serializableConfiguration = new SerializableConfiguration(sparkContext.hadoopConfiguration());
    List<String> cloudFiles = source
        .filter(filter)
        .select("s3.bucket.name", "s3.object.key")
        .distinct()
        .rdd()
        // JavaRDD simplifies coding with collect and suitable mapPartitions signature. check if this can be avoided.
        .toJavaRDD()
        .mapPartitions(fileListIterator -> {
          List<String> cloudFilesPerPartition = new ArrayList<>();
          fileListIterator.forEachRemaining(row -> {
            // TODO: configuration is updated in the getFs call. check if new copy is needed w.r.t to getFs.
            final Configuration configuration = serializableConfiguration.newCopy();
            String bucket = row.getString(0);
            String filePath = s3Prefix + bucket + "/" + row.getString(1);
            try {
              String decodeUrl = URLDecoder.decode(filePath, StandardCharsets.UTF_8.name());
              if (checkExists) {
                FileSystem fs = FSUtils.getFs(s3Prefix + bucket, configuration);
                try {
                  if (fs.exists(new Path(decodeUrl))) {
                    cloudFilesPerPartition.add(decodeUrl);
                  }
                } catch (IOException e) {
                  LOG.error(String.format("Error while checking path exists for %s ", decodeUrl), e);
                }
              } else {
                cloudFilesPerPartition.add(decodeUrl);
              }
            } catch (Exception exception) {
              LOG.warn("Failed to add cloud file ", exception);
            }
          });
          return cloudFilesPerPartition.iterator();
        }).collect();

    Option<Dataset<Row>> dataset = Option.empty();
    if (!cloudFiles.isEmpty()) {
      DataFrameReader dataFrameReader = getDataFrameReader(fileFormat);
      dataset = Option.of(dataFrameReader.load(cloudFiles.toArray(new String[0])));
    }
    LOG.debug("Extracted distinct files " + cloudFiles.size()
        + " and some samples " + cloudFiles.stream().limit(10).collect(Collectors.toList()));
    return Pair.of(dataset, sourceMetadata.getRight());
  }
}