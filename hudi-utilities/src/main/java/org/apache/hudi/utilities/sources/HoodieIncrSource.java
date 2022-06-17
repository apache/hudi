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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.sync.common.model.partextractor.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;

public class HoodieIncrSource extends RowSource {

  private static final Logger LOG = LogManager.getLogger(HoodieIncrSource.class);

  static class Config {

    /**
     * {@value #HOODIE_SRC_BASE_PATH} is the base-path for the source Hoodie table.
     */
    static final String HOODIE_SRC_BASE_PATH = "hoodie.deltastreamer.source.hoodieincr.path";

    /**
     * {@value #NUM_INSTANTS_PER_FETCH} allows the max number of instants whose changes can be incrementally fetched.
     */
    static final String NUM_INSTANTS_PER_FETCH = "hoodie.deltastreamer.source.hoodieincr.num_instants";
    static final Integer DEFAULT_NUM_INSTANTS_PER_FETCH = 1;

    /**
     * {@value #HOODIE_SRC_PARTITION_FIELDS} specifies partition fields that needs to be added to source table after
     * parsing _hoodie_partition_path.
     */
    static final String HOODIE_SRC_PARTITION_FIELDS = "hoodie.deltastreamer.source.hoodieincr.partition.fields";

    /**
     * {@value #HOODIE_SRC_PARTITION_EXTRACTORCLASS} PartitionValueExtractor class to extract partition fields from
     * _hoodie_partition_path.
     */
    static final String HOODIE_SRC_PARTITION_EXTRACTORCLASS =
        "hoodie.deltastreamer.source.hoodieincr.partition.extractor.class";
    static final String DEFAULT_HOODIE_SRC_PARTITION_EXTRACTORCLASS =
        SlashEncodedDayPartitionValueExtractor.class.getCanonicalName();

    /**
     * {@value #READ_LATEST_INSTANT_ON_MISSING_CKPT} allows delta-streamer to incrementally fetch from latest committed
     * instant when checkpoint is not provided. This config is deprecated. Please refer to {@link #MISSING_CHECKPOINT_STRATEGY}.
     */
    @Deprecated
    static final String READ_LATEST_INSTANT_ON_MISSING_CKPT =
        "hoodie.deltastreamer.source.hoodieincr.read_latest_on_missing_ckpt";
    static final Boolean DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT = false;

    /**
     * {@value #MISSING_CHECKPOINT_STRATEGY} allows delta-streamer to decide the checkpoint to consume from when checkpoint is not set.
     * instant when checkpoint is not provided.
     */
    static final String MISSING_CHECKPOINT_STRATEGY = "hoodie.deltastreamer.source.hoodieincr.missing.checkpoint.strategy";

    /**
     * {@value #SOURCE_FILE_FORMAT} is passed to the reader while loading dataset. Default value is parquet.
     */
    static final String SOURCE_FILE_FORMAT = "hoodie.deltastreamer.source.hoodieincr.file.format";
    static final String DEFAULT_SOURCE_FILE_FORMAT = "parquet";
  }

  public HoodieIncrSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {

    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.HOODIE_SRC_BASE_PATH));

    /*
     * DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.HOODIE_SRC_BASE_PATH,
     * Config.HOODIE_SRC_PARTITION_FIELDS)); List<String> partitionFields =
     * props.getStringList(Config.HOODIE_SRC_PARTITION_FIELDS, ",", new ArrayList<>()); PartitionValueExtractor
     * extractor = DataSourceUtils.createPartitionExtractor(props.getString( Config.HOODIE_SRC_PARTITION_EXTRACTORCLASS,
     * Config.DEFAULT_HOODIE_SRC_PARTITION_EXTRACTORCLASS));
     */
    String srcPath = props.getString(Config.HOODIE_SRC_BASE_PATH);
    int numInstantsPerFetch = props.getInteger(Config.NUM_INSTANTS_PER_FETCH, Config.DEFAULT_NUM_INSTANTS_PER_FETCH);
    boolean readLatestOnMissingCkpt = props.getBoolean(Config.READ_LATEST_INSTANT_ON_MISSING_CKPT,
        Config.DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT);
    IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy = (props.containsKey(Config.MISSING_CHECKPOINT_STRATEGY))
        ? IncrSourceHelper.MissingCheckpointStrategy.valueOf(props.getString(Config.MISSING_CHECKPOINT_STRATEGY)) : null;
    if (readLatestOnMissingCkpt) {
      missingCheckpointStrategy = IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST;
    }

    // Use begin Instant if set and non-empty
    Option<String> beginInstant =
        lastCkptStr.isPresent() ? lastCkptStr.get().isEmpty() ? Option.empty() : lastCkptStr : Option.empty();

    Pair<String, Pair<String, String>> queryTypeAndInstantEndpts = IncrSourceHelper.calculateBeginAndEndInstants(sparkContext, srcPath,
        numInstantsPerFetch, beginInstant, missingCheckpointStrategy);

    if (queryTypeAndInstantEndpts.getValue().getKey().equals(queryTypeAndInstantEndpts.getValue().getValue())) {
      LOG.warn("Already caught up. Begin Checkpoint was :" + queryTypeAndInstantEndpts.getValue().getKey());
      return Pair.of(Option.empty(), queryTypeAndInstantEndpts.getValue().getKey());
    }

    Dataset<Row> source = null;
    // Do Incr pull. Set end instant if available
    if (queryTypeAndInstantEndpts.getKey().equals(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())) {
      source = sparkSession.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL())
          .option(DataSourceReadOptions.BEGIN_INSTANTTIME().key(), queryTypeAndInstantEndpts.getValue().getLeft())
          .option(DataSourceReadOptions.END_INSTANTTIME().key(), queryTypeAndInstantEndpts.getValue().getRight())
          .option(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES().key(),
              props.getString(DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES().key(),
                  DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES().defaultValue()))
          .load(srcPath);
    } else {
      // if checkpoint is missing from source table, and if strategy is set to READ_UPTO_LATEST_COMMIT, we have to issue snapshot query
      source = sparkSession.read().format("org.apache.hudi")
          .option(DataSourceReadOptions.QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
          .load(srcPath)
          // add filtering so that only interested records are returned.
          .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
              queryTypeAndInstantEndpts.getRight().getLeft()));
    }

    /*
     * log.info("Partition Fields are : (" + partitionFields + "). Initial Source Schema :" + source.schema());
     *
     * StructType newSchema = new StructType(source.schema().fields()); for (String field : partitionFields) { newSchema
     * = newSchema.add(field, DataTypes.StringType, true); }
     *
     * /** Validates if the commit time is sane and also generates Partition fields from _hoodie_partition_path if
     * configured
     *
     * Dataset<Row> validated = source.map((MapFunction<Row, Row>) (Row row) -> { // _hoodie_instant_time String
     * instantTime = row.getString(0); IncrSourceHelper.validateInstantTime(row, instantTime, instantEndpts.getKey(),
     * instantEndpts.getValue()); if (!partitionFields.isEmpty()) { // _hoodie_partition_path String hoodiePartitionPath
     * = row.getString(3); List<Object> partitionVals =
     * extractor.extractPartitionValuesInPath(hoodiePartitionPath).stream() .map(o -> (Object)
     * o).collect(Collectors.toList()); ValidationUtils.checkArgument(partitionVals.size() == partitionFields.size(),
     * "#partition-fields != #partition-values-extracted"); List<Object> rowObjs = new
     * ArrayList<>(scala.collection.JavaConversions.seqAsJavaList(row.toSeq())); rowObjs.addAll(partitionVals); return
     * RowFactory.create(rowObjs.toArray()); } return row; }, RowEncoder.apply(newSchema));
     *
     * log.info("Validated Source Schema :" + validated.schema());
     */

    // Remove Hoodie meta columns except partition path from input source
    final Dataset<Row> src = source.drop(HoodieRecord.HOODIE_META_COLUMNS.stream()
        .filter(x -> !x.equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toArray(String[]::new));
    // log.info("Final Schema from Source is :" + src.schema());
    return Pair.of(Option.of(src), queryTypeAndInstantEndpts.getRight().getRight());
  }
}
