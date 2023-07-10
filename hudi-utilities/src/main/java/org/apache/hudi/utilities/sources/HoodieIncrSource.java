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
import org.apache.hudi.common.table.timeline.TimelineUtils.HollowCommitHandling;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.HoodieIncrSourceConfig;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.IncrSourceHelper;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.apache.hudi.DataSourceReadOptions.BEGIN_INSTANTTIME;
import static org.apache.hudi.DataSourceReadOptions.END_INSTANTTIME;
import static org.apache.hudi.DataSourceReadOptions.INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES;
import static org.apache.hudi.DataSourceReadOptions.INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE;
import static org.apache.hudi.DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL;
import static org.apache.hudi.utilities.UtilHelpers.createRecordMerger;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.calculateBeginAndEndInstants;
import static org.apache.hudi.utilities.sources.helpers.IncrSourceHelper.getHollowCommitHandleMode;

public class HoodieIncrSource extends RowSource {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieIncrSource.class);

  public static class Config {

    /**
     * {@link #HOODIE_SRC_BASE_PATH} is the base-path for the source Hoodie table.
     */
    @Deprecated
    static final String HOODIE_SRC_BASE_PATH = HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key();

    /**
     * {@link #NUM_INSTANTS_PER_FETCH} allows the max number of instants whose changes can be incrementally fetched.
     */
    @Deprecated
    static final String NUM_INSTANTS_PER_FETCH = HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key();
    @Deprecated
    static final Integer DEFAULT_NUM_INSTANTS_PER_FETCH = HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.defaultValue();

    /**
     * {@link #HOODIE_SRC_PARTITION_FIELDS} specifies partition fields that needs to be added to source table after
     * parsing _hoodie_partition_path.
     */
    @Deprecated
    static final String HOODIE_SRC_PARTITION_FIELDS = HoodieIncrSourceConfig.HOODIE_SRC_PARTITION_FIELDS.key();

    /**
     * {@link #HOODIE_SRC_PARTITION_EXTRACTORCLASS} PartitionValueExtractor class to extract partition fields from
     * _hoodie_partition_path.
     */
    @Deprecated
    static final String HOODIE_SRC_PARTITION_EXTRACTORCLASS = HoodieIncrSourceConfig.HOODIE_SRC_PARTITION_EXTRACTORCLASS.key();
    @Deprecated
    static final String DEFAULT_HOODIE_SRC_PARTITION_EXTRACTORCLASS =
        HoodieIncrSourceConfig.HOODIE_SRC_PARTITION_EXTRACTORCLASS.defaultValue();

    /**
     * {@link  #READ_LATEST_INSTANT_ON_MISSING_CKPT} allows Hudi Streamer to incrementally fetch from latest committed
     * instant when checkpoint is not provided. This config is deprecated. Please refer to {@link #MISSING_CHECKPOINT_STRATEGY}.
     */
    @Deprecated
    public static final String READ_LATEST_INSTANT_ON_MISSING_CKPT = HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT.key();
    @Deprecated
    public static final Boolean DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT =
        HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT.defaultValue();

    /**
     * {@link  #MISSING_CHECKPOINT_STRATEGY} allows Hudi Streamer to decide the checkpoint to consume from when checkpoint is not set.
     * instant when checkpoint is not provided.
     */
    @Deprecated
    public static final String MISSING_CHECKPOINT_STRATEGY = HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY.key();

    /**
     * {@link  #SOURCE_FILE_FORMAT} is passed to the reader while loading dataset. Default value is parquet.
     */
    @Deprecated
    static final String SOURCE_FILE_FORMAT = HoodieIncrSourceConfig.SOURCE_FILE_FORMAT.key();
    @Deprecated
    static final String DEFAULT_SOURCE_FILE_FORMAT = HoodieIncrSourceConfig.SOURCE_FILE_FORMAT.defaultValue();

    /**
     * Drops all meta fields from the source hudi table while ingesting into sink hudi table.
     */
    @Deprecated
    static final String HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE = HoodieIncrSourceConfig.HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE.key();
    @Deprecated
    public static final Boolean DEFAULT_HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE =
        HoodieIncrSourceConfig.HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE.defaultValue();
  }

  public HoodieIncrSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          SchemaProvider schemaProvider) {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  @Override
  public Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {

    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key()));

    /*
     * DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.HOODIE_SRC_BASE_PATH,
     * Config.HOODIE_SRC_PARTITION_FIELDS)); List<String> partitionFields =
     * props.getStringList(Config.HOODIE_SRC_PARTITION_FIELDS, ",", new ArrayList<>()); PartitionValueExtractor
     * extractor = DataSourceUtils.createPartitionExtractor(props.getString( Config.HOODIE_SRC_PARTITION_EXTRACTORCLASS,
     * Config.DEFAULT_HOODIE_SRC_PARTITION_EXTRACTORCLASS));
     */
    String srcPath = props.getString(HoodieIncrSourceConfig.HOODIE_SRC_BASE_PATH.key());
    int numInstantsPerFetch = props.getInteger(HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.key(),
        HoodieIncrSourceConfig.NUM_INSTANTS_PER_FETCH.defaultValue());
    boolean readLatestOnMissingCkpt = props.getBoolean(HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT.key(),
        HoodieIncrSourceConfig.READ_LATEST_INSTANT_ON_MISSING_CKPT.defaultValue());
    IncrSourceHelper.MissingCheckpointStrategy missingCheckpointStrategy =
        (props.containsKey(HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY.key()))
            ? IncrSourceHelper.MissingCheckpointStrategy.valueOf(props.getString(HoodieIncrSourceConfig.MISSING_CHECKPOINT_STRATEGY.key())) : null;
    if (readLatestOnMissingCkpt) {
      missingCheckpointStrategy = IncrSourceHelper.MissingCheckpointStrategy.READ_LATEST;
    }

    // Use begin Instant if set and non-empty
    Option<String> beginInstant =
        lastCkptStr.isPresent() ? lastCkptStr.get().isEmpty() ? Option.empty() : lastCkptStr : Option.empty();

    HollowCommitHandling handlingMode = getHollowCommitHandleMode(props);
    Pair<String, Pair<String, String>> queryTypeAndInstantEndpts = calculateBeginAndEndInstants(sparkContext, srcPath,
        numInstantsPerFetch, beginInstant, missingCheckpointStrategy, handlingMode);

    if (queryTypeAndInstantEndpts.getValue().getKey().equals(queryTypeAndInstantEndpts.getValue().getValue())) {
      LOG.warn("Already caught up. Begin Checkpoint was :" + queryTypeAndInstantEndpts.getValue().getKey());
      return Pair.of(Option.empty(), queryTypeAndInstantEndpts.getValue().getKey());
    }

    Dataset<Row> source;
    // Do Incr pull. Set end instant if available
    if (queryTypeAndInstantEndpts.getKey().equals(QUERY_TYPE_INCREMENTAL_OPT_VAL())) {
      source = sparkSession.read().format("org.apache.hudi")
          .option(QUERY_TYPE().key(), QUERY_TYPE_INCREMENTAL_OPT_VAL())
          .option(BEGIN_INSTANTTIME().key(), queryTypeAndInstantEndpts.getValue().getLeft())
          .option(END_INSTANTTIME().key(), queryTypeAndInstantEndpts.getValue().getRight())
          .option(INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES().key(),
              props.getString(INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES().key(),
                  INCREMENTAL_FALLBACK_TO_FULL_TABLE_SCAN_FOR_NON_EXISTING_FILES().defaultValue()))
          .option(INCREMENTAL_READ_HANDLE_HOLLOW_COMMIT().key(), handlingMode.name())
          .load(srcPath);
    } else {
      // if checkpoint is missing from source table, and if strategy is set to READ_UPTO_LATEST_COMMIT, we have to issue snapshot query
      source = sparkSession.read().format("org.apache.hudi")
          .option(QUERY_TYPE().key(), DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL())
          .load(srcPath)
          // add filtering so that only interested records are returned.
          .filter(String.format("%s > '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
              queryTypeAndInstantEndpts.getRight().getLeft()))
          .filter(String.format("%s <= '%s'", HoodieRecord.COMMIT_TIME_METADATA_FIELD,
              queryTypeAndInstantEndpts.getRight().getRight()));
    }

    HoodieRecord.HoodieRecordType recordType = createRecordMerger(props).getRecordType();

    boolean shouldDropMetaFields = props.getBoolean(HoodieIncrSourceConfig.HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE.key(),
        HoodieIncrSourceConfig.HOODIE_DROP_ALL_META_FIELDS_FROM_SOURCE.defaultValue())
        // NOTE: In case when Spark native [[RecordMerger]] is used, we have to make sure
        //       all meta-fields have been properly cleaned up from the incoming dataset
        //
        || recordType == HoodieRecord.HoodieRecordType.SPARK;

    // Remove Hoodie meta columns except partition path from input source
    String[] colsToDrop = shouldDropMetaFields ? HoodieRecord.HOODIE_META_COLUMNS.stream().toArray(String[]::new) :
        HoodieRecord.HOODIE_META_COLUMNS.stream().filter(x -> !x.equals(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toArray(String[]::new);
    final Dataset<Row> src = source.drop(colsToDrop);
    return Pair.of(Option.of(src), queryTypeAndInstantEndpts.getRight().getRight());
  }
}
