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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.ingestion.HoodieIngestionMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.CloudDataFetcher;
import org.apache.hudi.utilities.sources.helpers.QueryRunner;
import org.apache.hudi.utilities.streamer.DefaultStreamContext;
import org.apache.hudi.utilities.streamer.StreamContext;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MockS3EventsHoodieIncrSource extends S3EventsHoodieIncrSource {

  public static final String OP_FETCH_NEXT_BATCH = "mockTestFetchNextBatchOp";
  public static final String VAL_INPUT_CKP = "valInputCkp";

  public static final String OP_EMPTY_ROW_SET_NONE_NULL_CKP1_KEY = "OP_EMPTY_ROW_SET_NONE_NULL_CKP1_KEY";
  public static final String CUSTOM_CHECKPOINT1 = "custom-checkpoint1";

  private static final BiFunction<Option<Checkpoint>, Long, Pair<Option<Dataset<Row>>, Checkpoint>> EMPTY_ROW_SET_NONE_NULL_CKP_1 =
      (checkpoint, limit) -> {
        Option<Dataset<Row>> empty = Option.empty();
        return Pair.of(
          empty,
          new StreamerCheckpointV1(CUSTOM_CHECKPOINT1));
      };

  public static final String OP_EMPTY_ROW_SET_NONE_NULL_CKP2_KEY = "OP_EMPTY_ROW_SET_NONE_NULL_CKP2_KEY";
  public static final String CUSTOM_CHECKPOINT2 = "custom-checkpoint2";

  private static final BiFunction<Option<Checkpoint>, Long, Pair<Option<Dataset<Row>>, Checkpoint>> EMPTY_ROW_SET_NONE_NULL_CKP_2 =
      (checkpoint, limit) -> {
        Option<Dataset<Row>> empty = Option.empty();
        return Pair.of(
            empty,
            new StreamerCheckpointV1(CUSTOM_CHECKPOINT2)
        );
      };

  public static final String OP_EMPTY_ROW_SET_NULL_CKP_KEY = "OP_EMPTY_ROW_SET_NULL_CKP";
  private static final BiFunction<Option<Checkpoint>, Long, Pair<Option<Dataset<Row>>, Checkpoint>> EMPTY_ROW_SET_NULL_CKP =
      (checkpoint, limit) -> {
        Option<Dataset<Row>> empty = Option.empty();
        return Pair.of(
            empty,
            null
        );
      };

  public static final String VAL_CKP_KEY_EQ_VAL = "VAL_CKP_KEY";
  public static final String VAL_CKP_RESET_KEY_EQUALS = "VAL_CKP_RESET_KEY";
  public static final String VAL_CKP_RESET_KEY_IS_NULL = "VAL_CKP_RESET_KEY_IS_NULL";
  public static final String VAL_CKP_IGNORE_KEY_EQUALS = "VAL_CKP_IGNORE_KEY";
  public static final String VAL_CKP_IGNORE_KEY_IS_NULL = "VAL_CKP_IGNORE_KEY_IS_NULL";

  public static final String VAL_NON_EMPTY_CKP_ALL_MEMBERS = "VAL_NON_EMPTY_CKP_ALL_MEMBERS";
  private static final BiFunction<Option<Checkpoint>, TypedProperties, Void> VAL_NON_EMPTY_CKP_WITH_FIXED_VALUE = (ckpOpt, props) -> {
    assertFalse(ckpOpt.isEmpty());
    if (props.containsKey(VAL_CKP_KEY_EQ_VAL)) {
      assertEquals(props.getString(VAL_CKP_KEY_EQ_VAL), ckpOpt.get().getCheckpointKey());
    }

    if (props.containsKey(VAL_CKP_RESET_KEY_EQUALS)) {
      assertEquals(ckpOpt.get().getCheckpointResetKey(), props.getString(VAL_CKP_RESET_KEY_EQUALS));
    }
    if (props.containsKey(VAL_CKP_RESET_KEY_IS_NULL)) {
      assertNull(ckpOpt.get().getCheckpointResetKey());
    }

    if (props.containsKey(VAL_CKP_IGNORE_KEY_EQUALS)) {
      assertEquals(ckpOpt.get().getCheckpointIgnoreKey(), props.getString(VAL_CKP_IGNORE_KEY_EQUALS));
    }
    if (props.containsKey(VAL_CKP_IGNORE_KEY_IS_NULL)) {
      assertNull(ckpOpt.get().getCheckpointIgnoreKey());
    }
    return null;
  };

  public static final String VAL_EMPTY_CKP_KEY = "VAL_EMPTY_CKP_KEY";
  private static final BiFunction<Option<Checkpoint>, TypedProperties, Void> VAL_EMPTY_CKP = (ckpOpt, props) -> {
    assertTrue(ckpOpt.isEmpty());
    return null;
  };

  public static final String VAL_NO_OP = "VAL_NO_OP";

  public MockS3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      SchemaProvider schemaProvider,
      HoodieIngestionMetrics metrics) {
    this(props, sparkContext, sparkSession, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, sparkContext, sparkSession, metrics), new DefaultStreamContext(schemaProvider, Option.empty()));
  }

  public MockS3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      HoodieIngestionMetrics metrics,
      StreamContext streamContext) {
    this(props, sparkContext, sparkSession, new QueryRunner(sparkSession, props),
        new CloudDataFetcher(props, sparkContext, sparkSession, metrics), streamContext);
  }

  MockS3EventsHoodieIncrSource(
      TypedProperties props,
      JavaSparkContext sparkContext,
      SparkSession sparkSession,
      QueryRunner queryRunner,
      CloudDataFetcher cloudDataFetcher,
      StreamContext streamContext) {
    super(props, sparkContext, sparkSession, queryRunner, cloudDataFetcher, streamContext);
  }

  @Override
  public Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit) {
    String valType = (String) props.getOrDefault(VAL_INPUT_CKP, VAL_NO_OP);
    validateCheckpointOption(lastCheckpoint, valType);

    String opType = (String) props.getOrDefault(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP1_KEY);
    return applyDummyOpOfFetchNextBatch(lastCheckpoint, sourceLimit, opType);
  }

  private Pair<Option<Dataset<Row>>, Checkpoint> applyDummyOpOfFetchNextBatch(Option<Checkpoint> lastCheckpoint, long sourceLimit, String opType) {
    if (opType.equals(OP_EMPTY_ROW_SET_NONE_NULL_CKP1_KEY)) {
      return EMPTY_ROW_SET_NONE_NULL_CKP_1.apply(lastCheckpoint, sourceLimit);
    }
    if (opType.equals(OP_EMPTY_ROW_SET_NONE_NULL_CKP2_KEY)) {
      return EMPTY_ROW_SET_NONE_NULL_CKP_2.apply(lastCheckpoint, sourceLimit);
    }
    if (opType.equals(OP_EMPTY_ROW_SET_NULL_CKP_KEY)) {
      return EMPTY_ROW_SET_NULL_CKP.apply(lastCheckpoint, sourceLimit);
    }
    throw new IllegalArgumentException("Unsupported operation type: " + opType);
  }

  private void validateCheckpointOption(Option<Checkpoint> lastCheckpoint, String valType) {
    if (!lastCheckpoint.isEmpty()) {
      assertInstanceOf(StreamerCheckpointV1.class, lastCheckpoint.get());
    }
    if (valType.equals(VAL_NO_OP)) {
      return;
    }
    if (valType.equals(VAL_NON_EMPTY_CKP_ALL_MEMBERS)) {
      VAL_NON_EMPTY_CKP_WITH_FIXED_VALUE.apply(lastCheckpoint, props);
    }
    if (valType.equals(VAL_EMPTY_CKP_KEY)) {
      VAL_EMPTY_CKP.apply(lastCheckpoint, props);
    }
  }
}
