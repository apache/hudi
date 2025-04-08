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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Helper class to execute dummy operations for testing S3EventsHoodieIncrSource.
 * Provides different scenarios of empty row sets with various checkpoint configurations.
 */
public class DummyOperationExecutor {

  public static final String OP_FETCH_NEXT_BATCH = "mockTestFetchNextBatchOp";

  /**
   * Operation keys for different test scenarios returning empty row sets with different checkpoint configurations.
   * For corresponding dummy operations please extract the lambda function name from the key OP_[DummyOperationFunctionName]_KEY
   * and check the corresponding [DummyOperationFunctionName] member definition.
   */
  public static final String OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY = "OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY";
  public static final String OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY = "OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY";
  public static final String OP_EMPTY_ROW_SET_NULL_CKP_KEY = "OP_EMPTY_ROW_SET_NULL_CKP";
  
  /**
   * Custom checkpoint values used in testing.
   */
  public static final String CUSTOM_CHECKPOINT1 = "custom-checkpoint1";
  public static final String RETURN_CHECKPOINT_KEY = "RETURN_CHECKPOINT_KEY";

  @FunctionalInterface
  private interface OperationFunction {
    Pair<Option<Dataset<Row>>, Checkpoint> apply(Option<Checkpoint> checkpoint, Long limit, TypedProperties props);
  }

  private static final OperationFunction EMPTY_ROW_SET_NONE_NULL_CKP_V1 =
      (checkpoint, limit, props) -> {
        Option<Dataset<Row>> empty = Option.empty();
        String returnCheckpoint = props.getString(RETURN_CHECKPOINT_KEY, CUSTOM_CHECKPOINT1);
        return Pair.of(empty, new StreamerCheckpointV1(returnCheckpoint));
      };

  private static final OperationFunction EMPTY_ROW_SET_NULL_CKP =
      (checkpoint, limit, props) -> {
        Option<Dataset<Row>> empty = Option.empty();
        return Pair.of(empty, null);
      };

  private static final OperationFunction EMPTY_ROW_SET_NONE_NULL_CKP_V2 =
      (checkpoint, limit, props) -> {
        Option<Dataset<Row>> empty = Option.empty();
        String returnCheckpoint = props.getString(RETURN_CHECKPOINT_KEY, CUSTOM_CHECKPOINT1);
        return Pair.of(empty, new StreamerCheckpointV2(returnCheckpoint));
      };

  /**
   * Executes the dummy operation based on the operation type in props.
   *
   * @param lastCheckpoint Option containing the last checkpoint
   * @param sourceLimit maximum number of records to fetch
   * @param props TypedProperties containing the operation type
   * @return Pair containing Option<Dataset<Row>> and Checkpoint
   * @throws IllegalArgumentException if operation type is not supported
   */
  public static Pair<Option<Dataset<Row>>, Checkpoint> executeDummyOperation(
      Option<Checkpoint> lastCheckpoint, long sourceLimit, TypedProperties props) {
    String opType = props.getString(OP_FETCH_NEXT_BATCH, OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY);
    switch (opType) {
      case OP_EMPTY_ROW_SET_NONE_NULL_CKP_V1_KEY:
        return EMPTY_ROW_SET_NONE_NULL_CKP_V1.apply(lastCheckpoint, sourceLimit, props);
      case OP_EMPTY_ROW_SET_NONE_NULL_CKP_V2_KEY:
        return EMPTY_ROW_SET_NONE_NULL_CKP_V2.apply(lastCheckpoint, sourceLimit, props);
      case OP_EMPTY_ROW_SET_NULL_CKP_KEY:
        return EMPTY_ROW_SET_NULL_CKP.apply(lastCheckpoint, sourceLimit, props);
      default:
        throw new IllegalArgumentException("Unsupported operation type: " + opType);
    }
  }
} 