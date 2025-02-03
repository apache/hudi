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
import org.apache.hudi.common.table.checkpoint.UnresolvedStreamerCheckpointBasedOnCfg;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;

import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
/**
 * Helper class to validate checkpoint options in test scenarios.
 * Used by MockS3EventsHoodieIncrSource to validate checkpoint behavior.
 */
public class CheckpointValidator {
  public static final String VAL_INPUT_CKP = "valInputCkp";

  /**
   * Validation keys for checkpoint testing.
   */
  public static final String VAL_CKP_INSTANCE_OF = "VAL_CKP_INSTANCE_OF_KEY";
  public static final String VAL_CKP_KEY_EQ_VAL = "VAL_CKP_KEY_EQ_VAL_KEY";
  public static final String VAL_CKP_RESET_KEY_EQUALS = "VAL_CKP_RESET_KEY";
  public static final String VAL_CKP_RESET_KEY_IS_NULL = "VAL_CKP_RESET_KEY_IS_NULL";
  public static final String VAL_CKP_IGNORE_KEY_EQUALS = "VAL_CKP_IGNORE_KEY";
  public static final String VAL_CKP_IGNORE_KEY_IS_NULL = "VAL_CKP_IGNORE_KEY_IS_NULL";
  public static final String VAL_NON_EMPTY_CKP_ALL_MEMBERS = "VAL_NON_EMPTY_CKP_ALL_MEMBERS";
  public static final String VAL_EMPTY_CKP_KEY = "VAL_EMPTY_CKP_KEY";
  public static final String VAL_NO_INGESTION_HAPPENS = "VAL_NO_INGESTION_HAPPENS_KEY";
  public static final String VAL_NO_OP = "VAL_NO_OP";

  /*
   * Checkpoint validation method that assert value equals to expected ones.
   * */
  private static final BiFunction<Option<Checkpoint>, TypedProperties, Void> VAL_NON_EMPTY_CKP_WITH_FIXED_VALUE = (ckpOpt, props) -> {
    assertFalse(ckpOpt.isEmpty());
    Checkpoint ckp = ckpOpt.get();
    assertCheckpointIsInstanceOf(props, ckp);
    assertCheckpointValueEqualsTo(props, ckp);
    return null;
  };

  private static void assertCheckpointValueEqualsTo(TypedProperties props, Checkpoint ckp) {
    if (props.containsKey(VAL_CKP_KEY_EQ_VAL)) {
      assertEquals(props.getString(VAL_CKP_KEY_EQ_VAL), ckp.getCheckpointKey());
    }

    if (props.containsKey(VAL_CKP_RESET_KEY_EQUALS)) {
      assertEquals(ckp.getCheckpointResetKey(), props.getString(VAL_CKP_RESET_KEY_EQUALS));
    }
    if (props.containsKey(VAL_CKP_RESET_KEY_IS_NULL)) {
      assertNull(ckp.getCheckpointResetKey());
    }

    if (props.containsKey(VAL_CKP_IGNORE_KEY_EQUALS)) {
      assertEquals(ckp.getCheckpointIgnoreKey(), props.getString(VAL_CKP_IGNORE_KEY_EQUALS));
    }
    if (props.containsKey(VAL_CKP_IGNORE_KEY_IS_NULL)) {
      assertNull(ckp.getCheckpointIgnoreKey());
    }
  }

  private static void assertCheckpointIsInstanceOf(TypedProperties props, Checkpoint ckp) {
    if (props.containsKey(VAL_CKP_INSTANCE_OF)) {
      switch ((String) props.get(VAL_CKP_INSTANCE_OF)) {
        case "org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1":
          assertInstanceOf(StreamerCheckpointV1.class, ckp);
          break;
        case "org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2":
          assertInstanceOf(StreamerCheckpointV2.class, ckp);
          break;
        case "org.apache.hudi.common.table.checkpoint.StreamerCheckpointFromCfgCkp":
          assertInstanceOf(UnresolvedStreamerCheckpointBasedOnCfg.class, ckp);
          break;
        default:
          throw new RuntimeException("Unknown checkpoint class to validate " + props.get(VAL_CKP_INSTANCE_OF));
      }
    }
  }

  private static final BiFunction<Option<Checkpoint>, TypedProperties, Void> VAL_EMPTY_CKP = (ckpOpt, props) -> {
    assertTrue(ckpOpt.isEmpty());
    return null;
  };

  /**
   * Validates the checkpoint option based on validation type specified in props.
   *
   * @param lastCheckpoint Option containing the last checkpoint to validate
   * @param props TypedProperties containing validation configuration
   * @throws IllegalArgumentException if validation fails
   */
  public static void validateCheckpointOption(Option<Checkpoint> lastCheckpoint, TypedProperties props) {
    String valType = props.getString(VAL_INPUT_CKP, VAL_NO_OP);

    switch (valType) {
      case VAL_NO_OP:
        break;
      case VAL_NON_EMPTY_CKP_ALL_MEMBERS:
        VAL_NON_EMPTY_CKP_WITH_FIXED_VALUE.apply(lastCheckpoint, props);
        break;
      case VAL_EMPTY_CKP_KEY:
        VAL_EMPTY_CKP.apply(lastCheckpoint, props);
        break;
      case VAL_NO_INGESTION_HAPPENS:
        throw new IllegalStateException(
            String.format("Expected no ingestion happening but the source is asked to resume from checkpoint %s with props %s", lastCheckpoint, props));
      default:
        throw new IllegalArgumentException("Unsupported validation type: " + valType);
    }
  }
}
