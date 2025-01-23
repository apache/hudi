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
  /**
   * Validation keys for checkpoint testing.
   */
  public static final String VAL_CKP_KEY_EQ_VAL = "VAL_CKP_KEY";
  public static final String VAL_CKP_RESET_KEY_EQUALS = "VAL_CKP_RESET_KEY";
  public static final String VAL_CKP_RESET_KEY_IS_NULL = "VAL_CKP_RESET_KEY_IS_NULL";
  public static final String VAL_CKP_IGNORE_KEY_EQUALS = "VAL_CKP_IGNORE_KEY";
  public static final String VAL_CKP_IGNORE_KEY_IS_NULL = "VAL_CKP_IGNORE_KEY_IS_NULL";
  public static final String VAL_NON_EMPTY_CKP_ALL_MEMBERS = "VAL_NON_EMPTY_CKP_ALL_MEMBERS";
  public static final String VAL_EMPTY_CKP_KEY = "VAL_EMPTY_CKP_KEY";
  public static final String VAL_NO_OP = "VAL_NO_OP";

  /*
  * Checkpoint validation method that assert value equals to expected ones.
  * */
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

  private static final BiFunction<Option<Checkpoint>, TypedProperties, Void> VAL_EMPTY_CKP = (ckpOpt, props) -> {
    assertTrue(ckpOpt.isEmpty());
    return null;
  };

  /**
   * Validates the checkpoint option based on the validation type specified in properties.
   *
   * @param lastCheckpoint Option containing the checkpoint to validate
   * @param valType validation type to perform
   * @param props TypedProperties containing validation configuration
   */
  public static void validateCheckpointOption(Option<Checkpoint> lastCheckpoint, String valType, TypedProperties props) {
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