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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.CommitUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Which rounds' inflight the streamer marks as pending a workload profile. */
class TestStreamSyncInflightMarker {

  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"INSERT", "UPSERT", "DELETE", "BULK_INSERT", "INSERT_OVERWRITE",
      "INSERT_OVERWRITE_TABLE", "DELETE_PARTITION"})
  void onlyCommitsAndDeltaCommitsThatSaveAProfileAreMarked(WriteOperationType operation) {
    for (HoodieTableType tableType : HoodieTableType.values()) {
      String action = CommitUtils.getCommitActionType(operation, tableType);
      boolean expected = operation == WriteOperationType.INSERT || operation == WriteOperationType.UPSERT
          || operation == WriteOperationType.DELETE;
      assertEquals(expected, StreamSync.marksInflightPendingProfile(action, operation), operation + " on " + tableType);
    }
  }
}
