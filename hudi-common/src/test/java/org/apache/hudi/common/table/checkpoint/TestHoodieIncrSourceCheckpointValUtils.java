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

package org.apache.hudi.common.table.checkpoint;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieIncrSourceCheckpointValUtils {

  @Test
  public void testResolveToV1V2CheckpointWithRequestTime() {
    String checkpoint = "20240301";
    UnresolvedStreamerCheckpointBasedOnCfg mockCheckpoint = mock(UnresolvedStreamerCheckpointBasedOnCfg.class);
    when(mockCheckpoint.getCheckpointKey()).thenReturn("resumeFromInstantRequestTime:" + checkpoint);

    Checkpoint result = HoodieIncrSourceCheckpointValUtils.resolveToActualCheckpointVersion(mockCheckpoint);

    assertInstanceOf(StreamerCheckpointV1.class, result);
    assertEquals(checkpoint, result.getCheckpointKey());
  }

  @Test
  public void testResolveToV1V2CheckpointWithCompletionTime() {
    String checkpoint = "20240302";
    UnresolvedStreamerCheckpointBasedOnCfg mockCheckpoint = mock(UnresolvedStreamerCheckpointBasedOnCfg.class);
    when(mockCheckpoint.getCheckpointKey()).thenReturn("resumeFromInstantCompletionTime:" + checkpoint);

    Checkpoint result = HoodieIncrSourceCheckpointValUtils.resolveToActualCheckpointVersion(mockCheckpoint);

    assertInstanceOf(StreamerCheckpointV2.class, result);
    assertEquals(checkpoint, result.getCheckpointKey());
  }

  @Test
  public void testResolveToV1V2CheckpointWithInvalidPrefix() {
    UnresolvedStreamerCheckpointBasedOnCfg mockCheckpoint = mock(UnresolvedStreamerCheckpointBasedOnCfg.class);
    when(mockCheckpoint.getCheckpointKey()).thenReturn("invalidPrefix:20240303");

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> HoodieIncrSourceCheckpointValUtils.resolveToActualCheckpointVersion(mockCheckpoint)
    );
    assertTrue(exception.getMessage().contains("Illegal checkpoint key override"));
  }

  @Test
  public void testResolveToV1V2CheckpointWithMalformedInput() {
    UnresolvedStreamerCheckpointBasedOnCfg mockCheckpoint = mock(UnresolvedStreamerCheckpointBasedOnCfg.class);
    when(mockCheckpoint.getCheckpointKey()).thenReturn("malformedInput");

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> HoodieIncrSourceCheckpointValUtils.resolveToActualCheckpointVersion(mockCheckpoint)
    );
    assertTrue(exception.getMessage().contains("Illegal checkpoint key override"));
  }
} 