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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHoodieIncrSourceCheckpointValUtils {

  @Test
  public void testResolveToV1V2CheckpointWithRequestTime() {
    StreamerCheckpointFromCfgCkp mockCheckpoint = mock(StreamerCheckpointFromCfgCkp.class);
    when(mockCheckpoint.getCheckpointKey()).thenReturn("resumeFromInstantRequestTime:20240301");

    Checkpoint result = HoodieIncrSourceCheckpointValUtils.resolveToV1V2Checkpoint(mockCheckpoint);

    assertTrue(result instanceof StreamerCheckpointV1);
    assertEquals("20240301", result.getCheckpointKey());
  }

  @Test
  public void testResolveToV1V2CheckpointWithCompletionTime() {
    StreamerCheckpointFromCfgCkp mockCheckpoint = mock(StreamerCheckpointFromCfgCkp.class);
    when(mockCheckpoint.getCheckpointKey()).thenReturn("resumeFromInstantCompletionTime:20240302");

    Checkpoint result = HoodieIncrSourceCheckpointValUtils.resolveToV1V2Checkpoint(mockCheckpoint);

    assertTrue(result instanceof StreamerCheckpointV2);
    assertEquals("20240302", result.getCheckpointKey());
  }

  @Test
  public void testResolveToV1V2CheckpointWithInvalidPrefix() {
    StreamerCheckpointFromCfgCkp mockCheckpoint = mock(StreamerCheckpointFromCfgCkp.class);
    when(mockCheckpoint.getCheckpointKey()).thenReturn("invalidPrefix:20240303");

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> HoodieIncrSourceCheckpointValUtils.resolveToV1V2Checkpoint(mockCheckpoint)
    );
    assertTrue(exception.getMessage().contains("Illegal checkpoint key override"));
  }

  @Test
  public void testResolveToV1V2CheckpointWithMalformedInput() {
    StreamerCheckpointFromCfgCkp mockCheckpoint = mock(StreamerCheckpointFromCfgCkp.class);
    when(mockCheckpoint.getCheckpointKey()).thenReturn("malformedInput");

    IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> HoodieIncrSourceCheckpointValUtils.resolveToV1V2Checkpoint(mockCheckpoint)
    );
    assertTrue(exception.getMessage().contains("Illegal checkpoint key override"));
  }

  @Test
  public void testCheckpointWithModeToString() {
    HoodieIncrSourceCheckpointValUtils.CheckpointWithMode checkpointWithMode =
        new HoodieIncrSourceCheckpointValUtils.CheckpointWithMode("REQUEST_TIME", "20240304");

    String result = checkpointWithMode.toString();

    assertTrue(result.contains("REQUEST_TIME"));
    assertTrue(result.contains("20240304"));
    assertEquals("REQUEST_TIME", checkpointWithMode.getEventOrderingMode());
    assertEquals("20240304", checkpointWithMode.getCheckpointKey());
  }
} 