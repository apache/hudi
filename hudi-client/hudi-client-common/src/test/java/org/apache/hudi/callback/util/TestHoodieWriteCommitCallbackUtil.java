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

package org.apache.hudi.callback.util;

import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage.PrevFilePaths;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for
 * {@link HoodieWriteCommitCallbackUtil#resolvePrevFilePaths(List, BaseFileOnlyView)}, which
 * pre-resolves the previous base file (and bootstrap source, if any) for each updated file
 * group from a cached {@link BaseFileOnlyView}, so callback implementations receive the
 * read/write file pairing without rebuilding a file-system view.
 */
public class TestHoodieWriteCommitCallbackUtil {

  private static final String PARTITION = "2024/01/01";
  private static final String PREV_COMMIT = "001";

  private static HoodieWriteStat stat(String fileId, String partitionPath, String prevCommit) {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setPartitionPath(partitionPath);
    writeStat.setPrevCommit(prevCommit);
    return writeStat;
  }

  @Test
  public void resolvePrevFilePathsReturnsEmptyForNullInputs() {
    BaseFileOnlyView view = mock(BaseFileOnlyView.class);
    assertTrue(HoodieWriteCommitCallbackUtil.resolvePrevFilePaths(null, view).isEmpty(),
        "null stats must yield an empty map");
    assertTrue(HoodieWriteCommitCallbackUtil.resolvePrevFilePaths(
        Collections.singletonList(stat("f0", PARTITION, PREV_COMMIT)), null).isEmpty(),
        "null file-system view must yield an empty map");
  }

  @Test
  public void resolvePrevFilePathsSkipsStatsWithoutAPrevCommit() {
    BaseFileOnlyView view = mock(BaseFileOnlyView.class);
    List<HoodieWriteStat> inserts = Arrays.asList(
        stat("f-null", PARTITION, null),
        stat("f-empty", PARTITION, ""),
        stat("f-nullcommit", PARTITION, HoodieWriteStat.NULL_COMMIT));

    Map<String, PrevFilePaths> resolved =
        HoodieWriteCommitCallbackUtil.resolvePrevFilePaths(inserts, view);

    assertTrue(resolved.isEmpty(), "inserts (no prevCommit) must not resolve a prev base file");
    // The view must not even be consulted for inserts.
    verify(view, never()).getBaseFileOn(anyString(), anyString(), anyString());
  }

  @Test
  public void resolvePrevFilePathsResolvesUpdatePrevBaseFile() {
    BaseFileOnlyView view = mock(BaseFileOnlyView.class);
    HoodieBaseFile prevBase = new HoodieBaseFile("/tbl/" + PARTITION + "/f0_0-1-1_" + PREV_COMMIT + ".parquet");
    when(view.getBaseFileOn(PARTITION, PREV_COMMIT, "f0")).thenReturn(Option.of(prevBase));

    Map<String, PrevFilePaths> resolved = HoodieWriteCommitCallbackUtil.resolvePrevFilePaths(
        Collections.singletonList(stat("f0", PARTITION, PREV_COMMIT)), view);

    assertEquals(1, resolved.size());
    assertEquals(prevBase.getPath(), resolved.get("f0").getBaseFilePath());
    assertNull(resolved.get("f0").getBootstrapBaseFilePath(), "non-bootstrap update has no bootstrap path");
  }

  @Test
  public void resolvePrevFilePathsCapturesBootstrapBaseFile() {
    BaseFileOnlyView view = mock(BaseFileOnlyView.class);
    BaseFile bootstrap = new BaseFile("/bootstrap/source/f0.parquet");
    HoodieBaseFile prevBase = new HoodieBaseFile("/tbl/" + PARTITION + "/f0_0-1-1_" + PREV_COMMIT + ".parquet", bootstrap);
    when(view.getBaseFileOn(PARTITION, PREV_COMMIT, "f0")).thenReturn(Option.of(prevBase));

    Map<String, PrevFilePaths> resolved = HoodieWriteCommitCallbackUtil.resolvePrevFilePaths(
        Collections.singletonList(stat("f0", PARTITION, PREV_COMMIT)), view);

    assertEquals(prevBase.getPath(), resolved.get("f0").getBaseFilePath());
    assertEquals(bootstrap.getPath(), resolved.get("f0").getBootstrapBaseFilePath(),
        "bootstrap source path must be carried through for bootstrapped file groups");
  }

  @Test
  public void resolvePrevFilePathsSkipsWhenBaseFileAbsent() {
    BaseFileOnlyView view = mock(BaseFileOnlyView.class);
    when(view.getBaseFileOn(PARTITION, PREV_COMMIT, "f0")).thenReturn(Option.empty());

    Map<String, PrevFilePaths> resolved = HoodieWriteCommitCallbackUtil.resolvePrevFilePaths(
        Collections.singletonList(stat("f0", PARTITION, PREV_COMMIT)), view);

    assertTrue(resolved.isEmpty(), "a missing prev base file must be skipped, not mapped to null");
  }

  @Test
  public void resolvePrevFilePathsIsBestEffortOnViewFailure() {
    BaseFileOnlyView view = mock(BaseFileOnlyView.class);
    when(view.getBaseFileOn(PARTITION, PREV_COMMIT, "boom"))
        .thenThrow(new RuntimeException("stale or remote view error"));
    HoodieBaseFile prevBase = new HoodieBaseFile("/tbl/" + PARTITION + "/ok_0-1-1_" + PREV_COMMIT + ".parquet");
    when(view.getBaseFileOn(PARTITION, PREV_COMMIT, "ok")).thenReturn(Option.of(prevBase));

    Map<String, PrevFilePaths> resolved = HoodieWriteCommitCallbackUtil.resolvePrevFilePaths(
        Arrays.asList(stat("boom", PARTITION, PREV_COMMIT), stat("ok", PARTITION, PREV_COMMIT)), view);

    // The failing file group is dropped; resolution continues for the rest (must not fail the commit).
    assertFalse(resolved.containsKey("boom"));
    assertEquals(prevBase.getPath(), resolved.get("ok").getBaseFilePath());
  }
}
