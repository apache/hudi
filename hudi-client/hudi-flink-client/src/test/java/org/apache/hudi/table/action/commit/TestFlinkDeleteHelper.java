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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link FlinkDeleteHelper}. */
@SuppressWarnings({"rawtypes", "unchecked"})
class TestFlinkDeleteHelper {

  @Test
  void testDeduplicateKeysForGlobalAndPartitionedIndexes() {
    FlinkDeleteHelper helper = FlinkDeleteHelper.newInstance();
    assertSame(helper, FlinkDeleteHelper.newInstance());

    HoodieTable table = mock(HoodieTable.class);
    HoodieIndex index = mock(HoodieIndex.class);
    when(table.getIndex()).thenReturn(index);
    List<HoodieKey> keys = new ArrayList<>(Arrays.asList(
        new HoodieKey("id1", "p1"),
        new HoodieKey("id1", "p2"),
        new HoodieKey("id2", "p1"),
        new HoodieKey("id2", "p1")));

    when(index.isGlobal()).thenReturn(true);
    List<HoodieKey> globalResult = helper.deduplicateKeys(keys, table, 1);
    assertEquals(Arrays.asList("id1", "id2"), globalResult.stream()
        .map(HoodieKey::getRecordKey).collect(Collectors.toList()));
    assertEquals(Arrays.asList("p1", "p1"), globalResult.stream()
        .map(HoodieKey::getPartitionPath).collect(Collectors.toList()));
    assertNotSame(keys, globalResult);

    when(index.isGlobal()).thenReturn(false);
    List<HoodieKey> partitionedResult = helper.deduplicateKeys(keys, table, 1);
    assertSame(keys, partitionedResult);
    assertEquals(Arrays.asList(
        new HoodieKey("id1", "p1"),
        new HoodieKey("id1", "p2"),
        new HoodieKey("id2", "p1")), partitionedResult);
  }

  @Test
  void testExecuteTagsExistingRecordsAndDelegatesDelete() {
    HoodieTable table = mock(HoodieTable.class);
    HoodieIndex index = mock(HoodieIndex.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    BaseCommitActionExecutor executor = mock(BaseCommitActionExecutor.class);
    when(table.getIndex()).thenReturn(index);
    when(index.isGlobal()).thenReturn(false);

    when(index.tagLocation(any(HoodieData.class), eq(context), eq(table))).thenAnswer(invocation -> {
      HoodieData<HoodieRecord<EmptyHoodieRecordPayload>> records = invocation.getArgument(0);
      List<HoodieRecord<EmptyHoodieRecordPayload>> tagged = records.collectAsList();
      tagged.get(0).setCurrentLocation(new HoodieRecordLocation("001", "file-1"));
      return HoodieListData.eager(tagged);
    });

    HoodieWriteMetadata<List<WriteStatus>> expected = new HoodieWriteMetadata<>();
    expected.setWriteStatuses(Collections.singletonList(new WriteStatus(false, 0.0)));
    when(executor.execute(any(List.class))).thenReturn(expected);

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/flink-delete-helper")
        .combineDeleteInput(true)
        .build();
    List<HoodieKey> keys = new ArrayList<>(Arrays.asList(
        new HoodieKey("id1", "p1"), new HoodieKey("id1", "p1"), new HoodieKey("missing", "p1")));

    HoodieWriteMetadata<List<WriteStatus>> result = FlinkDeleteHelper.newInstance()
        .execute("002", keys, context, config, table, executor);

    assertSame(expected, result);
    assertTrue(result.getIndexLookupDuration().isPresent());
    verify(executor).execute(any(List.class));
    verify(executor, never()).saveWorkloadProfileMetadataToInflight(any(), any());
  }

  @Test
  void testExecuteWithNoExistingRecordsCreatesEmptyMetadata() {
    HoodieTable table = mock(HoodieTable.class);
    HoodieIndex index = mock(HoodieIndex.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    BaseCommitActionExecutor executor = mock(BaseCommitActionExecutor.class);
    when(table.getIndex()).thenReturn(index);
    when(index.tagLocation(any(HoodieData.class), eq(context), eq(table)))
        .thenAnswer(invocation -> invocation.getArgument(0));

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/flink-delete-helper")
        .combineDeleteInput(false)
        .build();
    HoodieWriteMetadata<List<WriteStatus>> result = FlinkDeleteHelper.newInstance().execute(
        "003", Collections.singletonList(new HoodieKey("missing", "p1")),
        context, config, table, executor);

    assertTrue(result.getWriteStatuses().isEmpty());
    verify(executor).saveWorkloadProfileMetadataToInflight(any(), eq("003"));
    verify(executor).runPrecommitValidators(result);
    verify(executor, never()).execute(any(List.class));
  }

  @Test
  void testExecutePreservesOrWrapsFailures() {
    HoodieTable table = mock(HoodieTable.class);
    HoodieEngineContext context = mock(HoodieEngineContext.class);
    BaseCommitActionExecutor executor = mock(BaseCommitActionExecutor.class);
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/flink-delete-helper")
        .build();
    List<HoodieKey> keys = Collections.singletonList(new HoodieKey("id1", "p1"));

    HoodieUpsertException upsertException = new HoodieUpsertException("expected");
    when(table.getIndex()).thenThrow(upsertException);
    HoodieUpsertException firstFailure = assertThrows(HoodieUpsertException.class,
        () -> FlinkDeleteHelper.newInstance().execute("004", keys, context, config, table, executor));
    assertSame(upsertException, firstFailure);

    reset(table);
    when(table.getIndex()).thenThrow(new IllegalStateException("boom"));
    HoodieUpsertException wrapped = assertThrows(HoodieUpsertException.class,
        () -> FlinkDeleteHelper.newInstance().execute("005", keys, context, config, table, executor));
    assertTrue(wrapped.getCause() instanceof IllegalStateException);
  }
}
