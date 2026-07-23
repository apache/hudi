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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link JavaDeleteHelper}. */
@SuppressWarnings({"rawtypes", "unchecked"})
class TestJavaDeleteHelper {

  @Test
  void testDeduplicateKeysForGlobalAndPartitionedIndexes() {
    JavaDeleteHelper helper = JavaDeleteHelper.newInstance();
    assertSame(helper, JavaDeleteHelper.newInstance());

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
}
