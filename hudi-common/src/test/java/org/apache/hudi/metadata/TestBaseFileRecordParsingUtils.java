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

package org.apache.hudi.metadata;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.FileFormatUtils;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.metadata.BaseFileRecordParsingUtils.RecordStatus.DELETE;
import static org.apache.hudi.metadata.BaseFileRecordParsingUtils.RecordStatus.INSERT;
import static org.apache.hudi.metadata.BaseFileRecordParsingUtils.RecordStatus.UPDATE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class TestBaseFileRecordParsingUtils {

  @Test
  void testRecordKeyStatusClassificationAndSecondaryIndexKeys() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieIOFactory ioFactory = mock(HoodieIOFactory.class);
    FileFormatUtils fileFormatUtils = mock(FileFormatUtils.class);
    when(ioFactory.getFileFormatUtils(HoodieFileFormat.PARQUET)).thenReturn(fileFormatUtils);
    when(fileFormatUtils.readRowKeys(any(), any(StoragePath.class))).thenAnswer(invocation -> {
      StoragePath path = invocation.getArgument(1);
      return path.getName().equals("latest.parquet")
          ? new HashSet<>(Arrays.asList("inserted", "updated"))
          : new HashSet<>(Arrays.asList("updated", "deleted"));
    });

    try (MockedStatic<HoodieIOFactory> ioFactoryMock = mockStatic(HoodieIOFactory.class)) {
      ioFactoryMock.when(() -> HoodieIOFactory.getIOFactory(storage)).thenReturn(ioFactory);

      Map<BaseFileRecordParsingUtils.RecordStatus, List<String>> statuses =
          BaseFileRecordParsingUtils.getRecordKeyStatuses(
              "/table", "partition", "latest.parquet", "previous.parquet", storage,
              EnumSet.allOf(BaseFileRecordParsingUtils.RecordStatus.class));
      assertEquals(Collections.singletonList("inserted"), statuses.get(INSERT));
      assertEquals(Collections.singletonList("updated"), statuses.get(UPDATE));
      assertEquals(Collections.singletonList("deleted"), statuses.get(DELETE));

      assertTrue(BaseFileRecordParsingUtils.getRecordKeyStatuses(
          "/table", "partition", "latest.parquet", null, storage, EnumSet.of(UPDATE, DELETE)).isEmpty());
      assertEquals(
          new HashSet<>(Arrays.asList("inserted", "updated")),
          new HashSet<>(BaseFileRecordParsingUtils.getRecordKeyStatuses(
              "/table", "partition", "latest.parquet", null, storage, EnumSet.of(INSERT)).get(INSERT)));

      HoodieWriteStat writeStat = mock(HoodieWriteStat.class);
      when(writeStat.getPath()).thenReturn("partition/latest.parquet");
      when(writeStat.getPartitionPath()).thenReturn("partition");
      when(writeStat.getPrevBaseFile()).thenReturn("previous.parquet");
      List<String> changedKeys =
          BaseFileRecordParsingUtils.getRecordKeysDeletedOrUpdated("/table", writeStat, storage);
      assertEquals(new HashSet<>(Arrays.asList("updated", "deleted")), new HashSet<>(changedKeys));
    }
  }
}
