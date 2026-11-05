/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner.index;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestIndexRowUtils {

  @Test
  void testCreateRecordIndexRowsForInsertAndDelete() {
    String fileId = UUID.randomUUID().toString();
    HoodieFlinkInternalRow insert = internalRow("I", fileId);
    HoodieFlinkInternalRow delete = internalRow("D", fileId);

    RowData insertIndexRow = IndexRowUtils.createRecordIndexRow(insert);
    RowData deleteIndexRow = IndexRowUtils.createRecordIndexRow(delete);

    assertEquals(RowKind.INSERT, insertIndexRow.getRowKind());
    assertEquals(RowKind.DELETE, deleteIndexRow.getRowKind());
    assertEquals(IndexRowUtils.RLI_TYPE, insertIndexRow.getByte(0));
    assertEquals("key-1", IndexRowUtils.getRecordKey(insertIndexRow));
    assertEquals("partition-1", IndexRowUtils.getPartition(insertIndexRow));
    assertEquals(new HoodieKey("key-1", "partition-1"), IndexRowUtils.getHoodieKey(insertIndexRow));
  }

  @Test
  void testCreateRecordIndexRowRejectsUnexpectedOperation() {
    HoodieException exception = assertThrows(HoodieException.class,
        () -> IndexRowUtils.createRecordIndexRow(internalRow("U", UUID.randomUUID().toString())));
    assertEquals("Unexpected operation type: U", exception.getMessage());
  }

  @Test
  void testConvertInsertAndDeleteIndexRowsToHoodieRecords() {
    String fileId = UUID.randomUUID().toString();
    RowData insertIndexRow = IndexRowUtils.createRecordIndexRow(internalRow("I", fileId));
    RowData deleteIndexRow = IndexRowUtils.createRecordIndexRow(internalRow("D", fileId));
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    when(writeConfig.getWritesFileIdEncoding()).thenReturn(0);
    when(writeConfig.isRecordLevelIndexEnabled()).thenReturn(true);

    HoodieRecord insertRecord = IndexRowUtils.convertToHoodieRecord(1L, insertIndexRow, writeConfig);
    HoodieRecord deleteRecord = IndexRowUtils.convertToHoodieRecord(1L, deleteIndexRow, writeConfig);

    assertNotNull(insertRecord);
    assertNotNull(deleteRecord);
    assertEquals("key-1", insertRecord.getRecordKey());
    assertEquals("key-1", deleteRecord.getRecordKey());
  }

  @Test
  void testConvertRejectsUnsupportedRowKindAndIndexType() {
    HoodieWriteConfig writeConfig = mock(HoodieWriteConfig.class);
    GenericRowData unsupportedKind = indexRow(IndexRowUtils.RLI_TYPE, RowKind.UPDATE_AFTER);
    GenericRowData unsupportedType = indexRow((byte) 1, RowKind.INSERT);

    HoodieException kindException = assertThrows(HoodieException.class,
        () -> IndexRowUtils.convertToHoodieRecord(1L, unsupportedKind, writeConfig));
    assertEquals("Unsupported operation type for index row: UPDATE_AFTER", kindException.getMessage());
    HoodieException typeException = assertThrows(HoodieException.class,
        () -> IndexRowUtils.convertToHoodieRecord(1L, unsupportedType, writeConfig));
    assertEquals("Unsupported type for index row: 1", typeException.getMessage());
  }

  private static HoodieFlinkInternalRow internalRow(String operation, String fileId) {
    HoodieFlinkInternalRow row =
        new HoodieFlinkInternalRow("key-1", "partition-1", operation, new GenericRowData(0));
    row.setFileId(fileId);
    return row;
  }

  private static GenericRowData indexRow(byte indexType, RowKind rowKind) {
    GenericRowData row = new GenericRowData(IndexRowUtils.INDEX_ROW_TYPE.getFieldCount());
    row.setField(0, indexType);
    row.setField(1, StringData.fromString("key-1"));
    row.setField(2, StringData.fromString("partition-1"));
    row.setField(3, StringData.fromString(UUID.randomUUID().toString()));
    row.setRowKind(rowKind);
    return row;
  }
}
