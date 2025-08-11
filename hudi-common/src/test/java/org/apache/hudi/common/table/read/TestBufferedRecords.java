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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.util.OrderingValues;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.table.read.BufferedRecords.getOrderingValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBufferedRecords {

  @Test
  void testGetOrderingValueFromDeleteRecord() {
    RecordContext recordContext = mock(RecordContext.class);
    DeleteRecord deleteRecord = mock(DeleteRecord.class);
    mockDeleteRecord(deleteRecord, null);
    assertEquals(OrderingValues.getDefault(), getOrderingValue(recordContext, deleteRecord));
    mockDeleteRecord(deleteRecord, OrderingValues.getDefault());
    assertEquals(OrderingValues.getDefault(), getOrderingValue(recordContext, deleteRecord));
    Comparable orderingValue = "xyz";
    Comparable convertedValue = "_xyz";
    mockDeleteRecord(deleteRecord, orderingValue);
    when(recordContext.convertOrderingValueToEngineType(orderingValue)).thenReturn(convertedValue);
    assertEquals(convertedValue, getOrderingValue(recordContext, deleteRecord));
  }

  private void mockDeleteRecord(DeleteRecord deleteRecord,
                                Comparable orderingValue) {
    when(deleteRecord.getOrderingValue()).thenReturn(orderingValue);
  }
}
