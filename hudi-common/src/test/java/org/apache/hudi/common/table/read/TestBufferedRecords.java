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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestBufferedRecords {

  @Test
  void testGetOrderingValueFromDeleteRecord() {
    RecordContext recordContext = mock(RecordContext.class);
    when(recordContext.getOrderingValue(any(DeleteRecord.class))).thenCallRealMethod();
    DeleteRecord deleteRecord = mock(DeleteRecord.class);
    mockDeleteRecord(deleteRecord, null);
    assertEquals(OrderingValues.getDefault(), recordContext.getOrderingValue(deleteRecord));
    mockDeleteRecord(deleteRecord, OrderingValues.getDefault());
    assertEquals(OrderingValues.getDefault(), recordContext.getOrderingValue(deleteRecord));
    Comparable orderingValue = "xyz";
    Comparable convertedValue = "_xyz";
    mockDeleteRecord(deleteRecord, orderingValue);
    when(recordContext.convertOrderingValueToEngineType(orderingValue)).thenReturn(convertedValue);
    assertEquals(convertedValue, recordContext.getOrderingValue(deleteRecord));
  }

  private void mockDeleteRecord(DeleteRecord deleteRecord,
                                Comparable orderingValue) {
    when(deleteRecord.getOrderingValue()).thenReturn(orderingValue);
  }
}
