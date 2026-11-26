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

package org.apache.hudi.sink.bulk;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.io.storage.row.HoodieRowDataCreateHandle;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.FlinkTables;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.table.data.RowData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Test cases for {@link BulkInsertWriteFunction}.
 */
class TestBulkInsertWriteFunction {

  private BulkInsertWriteFunction<RowData> function;
  private HoodieFlinkWriteClient writeClient;
  private OperatorEventGateway gateway;

  @BeforeEach
  void setUp() throws Exception {
    function = new BulkInsertWriteFunction<>(new Configuration(), TestConfigurations.ROW_TYPE);
    writeClient = mock(HoodieFlinkWriteClient.class);
    gateway = mock(OperatorEventGateway.class);
    setField("writeClient", writeClient);
    function.setOperatorEventGateway(gateway);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCloseBeforeWriterHelperInitialization(boolean clientCreated) throws Exception {
    if (!clientCreated) {
      setField("writeClient", null);
    }

    function.close();

    if (clientCreated) {
      verify(writeClient).close();
    } else {
      verifyNoInteractions(writeClient);
    }
    verifyNoInteractions(gateway);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCloseBeforeEndInput(boolean writeFails) throws Exception {
    BulkInsertWriterHelper helper = mock(BulkInsertWriterHelper.class);
    setField("writerHelper", helper);
    RowData row = TestData.DATA_SET_INSERT.get(0);
    function.processElement(row, null, null);
    if (writeFails) {
      IOException failure = new IOException("write failed");
      doThrow(failure).when(helper).write(row);
      assertSame(failure, assertThrows(IOException.class, () -> function.processElement(row, null, null)));
    }

    function.close();

    InOrder order = inOrder(helper, writeClient);
    order.verify(helper).close();
    order.verify(writeClient).close();
    verifyNoInteractions(gateway);
  }

  @ParameterizedTest
  @CsvSource({"false, false", "true, false", "false, true", "true, true"})
  void testCloseFailureStillClosesWriteClient(boolean runtimeFailure, boolean clientCloseFails) throws Exception {
    BulkInsertWriterHelper helper = mock(BulkInsertWriterHelper.class);
    setField("writerHelper", helper);
    Exception failure = runtimeFailure ? new IllegalStateException("close failed") : new IOException("close failed");
    doThrow(failure).when(helper).close();
    RuntimeException clientFailure = new RuntimeException("client close failed");
    if (clientCloseFails) {
      doThrow(clientFailure).when(writeClient).close();
    }

    Exception thrown = assertThrows(Exception.class, function::close);
    assertSame(failure, thrown);
    assertArrayEquals(clientCloseFails ? new Throwable[] {clientFailure} : new Throwable[0], thrown.getSuppressed());

    InOrder order = inOrder(helper, writeClient);
    order.verify(helper).close();
    order.verify(writeClient).close();
    verifyNoInteractions(gateway);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCloseHandlesWithOrWithoutEndInput(boolean endInput, @TempDir File tempDir) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    StreamerUtil.initTableIfNotExists(conf);
    HoodieFlinkTable<?> table = FlinkTables.createTable(conf);
    BulkInsertWriterHelper helper = new BulkInsertWriterHelper(conf, table, table.getConfig(),
        WriteClientTestUtils.createNewInstantTime(), 0, 1, 0, TestConfigurations.ROW_TYPE);
    HoodieRowDataCreateHandle handle = mock(HoodieRowDataCreateHandle.class);
    WriteStatus status = new WriteStatus();
    when(handle.close()).thenReturn(status);
    helper.handles.put("par1", handle);
    setField("writerHelper", helper);

    if (endInput) {
      function.endInput();
    }
    function.close();

    verify(handle).close();
    assertTrue(helper.handles.isEmpty());
    assertEquals(Collections.singletonList(status), helper.getWriteStatuses(0));
    verify(writeClient).close();
    if (endInput) {
      verify(gateway).sendEventToCoordinator(any(WriteMetadataEvent.class));
      verifyNoMoreInteractions(gateway);
    } else {
      verifyNoInteractions(gateway);
    }
  }

  private void setField(String name, Object value) throws Exception {
    Field field = BulkInsertWriteFunction.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(function, value);
  }
}
