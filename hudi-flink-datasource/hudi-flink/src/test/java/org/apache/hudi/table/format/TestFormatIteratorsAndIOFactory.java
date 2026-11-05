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

package org.apache.hudi.table.format;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.row.HoodieRowDataFileWriterFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.table.format.cow.vector.reader.ParquetColumnarRowSplitReader;
import org.apache.hudi.util.RowDataProjection;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestFormatIteratorsAndIOFactory {

  @Test
  void testParquetSplitIteratorDelegatesLifecycle() throws Exception {
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    RowData row = GenericRowData.of(1);
    when(reader.reachedEnd()).thenReturn(false, true);
    when(reader.nextRecord()).thenReturn(row);
    ParquetSplitRecordIterator iterator = new ParquetSplitRecordIterator(reader);

    assertTrue(iterator.hasNext());
    assertSame(row, iterator.next());
    assertFalse(iterator.hasNext());
    iterator.close();
    verify(reader).close();
  }

  @Test
  void testParquetSplitIteratorWrapsReaderIoFailures() throws Exception {
    ParquetColumnarRowSplitReader reader = mock(ParquetColumnarRowSplitReader.class);
    when(reader.reachedEnd()).thenThrow(new IOException("read failure"));
    ParquetSplitRecordIterator iterator = new ParquetSplitRecordIterator(reader);

    HoodieIOException readFailure = assertThrows(HoodieIOException.class, iterator::hasNext);
    assertInstanceOf(IOException.class, readFailure.getCause());

    org.mockito.Mockito.doThrow(new IOException("close failure")).when(reader).close();
    HoodieIOException closeFailure = assertThrows(HoodieIOException.class, iterator::close);
    assertInstanceOf(IOException.class, closeFailure.getCause());
  }

  @Test
  @SuppressWarnings("unchecked")
  void testSchemaEvolvedIteratorProjectsAndClosesNestedIterator() {
    ClosableIterator<RowData> nested = mock(ClosableIterator.class);
    GenericRowData input = GenericRowData.of(1, 2);
    when(nested.hasNext()).thenReturn(true);
    when(nested.next()).thenReturn(input);
    LogicalType[] projectedTypes = {DataTypes.INT().getLogicalType()};
    RowDataProjection projection = RowDataProjection.instance(projectedTypes, new int[] {1});
    SchemaEvolvedRecordIterator iterator = new SchemaEvolvedRecordIterator(nested, projection);

    assertTrue(iterator.hasNext());
    assertEquals(2, iterator.next().getInt(0));
    iterator.close();
    verify(nested).close();
  }

  @Test
  void testFlinkIoFactoryCreatesRowDataFactories() {
    HoodieStorage storage = mock(HoodieStorage.class);
    HoodieFlinkIOFactory factory = new HoodieFlinkIOFactory(storage);

    assertInstanceOf(HoodieRowDataFileWriterFactory.class,
        factory.getWriterFactory(HoodieRecord.HoodieRecordType.FLINK));
    assertInstanceOf(HoodieRowDataFileReaderFactory.class,
        factory.getReaderFactory(HoodieRecord.HoodieRecordType.FLINK));
  }
}
