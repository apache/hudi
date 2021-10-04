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

package org.apache.hudi.common.util;

import org.apache.hudi.exception.HoodieException;

import org.apache.parquet.hadoop.ParquetReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestParquetReaderIterator {

  @Test
  public void testParquetIteratorIdempotency() throws IOException {
    ParquetReader reader = mock(ParquetReader.class);
    // only 1 record in reader
    when(reader.read()).thenReturn(1).thenReturn(null);
    ParquetReaderIterator<Integer> iterator = new ParquetReaderIterator<>(reader);
    int idempotencyCheckCounter = 0;
    // call hasNext() 3 times
    while (idempotencyCheckCounter < 3) {
      assertTrue(iterator.hasNext());
      idempotencyCheckCounter++;
    }
  }

  @Test
  public void testParquetIterator() throws IOException {

    ParquetReader reader = mock(ParquetReader.class);
    // only one record to read
    when(reader.read()).thenReturn(1).thenReturn(null);
    ParquetReaderIterator<Integer> iterator = new ParquetReaderIterator<>(reader);
    // should return value even though hasNext() hasn't been called
    assertEquals(1, iterator.next());
    // no more entries to iterate on
    assertFalse(iterator.hasNext());
    assertThrows(HoodieException.class, iterator::next, "should throw an exception since there is only 1 record");
    verify(reader, times(1)).close();
  }
}
