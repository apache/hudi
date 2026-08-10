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

package org.apache.hudi.parquet.io;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.parquet.io.PositionOutputStream;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestOutputStreamBackedOutputFile {

  @Test
  public void testCreate() throws IOException {
    FSDataOutputStream mockStream = mock(FSDataOutputStream.class);
    OutputStreamBackedOutputFile outputFile = new OutputStreamBackedOutputFile(mockStream);

    PositionOutputStream posStream = outputFile.create(1024);
    assertNotNull(posStream);

    posStream.write(1);
    verify(mockStream).write(1);

    byte[] data = new byte[] {1, 2, 3};
    posStream.write(data, 0, 3);
    verify(mockStream).write(data, 0, 3);

    when(mockStream.getPos()).thenReturn(100L);
    assertEquals(100L, posStream.getPos());

    posStream.flush();
    verify(mockStream).flush();

    // Close on adapter should NOT close delegate
    posStream.close();
    verify(mockStream, Mockito.never()).close();
  }

  @Test
  public void testCreateOrOverwrite() {
    FSDataOutputStream mockStream = mock(FSDataOutputStream.class);
    OutputStreamBackedOutputFile outputFile = new OutputStreamBackedOutputFile(mockStream);
    assertNotNull(outputFile.createOrOverwrite(1024));
  }

  @Test
  public void testSupportsBlockSize() {
    FSDataOutputStream mockStream = mock(FSDataOutputStream.class);
    OutputStreamBackedOutputFile outputFile = new OutputStreamBackedOutputFile(mockStream);
    assertFalse(outputFile.supportsBlockSize());
  }

  @Test
  public void testDefaultBlockSize() {
    FSDataOutputStream mockStream = mock(FSDataOutputStream.class);
    OutputStreamBackedOutputFile outputFile = new OutputStreamBackedOutputFile(mockStream);
    assertEquals(1024L * 1024L, outputFile.defaultBlockSize());
  }
}
