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

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.ByteBufferBackedInputStream;
import org.apache.hudi.io.ByteArraySeekableDataInputStream;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests {@link HoodieLogBlock}
 */
public class TestHoodieLogBlock {
  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3})
  public void testErrorHandlingInInflate(int numReadFailTimes) throws IOException {
    int position = 20;
    int blockSize = 1000;
    SeekableDataInputStream stream = prepareMockedLogInputStream(blockSize, numReadFailTimes);
    Supplier<SeekableDataInputStream> inputStreamSupplier = () -> stream;
    HoodieAvroDataBlock dataBlock = new HoodieAvroDataBlock(
        inputStreamSupplier,
        Option.empty(),
        true,
        new HoodieLogBlock.HoodieLogBlockContentLocation(
            mock(HoodieStorage.class), new HoodieLogFile("log_file"), position, blockSize, position + blockSize),
        Option.of(SchemaBuilder.builder().record("test_schema").fields().endRecord()),
        Collections.emptyMap(),
        Collections.emptyMap(),
        "key"
    );
    assertFalse(dataBlock.getContent().isPresent());
    dataBlock.inflate();

    byte[] expected = expectedContent(blockSize);
    Option<byte[]> actual = dataBlock.getContent();
    assertTrue(actual.isPresent());
    assertArrayEquals(expected, actual.get());
  }

  @Test
  public void testHeaderMetadata() throws IOException {
    Map<HoodieLogBlock.HeaderMetadataType, String> a = new HashMap<>();
    a.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, "100");
    a.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, "1");
    a.put(HoodieLogBlock.HeaderMetadataType.SCHEMA, "{}");
    a.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE, "rollback");
    a.put(HoodieLogBlock.HeaderMetadataType.COMPACTED_BLOCK_TIMES, "1");
    a.put(HoodieLogBlock.HeaderMetadataType.RECORD_POSITIONS, "");
    a.put(HoodieLogBlock.HeaderMetadataType.BLOCK_IDENTIFIER, "1");
    a.put(HoodieLogBlock.HeaderMetadataType.IS_PARTIAL, "true");
    byte[] bytes = HoodieLogBlock.getHeaderMetadataBytes(a);

    Map<HoodieLogBlock.HeaderMetadataType, String> b = HoodieLogBlock.getHeaderMetadata(new ByteArraySeekableDataInputStream(new ByteBufferBackedInputStream(bytes)));
    Assertions.assertEquals("100", b.get(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME));
    Assertions.assertEquals("1", b.get(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME));
    Assertions.assertEquals("{}", b.get(HoodieLogBlock.HeaderMetadataType.SCHEMA));
    Assertions.assertEquals("rollback", b.get(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE));
    Assertions.assertEquals("1", b.get(HoodieLogBlock.HeaderMetadataType.COMPACTED_BLOCK_TIMES));
    Assertions.assertEquals("", b.get(HoodieLogBlock.HeaderMetadataType.RECORD_POSITIONS));
    Assertions.assertEquals("1", b.get(HoodieLogBlock.HeaderMetadataType.BLOCK_IDENTIFIER));
    Assertions.assertEquals("true", b.get(HoodieLogBlock.HeaderMetadataType.IS_PARTIAL));
  }

  private SeekableDataInputStream prepareMockedLogInputStream(int contentSize,
                                                              int numReadFailTimes) throws IOException {
    IOException exception = new IOException("Read content from log file fails");
    Answer<Integer> mockedContentAnswer = modifyContentWithRead(contentSize);
    InputStream stream = mock(InputStream.class);
    switch (numReadFailTimes) {
      case 0:
        when(stream.read(any(), eq(0), eq(contentSize)))
            .thenAnswer(mockedContentAnswer);
        break;
      case 1:
        when(stream.read(any(), eq(0), eq(contentSize)))
            .thenThrow(exception)
            .thenAnswer(mockedContentAnswer);
        break;
      case 2:
        when(stream.read(any(), eq(0), eq(contentSize)))
            .thenThrow(exception)
            .thenThrow(exception)
            .thenAnswer(mockedContentAnswer);
        break;
      case 3:
        when(stream.read(any(), eq(0), eq(contentSize)))
            .thenThrow(exception)
            .thenThrow(exception)
            .thenThrow(exception)
            .thenAnswer(mockedContentAnswer);
        break;
      default:
        throw new IllegalArgumentException("This mock does not support more than 3 failed read calls");
    }

    return new SeekableDataInputStream(stream) {
      @Override
      public long getPos() throws IOException {
        return 0;
      }

      @Override
      public void seek(long pos) throws IOException {
      }
    };
  }

  private Answer<Integer> modifyContentWithRead(int contentSize) {
    return invocation -> {
      Object[] args = invocation.getArguments();
      byte[] content = (byte[]) args[0];
      byte[] expected = expectedContent(contentSize);
      System.arraycopy(expected, 0, content, 0, content.length);
      return contentSize;
    };
  }

  private byte[] expectedContent(int contentSize) {
    byte[] content = new byte[contentSize];
    IntStream.range(0, contentSize).forEach(i -> content[i] = (byte) (i % 256));
    return content;
  }
}
