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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TestHoodieRealtimeFileSplit {

  private HoodieRealtimeFileSplit split;
  private String basePath;
  private List<HoodieLogFile> deltaLogFiles;
  private List<String> deltaLogPaths;
  private String fileSplitName;
  private FileSplit baseFileSplit;
  private String maxCommitTime;

  @BeforeEach
  public void setUp(@TempDir java.nio.file.Path tempDir) throws Exception {
    basePath = tempDir.toAbsolutePath().toString();
    deltaLogFiles = Collections.singletonList(new HoodieLogFile(new Path(basePath + "/1.log"), 0L));
    deltaLogPaths = Collections.singletonList(basePath + "/1.log");
    fileSplitName = basePath + "/test.file";
    baseFileSplit = new FileSplit(new Path(fileSplitName), 0, 100, new String[] {});
    maxCommitTime = "10001";

    split = new HoodieRealtimeFileSplit(baseFileSplit, basePath, deltaLogFiles, maxCommitTime, false, Option.empty());
  }

  @Test
  public void testWrite() throws IOException {
    // create a mock for DataOutput that will be used in the write method
    // this way we can capture and verify if correct arguments were passed
    DataOutput out = mock(DataOutput.class);

    // register expected method calls for void functions
    // so that we can verify what was called after the method call finishes
    doNothing().when(out).writeByte(anyInt());
    doNothing().when(out).writeInt(anyInt());
    doNothing().when(out).write(any(byte[].class), anyInt(), anyInt());
    doNothing().when(out).write(any(byte[].class));

    // call the method we want to test with the mocked input
    split.write(out);

    // verify the method calls on the mocked object in the order of the calls
    InOrder inorder = inOrder(out);
    inorder.verify(out, times(1)).writeByte(eq(fileSplitName.length()));
    inorder.verify(out, times(1)).write(aryEq(Text.encode(fileSplitName).array()), eq(0), eq(fileSplitName.length()));
    inorder.verify(out, times(1)).writeInt(eq(basePath.length()));
    inorder.verify(out, times(1)).write(aryEq(basePath.getBytes(StandardCharsets.UTF_8)));
    inorder.verify(out, times(1)).writeInt(eq(maxCommitTime.length()));
    inorder.verify(out, times(1)).write(aryEq(maxCommitTime.getBytes(StandardCharsets.UTF_8)));
    inorder.verify(out, times(1)).writeInt(eq(deltaLogPaths.size()));
    inorder.verify(out, times(1)).writeInt(eq(deltaLogPaths.get(0).length()));
    inorder.verify(out, times(1)).write(aryEq(deltaLogPaths.get(0).getBytes(StandardCharsets.UTF_8)));
    inorder.verify(out, times(1)).writeBoolean(false);
    // verify there are no more interactions happened on the mocked object
    inorder.verifyNoMoreInteractions();
  }

  @Test
  public void testReadFields() throws IOException {
    // create a mock for DataOutput that will be used in the readFields method
    // this way we can capture and verify if correct arguments were passed
    DataInput in = mock(DataInput.class);

    // register the mock responses to be returned when particular method call happens
    // on the mocked object
    when(in.readByte()).thenReturn((byte) fileSplitName.length());
    // Answer implementation is used to guarantee the response in sequence of the mock method calls
    // since the same method is called many times, we need to return the responses in proper sequence
    when(in.readInt()).thenAnswer(new Answer<Integer>() {
      private int count = 0;
      private int[] answers = new int[]{basePath.length(), maxCommitTime.length(), deltaLogPaths.size(), deltaLogPaths.get(0).length()};

      @Override
      public Integer answer(InvocationOnMock invocationOnMock) throws Throwable {
        return answers[count++];
      }
    });
    Answer<Void> readFullyAnswer = new Answer<Void>() {
      private int count = 0;
      private byte[][] answers = new byte[][]{
          fileSplitName.getBytes(StandardCharsets.UTF_8),
          basePath.getBytes(StandardCharsets.UTF_8),
          maxCommitTime.getBytes(StandardCharsets.UTF_8),
          deltaLogPaths.get(0).getBytes(StandardCharsets.UTF_8),
      };

      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        byte[] bytes = invocation.getArgument(0);
        byte[] answer = answers[count++];
        System.arraycopy(answer, 0, bytes, 0, answer.length);
        return null;
      }
    };
    doAnswer(readFullyAnswer).when(in).readFully(any());
    doAnswer(readFullyAnswer).when(in).readFully(any(), anyInt(), anyInt());

    // call readFields with mocked object
    HoodieRealtimeFileSplit read = new HoodieRealtimeFileSplit();
    read.readFields(in);

    // assert proper returns after reading from the mocked object
    assertEquals(basePath, read.getBasePath());
    assertEquals(maxCommitTime, read.getMaxCommitTime());
    assertEquals(deltaLogPaths, read.getDeltaLogPaths());
    assertEquals(split.toString(), read.toString());
  }

  @Test
  public void testSerDe(@TempDir java.nio.file.Path tempDir) throws IOException {
    final HoodieRealtimeFileSplit original = split;
    java.nio.file.Path tempFilePath = tempDir.resolve("tmp.txt");
    try (DataOutputStream out = new DataOutputStream(new FileOutputStream(tempFilePath.toFile()))) {
      original.write(out);
    }
    HoodieRealtimeFileSplit deserialized = new HoodieRealtimeFileSplit();
    try (DataInputStream in = new DataInputStream(new FileInputStream(tempFilePath.toFile()))) {
      deserialized.readFields(in);
    }
    assertEquals(original.toString(), deserialized.toString());
  }
}
