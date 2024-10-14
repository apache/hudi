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

package org.apache.spark;

import org.apache.hudi.exception.HoodieIOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestProtobufDynamicMessageSerializer {

  private ProtobufDynamicMessageSerializer serializer;
  private Kryo kryo;
  private Output output;
  private Input input;

  @BeforeEach
  public void setUp() {
    serializer = new ProtobufDynamicMessageSerializer();
    kryo = new Kryo();
  }

  @Test
  void testWriteAndRead() throws Exception {
    // Define a simple proto schema for testing
    DescriptorProtos.DescriptorProto descriptorProto = DescriptorProtos.DescriptorProto.newBuilder()
        .setName("TestMessage")
        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
            .setName("testField")
            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING)
            .setNumber(1))
        .build();
    Descriptors.Descriptor descriptor = Descriptors.FileDescriptor
        .buildFrom(DescriptorProtos.FileDescriptorProto.newBuilder()
            .addMessageType(descriptorProto)
            .build(), new Descriptors.FileDescriptor[] {})
        .findMessageTypeByName("TestMessage");
    DynamicMessage dynamicMessage = DynamicMessage.newBuilder(descriptor)
        .setField(descriptor.findFieldByName("testField"), "testValue")
        .build();
    // Serialize the DynamicMessage
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    output = new Output(byteArrayOutputStream);
    serializer.write(kryo, output, dynamicMessage);
    output.close();
    // Deserialize the DynamicMessage
    byte[] serializedBytes = byteArrayOutputStream.toByteArray();
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializedBytes);
    input = new Input(byteArrayInputStream);
    DynamicMessage deserializedMessage = serializer.read(kryo, input, DynamicMessage.class);
    // Verify that the deserialized message is the same as the original
    assertNotNull(deserializedMessage);
    assertArrayEquals(dynamicMessage.toByteArray(), deserializedMessage.toByteArray());
  }

  @Test
  void testInvalidMessageBytes() {
    // Mock an input to simulate a failure during DynamicMessage parsing
    Input mockInput = mock(Input.class);
    when(mockInput.readString()).thenReturn("TestMessage");
    when(mockInput.readInt()).thenReturn(10);  // Invalid descriptor length
    when(mockInput.readBytes(anyInt())).thenReturn(new byte[] {0x01});  // Invalid descriptor data
    // This should throw a HoodieIOException due to invalid bytes
    assertThrows(HoodieIOException.class, () -> serializer.read(kryo, mockInput, DynamicMessage.class));
  }
}
