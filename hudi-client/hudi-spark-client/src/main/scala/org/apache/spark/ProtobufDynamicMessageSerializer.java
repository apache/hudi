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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * ProtobufDynamicMessageSerializer is a kryo serializer for proto dynamic messages.
 * ProtobufSerializer available in chill library (<a href="https://github.com/twitter/chill/blob/master/chill-protobuf/src/main/java/com/twitter/chill/protobuf/ProtobufSerializer.java">...</a>)
 * uses parseFrom(byte[]..) method available in compiled proto messages. DynamicMessage doesn't have this method, and we need to handle this serialization through a concurrent hash map.
 */
public class ProtobufDynamicMessageSerializer extends Serializer<DynamicMessage> {

  @Override
  public void write(Kryo kryo, Output output, DynamicMessage dynamicMessage) {
    // Write the message type to the output stream
    String messageType = dynamicMessage.getDescriptorForType().getFullName();
    output.writeString(messageType);
    // Serialize the Descriptor into bytes and write the length followed by the bytes
    DescriptorProtos.DescriptorProto descriptorProto = dynamicMessage.getDescriptorForType().toProto();
    byte[] descriptorBytes = descriptorProto.toByteArray();
    output.writeInt(descriptorBytes.length);
    output.writeBytes(descriptorBytes);
    // Serialize the DynamicMessage into bytes and write the length followed by the bytes
    byte[] messageBytes = dynamicMessage.toByteArray();
    output.writeInt(messageBytes.length);
    output.writeBytes(messageBytes);
  }

  @Override
  public DynamicMessage read(Kryo kryo, Input input, Class<DynamicMessage> aClass) {
    try {
      String messageType = input.readString();
      // Lookup the Descriptor from the bytes.
      int descriptorLength = input.readInt();
      byte[] descriptorBytes = input.readBytes(descriptorLength);
      Descriptors.Descriptor descriptor = DescriptorProtos.DescriptorProto.parseFrom(descriptorBytes).getDescriptorForType();
      if (descriptor == null) {
        throw new IllegalArgumentException("Descriptor does not match for messageType: " + messageType);
      }
      // Read the serialized bytes
      int messageLength = input.readInt();
      byte[] messageBytes = input.readBytes(messageLength);
      // Deserialize the DynamicMessage using the descriptor
      return DynamicMessage.parseFrom(descriptor, messageBytes);
    } catch (InvalidProtocolBufferException e) {
      throw new HoodieIOException("Failed to deserialize DynamicMessage", e);
    }
  }
}

