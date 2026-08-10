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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.split.HoodieSourceSplitSerializer;
import org.apache.hudi.source.split.HoodieSourceSplitState;
import org.apache.hudi.source.split.HoodieSourceSplitStatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Serializer of Hoodie enumerator state.
 */
@Internal
public class HoodieEnumeratorStateSerializer implements SimpleVersionedSerializer<HoodieSplitEnumeratorState> {
  private static final int VERSION = 1;
  private final HoodieSourceSplitSerializer splitSerializer = new HoodieSourceSplitSerializer();

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(HoodieSplitEnumeratorState obj) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream out = new DataOutputStream(baos)) {

      // Serialize pending split states
      Collection<HoodieSourceSplitState> splitStates = obj.getPendingSplitStates();
      out.writeInt(splitStates.size());
      for (HoodieSourceSplitState splitState : splitStates) {
        byte[] splitBytes = splitSerializer.serialize(splitState.getSplit());
        out.writeInt(splitBytes.length);
        out.write(splitBytes);
        out.writeUTF(splitState.getStatus().name());
      }

      // Serialize lastEnumeratedInstant
      out.writeBoolean(obj.getLastEnumeratedInstant().isPresent());
      if (obj.getLastEnumeratedInstant().isPresent()) {
        out.writeUTF(obj.getLastEnumeratedInstant().get());
      }

      // Serialize lastEnumeratedInstantOffset
      out.writeBoolean(obj.getLastEnumeratedInstantOffset().isPresent());
      if (obj.getLastEnumeratedInstantOffset().isPresent()) {
        out.writeUTF(obj.getLastEnumeratedInstantOffset().get());
      }

      out.flush();
      return baos.toByteArray();
    }
  }

  @Override
  public HoodieSplitEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
         DataInputStream in = new DataInputStream(bais)) {

      // Deserialize pending split states
      int splitCount = in.readInt();
      List<HoodieSourceSplitState> splitStates = new ArrayList<>(splitCount);
      for (int i = 0; i < splitCount; i++) {
        int splitBytesLength = in.readInt();
        byte[] splitBytes = new byte[splitBytesLength];
        in.readFully(splitBytes);
        String statusName = in.readUTF();
        splitStates.add(new HoodieSourceSplitState(
            splitSerializer.deserialize(version, splitBytes),
            HoodieSourceSplitStatus.valueOf(statusName)));
      }

      // Deserialize lastEnumeratedInstant
      Option<String> lastEnumeratedInstant;
      if (in.readBoolean()) {
        lastEnumeratedInstant = Option.of(in.readUTF());
      } else {
        lastEnumeratedInstant = Option.empty();
      }

      // Deserialize lastEnumeratedInstantOffset
      Option<String> lastEnumeratedInstantOffset;
      if (in.readBoolean()) {
        lastEnumeratedInstantOffset = Option.of(in.readUTF());
      } else {
        lastEnumeratedInstantOffset = Option.empty();
      }

      return new HoodieSplitEnumeratorState(splitStates, lastEnumeratedInstant, lastEnumeratedInstantOffset);
    }
  }
}
