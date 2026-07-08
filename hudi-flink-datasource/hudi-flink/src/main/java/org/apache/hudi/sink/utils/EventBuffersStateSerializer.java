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

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.util.SerializationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.sink.event.WriteMetadataEvent;
import org.apache.hudi.sink.event.WriteMetadataStateSerializer;
import org.apache.hudi.sink.utils.EventBuffers.EventBuffer;

import org.apache.flink.core.io.SimpleVersionedSerialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Versioned serializer for the completed event buffers stored in coordinator checkpoints.
 *
 * <p>The outer version covers only the map and event-array layout. Changes to
 * {@link WriteMetadataEvent} are versioned independently by {@link WriteMetadataStateSerializer},
 * whose version is stored with every non-null event. State without the magic header is treated as
 * the legacy Java-serialized coordinator format.
 */
public final class EventBuffersStateSerializer {

  // ASCII "HEBS" (Hudi Event Buffers State).
  private static final int MAGIC = 0x48454253;
  private static final int VERSION = 1;
  private static final int NULL_EVENT = -1;

  private EventBuffersStateSerializer() {
  }

  /**
   * Serializes the outer event-buffer layout with its own version. Every non-null event is encoded
   * independently with {@link WriteMetadataStateSerializer}, including that serializer's version.
   */
  public static byte[] serialize(Map<Long, Pair<String, EventBuffer>> eventBuffers)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(MAGIC);
      out.writeInt(VERSION);
      out.writeInt(eventBuffers.size());
      for (Map.Entry<Long, Pair<String, EventBuffer>> entry
          : new TreeMap<>(eventBuffers).entrySet()) {
        out.writeLong(entry.getKey());
        writeString(out, entry.getValue().getLeft());
        EventBuffer eventBuffer = entry.getValue().getRight();
        writeEvents(out, eventBuffer.getDataWriteEventBuffer());
        writeEvents(out, eventBuffer.getIndexWriteEventBuffer());
      }
    }
    return bytes.toByteArray();
  }

  /**
   * Deserializes the versioned format, or falls back to the Java-serialized coordinator state used
   * before this envelope was introduced.
   */
  public static Map<Long, Pair<String, ?>> deserialize(byte[] bytes) throws IOException {
    if (!hasMagic(bytes)) {
      return SerializationUtils.deserialize(bytes);
    }

    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
      in.readInt();
      int version = in.readInt();
      if (version != VERSION) {
        throw new IOException("Unsupported event buffers state version: " + version);
      }

      int size = readNonNegativeInt(in, "event buffer count");
      Map<Long, Pair<String, ?>> eventBuffers = new LinkedHashMap<>(size);
      for (int i = 0; i < size; i++) {
        long checkpointId = in.readLong();
        String instantTime = readString(in);
        WriteMetadataEvent[] dataWriteEvents = readEvents(in);
        WriteMetadataEvent[] indexWriteEvents = readEvents(in);
        eventBuffers.put(
            checkpointId,
            Pair.of(instantTime, new EventBuffer(dataWriteEvents, indexWriteEvents)));
      }
      if (in.available() != 0) {
        throw new IOException("Unexpected trailing bytes in event buffers state");
      }
      return eventBuffers;
    }
  }

  private static void writeEvents(DataOutputStream out, WriteMetadataEvent[] events)
      throws IOException {
    out.writeInt(events.length);
    for (WriteMetadataEvent event : events) {
      if (event == null) {
        out.writeInt(NULL_EVENT);
      } else {
        byte[] eventBytes = SimpleVersionedSerialization.writeVersionAndSerialize(
            WriteMetadataStateSerializer.INSTANCE, event);
        out.writeInt(eventBytes.length);
        out.write(eventBytes);
      }
    }
  }

  private static WriteMetadataEvent[] readEvents(DataInputStream in) throws IOException {
    int size = readNonNegativeInt(in, "event array length");
    WriteMetadataEvent[] events = new WriteMetadataEvent[size];
    for (int i = 0; i < size; i++) {
      int eventLength = in.readInt();
      if (eventLength == NULL_EVENT) {
        continue;
      }
      if (eventLength < 0) {
        throw new IOException("Invalid serialized event length: " + eventLength);
      }
      byte[] eventBytes = new byte[eventLength];
      in.readFully(eventBytes);
      events[i] = SimpleVersionedSerialization.readVersionAndDeSerialize(
          WriteMetadataStateSerializer.INSTANCE, eventBytes);
    }
    return events;
  }

  private static void writeString(DataOutputStream out, String value) throws IOException {
    byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(valueBytes.length);
    out.write(valueBytes);
  }

  private static String readString(DataInputStream in) throws IOException {
    int length = readNonNegativeInt(in, "instant time length");
    byte[] valueBytes = new byte[length];
    in.readFully(valueBytes);
    return new String(valueBytes, StandardCharsets.UTF_8);
  }

  private static int readNonNegativeInt(DataInputStream in, String description) throws IOException {
    int value = in.readInt();
    if (value < 0) {
      throw new IOException("Invalid " + description + ": " + value);
    }
    return value;
  }

  private static boolean hasMagic(byte[] bytes) {
    return bytes.length >= Integer.BYTES
        && (bytes[0] & 0xFF) == (MAGIC >>> 24)
        && (bytes[1] & 0xFF) == ((MAGIC >>> 16) & 0xFF)
        && (bytes[2] & 0xFF) == ((MAGIC >>> 8) & 0xFF)
        && (bytes[3] & 0xFF) == (MAGIC & 0xFF);
  }
}
