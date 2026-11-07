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

package org.apache.hudi.sink.event;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Versioned Avro serializer for Flink writer metadata state.
 *
 * <p>The Flink operator state stores the resulting bytes, so Hudi model class
 * evolution is handled here instead of by Flink's Kryo/Pojo serializer snapshot.
 */
public class WriteMetadataStateSerializer implements SimpleVersionedSerializer<WriteMetadataEvent> {
  public static final String STATE_NAME = "write-metadata-state-v2";
  public static final WriteMetadataStateSerializer INSTANCE = new WriteMetadataStateSerializer();

  static final int VERSION = 1;

  @Override
  public int getVersion() {
    return VERSION;
  }

  @Override
  public byte[] serialize(WriteMetadataEvent event) throws IOException {
    return HoodieAvroUtils.avroToBytes(toAvro(event));
  }

  @Override
  public WriteMetadataEvent deserialize(int version, byte[] bytes) throws IOException {
    return fromAvro(HoodieAvroUtils.convertToSpecificRecord(
        org.apache.hudi.sink.avro.model.WriteMetadataEvent.class,
        HoodieAvroUtils.bytesToAvro(
            bytes, org.apache.hudi.sink.avro.model.WriteMetadataEvent.getClassSchema())));
  }

  private org.apache.hudi.sink.avro.model.WriteMetadataEvent toAvro(WriteMetadataEvent event)
      throws IOException {
    List<org.apache.hudi.sink.avro.model.WriteStatus> statusStates =
        new ArrayList<>(event.getWriteStatuses().size());
    for (WriteStatus status : event.getWriteStatuses()) {
      statusStates.add(WriteStatusAvroMapper.toAvro(status));
    }
    org.apache.hudi.sink.avro.model.WriteMetadataEvent state =
        new org.apache.hudi.sink.avro.model.WriteMetadataEvent();
    state.setWriteStatuses(statusStates);
    state.setTaskId(event.getTaskID());
    state.setInstantTime(event.getInstantTime());
    state.setLastBatch(event.isLastBatch());
    state.setEndInput(event.isEndInput());
    state.setBootstrap(event.isBootstrap());
    return state;
  }

  private WriteMetadataEvent fromAvro(org.apache.hudi.sink.avro.model.WriteMetadataEvent state)
      throws IOException {
    List<WriteStatus> statuses = new ArrayList<>(state.getWriteStatuses().size());
    for (org.apache.hudi.sink.avro.model.WriteStatus statusState : state.getWriteStatuses()) {
      statuses.add(WriteStatusAvroMapper.fromAvro(statusState));
    }
    return WriteMetadataEvent.builder()
        .writeStatus(statuses)
        .taskID(state.getTaskId())
        .instantTime(state.getInstantTime())
        .lastBatch(state.getLastBatch())
        .endInput(state.getEndInput())
        .bootstrap(state.getBootstrap())
        .build();
  }
}
