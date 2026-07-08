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

import org.apache.avro.Schema;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Versioned serializer for Flink writer metadata state.
 *
 * <p>The Flink operator state stores the resulting bytes, so Hudi model class
 * evolution is handled here instead of by Flink's Kryo/Pojo serializer snapshot. The complete
 * event state, including each write stat, is encoded as one Avro DTO graph.
 */
public class WriteMetadataStateSerializer implements SimpleVersionedSerializer<WriteMetadataEvent> {
  public static final String STATE_NAME = "write-metadata-state-v2";
  public static final WriteMetadataStateSerializer INSTANCE = new WriteMetadataStateSerializer();

  /*
   * Writer-state payload versions identify the complete Avro WriteMetadataEvent writer schema.
   * Changes to WriteStatus fields that are not persisted in state (for example failed records and
   * index stats) do not affect this versioning. Full restore of a release-specific Avro payload
   * additionally requires that release's writer schema. Each version below is described relative
   * to the preceding version. Currently this branch bundles the v1 and v4 writer schemas; v2 and
   * v3 reserve the version numbers and document the schemas to be supplied by their branch patches.
   *
   * 1: Hudi 0.14.x and 0.15.x with the Avro state patch.
   *    Baseline schema. The event header contains taskId, instantTime, lastBatch, endInput, and
   *    bootstrap, but no checkpointId or metadataTable. The Avro WriteStatus DTO stores the coordinator
   *    fields (stat, errors, globalError, fileId, partitionPath, totalRecords, totalErrorRecords,
   *    and recordsStats). The stat field is a union of Avro HoodieWriteStat and
   *    HoodieDeltaWriteStat. ColumnRange uses the legacy column-stat representation,
   *    without ValueMetadata or nanoValue in TypedValue.
   *
   * 2: Hudi 1.0.x with its release-specific Avro state patch, relative to v1.
   *    The event header and Avro WriteStatus fields are unchanged. Both write-stat records add
   *    prevBaseFile, numUpdates, and totalLogReadTimeMs. The remaining read/log counters moved
   *    from HoodieWriteStat to its HoodieReadStats superclass in the model, but retain the same
   *    state fields and are not removed from the Avro projection.
   *
   * 3: Hudi 1.1.x with its release-specific Avro state patch, relative to v2.
   *    The event header adds checkpointId; v1/v2 payloads restore it as -1. recordsStats moves
   *    from HoodieDeltaWriteStat to HoodieWriteStat in the model. Its state representation remains
   *    the existing Avro WriteStatus.recordsStats field, but the v3 writer can populate it for
   *    either stat subtype. ColumnRange adds ValueMetadata for the 1.1 column-stat model.
   *    Although WriteStatus gains isMetadataTable and IndexStats in this release, neither changes
   *    the payload: the event header still has no metadataTable field, so restored statuses default
   *    to non-metadata-table, and index stats are intentionally not persisted.
   *
   * 4: Hudi 1.2.x and this branch, relative to v3.
   *    The event header adds metadataTable; older payloads restore it as false and use that value
   *    when reconstructing every WriteStatus. TypedValue adds nanoValue and the mapper adds the
   *    current UUID and java.time Comparable value types. The remaining event, status, stat,
   *    ColumnRange, and ValueMetadata fields are unchanged from v3.
   */
  static final int VERSION_0_14 = 1;
  static final int VERSION_1_0 = 2;
  static final int VERSION_1_1 = 3;
  static final int VERSION = 4;

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
    Schema writerSchema = getWriterSchema(version);
    return fromAvro(HoodieAvroUtils.convertToSpecificRecord(
        org.apache.hudi.sink.avro.model.WriteMetadataEvent.class,
        HoodieAvroUtils.bytesToAvro(
            bytes, writerSchema, org.apache.hudi.sink.avro.model.WriteMetadataEvent.getClassSchema())));
  }

  static Schema getWriterSchema(int version) throws IOException {
    if (version == VERSION) {
      return org.apache.hudi.sink.avro.model.WriteMetadataEvent.getClassSchema();
    }
    if (version == VERSION_0_14) {
      return loadSchema("avro/write-metadata-state/v1/WriteMetadataEventState.avsc");
    }
    throw new IOException("Unsupported Avro write metadata state payload version: " + version);
  }

  private static Schema loadSchema(String resource) throws IOException {
    try (InputStream inputStream = WriteMetadataStateSerializer.class
        .getClassLoader().getResourceAsStream(resource)) {
      if (inputStream == null) {
        throw new IOException("Missing Avro write metadata state schema resource: " + resource);
      }
      return new Schema.Parser().parse(inputStream);
    }
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
    state.setCheckpointId(event.getCheckpointId());
    state.setInstantTime(event.getInstantTime());
    state.setLastBatch(event.isLastBatch());
    state.setEndInput(event.isEndInput());
    state.setBootstrap(event.isBootstrap());
    state.setMetadataTable(event.isMetadataTable());
    return state;
  }

  private WriteMetadataEvent fromAvro(org.apache.hudi.sink.avro.model.WriteMetadataEvent state)
      throws IOException {
    List<WriteStatus> statuses = new ArrayList<>(state.getWriteStatuses().size());
    for (org.apache.hudi.sink.avro.model.WriteStatus statusState : state.getWriteStatuses()) {
      statuses.add(WriteStatusAvroMapper.fromAvro(statusState, state.getMetadataTable()));
    }
    return WriteMetadataEvent.builder()
        .writeStatus(statuses)
        .taskID(state.getTaskId())
        .checkpointId(state.getCheckpointId())
        .instantTime(state.getInstantTime())
        .lastBatch(state.getLastBatch())
        .endInput(state.getEndInput())
        .bootstrap(state.getBootstrap())
        .metadataTable(state.getMetadataTable())
        .build();
  }
}
