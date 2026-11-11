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

package org.apache.hudi.common.model;

import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.Getter;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Base class for all AVRO record based payloads, that can be ordered based on a field.
 *
 * <p>A payload built from a record keeps that record's schema in memory as its writer schema, so a
 * request for another schema resolves fields by name and alias, with the same-position rename used
 * by older realtime readers as a fallback. The serialized forms do not carry the writer schema: a
 * payload read back from Java or Kryo serialization decodes positionally with the requested schema,
 * and so does a request with a null schema.
 */
public abstract class BaseAvroPayload implements Serializable, KryoSerializable {
  // The serialVersionUID computed for this class before it was pinned.
  private static final long serialVersionUID = 4076216714695518773L;

  // Keyed by schema identity, as records of one write share their schema instances.
  private static final Cache<Schema, Cache<Schema, Resolution>> RESOLUTIONS =
      Caffeine.newBuilder().weakKeys().maximumSize(1024).build();

  /**
   * Avro data extracted from the source converted to bytes.
   */
  private byte[] recordBytes;

  /**
   * For purposes of preCombining.
   */
  @Getter
  protected Comparable orderingVal;

  protected boolean isDeletedRecord;

  private transient GenericRecord record;

  private transient Schema writerSchema;

  /**
   * Instantiate {@link BaseAvroPayload}.
   *
   * @param record      Generic record for the payload.
   * @param orderingVal {@link Comparable} to be used in pre combine.
   */
  public BaseAvroPayload(GenericRecord record, Comparable orderingVal) {
    this.record = record;
    this.recordBytes = null; // only initialized when needed
    this.writerSchema = record == null ? null : record.getSchema();
    this.orderingVal = orderingVal;
    this.isDeletedRecord = record == null || isDeleteRecord(record);

    if (orderingVal == null) {
      throw new HoodieException("Ordering value is null for record: " + record);
    }
  }

  /**
   * Defines whether this implementation of {@link HoodieRecordPayload} is deleted.
   * We will not do deserialization in this method.
   */
  public boolean isDeleted(Schema schema, Properties props) {
    return isDeletedRecord;
  }

  /**
   * Defines whether this implementation of {@link HoodieRecordPayload} could produce
   * {@link HoodieRecord#SENTINEL}
   */
  public boolean canProduceSentinel() {
    return false;
  }

  /**
   * @param genericRecord instance of {@link GenericRecord} of interest.
   * @returns {@code true} if record represents a delete record. {@code false} otherwise.
   */
  protected boolean isDeleteRecord(GenericRecord genericRecord) {
    final String isDeleteKey = HoodieRecord.HOODIE_IS_DELETED_FIELD;
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema().getField(isDeleteKey) == null) {
      return false;
    }
    Object deleteMarker = genericRecord.get(isDeleteKey);
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

  public byte[] getRecordBytes() {
    if (recordBytes == null) {
      if (record == null) {
        recordBytes = new byte[0];
      } else {
        recordBytes = HoodieAvroUtils.avroToBytes(record);
      }
    }
    return recordBytes;
  }

  public Option<IndexedRecord> getIndexedRecord(Schema schema, Properties properties) throws IOException {
    return getRecord(schema);
  }

  protected boolean isEmptyRecord() {
    if (recordBytes == null) {
      return record == null;
    }
    return recordBytes.length == 0;
  }

  protected Option<IndexedRecord> getRecord(Schema schema) throws IOException {
    if (record != null && record.getSchema() == schema) {
      return Option.of(record);
    }
    byte[] bytes = getRecordBytes();
    if (bytes.length == 0) {
      return Option.empty();
    }
    record = writerSchema == null || schema == null ? decodePositionally(schema, bytes) : decodeWithWriterSchema(schema, bytes);
    return Option.of(record);
  }

  private static GenericRecord decodePositionally(Schema schema, byte[] bytes) {
    return SerializableIndexedRecord.fromAvroBytes(schema, bytes);
  }

  private GenericRecord decodeWithWriterSchema(Schema schema, byte[] bytes) throws IOException {
    Schema writer = writerSchema;
    Resolution resolution = RESOLUTIONS.get(writer, key -> Caffeine.newBuilder().weakKeys().maximumSize(64).build())
        .get(schema, reader -> Resolution.of(writer, reader));
    if (resolution.sameLayout) {
      return decodePositionally(schema, bytes);
    }
    return HoodieAvroUtils.bytesToAvro(bytes, resolution.renamedWriter != null ? resolution.renamedWriter : writer, schema);
  }

  private static final class Resolution {
    private final boolean sameLayout;
    // A copy of the writer schema whose fields carry the names of the reader fields they resolve to,
    // or null when no field is renamed. It shares no record schema with the writer schema.
    private final Schema renamedWriter;

    private Resolution(boolean sameLayout, Schema renamedWriter) {
      this.sameLayout = sameLayout;
      this.renamedWriter = renamedWriter;
    }

    private static Resolution of(Schema writer, Schema reader) {
      if (writer.equals(reader)) {
        return new Resolution(true, null);
      }
      WriterRenaming renaming = new WriterRenaming();
      Schema renamedWriter = renaming.copy(writer, writer, reader);
      return new Resolution(false, renaming.renamesField ? renamedWriter : null);
    }
  }

  /**
   * Copies the writer schema per pair of writer and reader schemas, so each record of a union branch or
   * a recursive type gets the field names of the reader record it resolves to.
   */
  private static final class WriterRenaming {
    private final Map<Schema, Map<Schema, Schema>> records = new IdentityHashMap<>();
    private boolean renamesField;

    private Schema copy(Schema writer, Schema enclosing, Schema reader) {
      if (writer.getType() == Schema.Type.UNION) {
        List<Schema> branches = new ArrayList<>(writer.getTypes().size());
        for (Schema branch : writer.getTypes()) {
          branches.add(copy(branch, writer, reader));
        }
        return Schema.createUnion(branches);
      }
      Schema target = matchingBranch(writer, enclosing, reader);
      switch (writer.getType()) {
        case RECORD:
          return copyRecord(writer, target);
        case ARRAY:
          return Schema.createArray(copy(writer.getElementType(), writer.getElementType(), target == null ? null : target.getElementType()));
        case MAP:
          return Schema.createMap(copy(writer.getValueType(), writer.getValueType(), target == null ? null : target.getValueType()));
        default:
          return writer;
      }
    }

    private Schema copyRecord(Schema writer, Schema reader) {
      Map<Schema, Schema> byReader = records.computeIfAbsent(writer, key -> new IdentityHashMap<>());
      Schema existing = byReader.get(reader);
      if (existing != null) {
        return existing;
      }
      Schema named = reader == null ? writer : reader;
      Schema copy = Schema.createRecord(named.getName(), null, named.getNamespace(), writer.isError());
      byReader.put(reader, copy);
      Map<String, Schema.Field> targets = reader == null ? Collections.emptyMap() : resolveFields(writer, reader);
      List<Schema.Field> fields = new ArrayList<>(writer.getFields().size());
      for (Schema.Field field : writer.getFields()) {
        Schema.Field target = targets.get(field.name());
        String name = field.name();
        if (target != null && !target.name().equals(name)) {
          name = target.name();
          renamesField = true;
        }
        fields.add(new Schema.Field(name, copy(field.schema(), field.schema(), target == null ? null : target.schema()), null, (Object) null));
      }
      copy.setFields(fields);
      return copy;
    }
  }

  private static Schema matchingBranch(Schema writer, Schema enclosing, Schema reader) {
    if (reader == null) {
      return null;
    }
    for (Schema branch : reader.getType() == Schema.Type.UNION ? reader.getTypes() : Collections.singletonList(reader)) {
      if (branch.getType() == writer.getType() && isSameRecordBranch(writer, branch, enclosing, reader)) {
        return branch;
      }
    }
    return null;
  }

  private static boolean isSameRecordBranch(Schema writerBranch, Schema readerBranch, Schema writer, Schema reader) {
    return writerBranch.getType() != Schema.Type.RECORD
        || writerBranch.getFullName().equals(readerBranch.getFullName())
        || readerBranch.getAliases().contains(writerBranch.getFullName())
        || (hasSingleRecordBranch(writer) && hasSingleRecordBranch(reader));
  }

  private static boolean hasSingleRecordBranch(Schema schema) {
    return schema.getType() != Schema.Type.UNION
        || schema.getTypes().stream().filter(branch -> branch.getType() == Schema.Type.RECORD).count() == 1;
  }

  /**
   * Maps each writer field name to the reader field that reads it: by name, then alias, then the
   * same-position rename.
   */
  private static Map<String, Schema.Field> resolveFields(Schema writer, Schema reader) {
    Map<String, Schema.Field> targets = new HashMap<>();
    for (Schema.Field field : reader.getFields()) {
      Option<Schema.Field> source = findByNameOrAlias(writer, field);
      if (!source.isPresent()) {
        source = findPositionalFallback(writer, reader, field);
      }
      if (source.isPresent()) {
        targets.putIfAbsent(source.get().name(), field);
      } else if (field.defaultVal() == null) {
        throw new AvroTypeException("Field '" + field.name() + "' of " + reader.getFullName() + " has no writer field or default");
      }
    }
    return targets;
  }

  private static Option<Schema.Field> findByNameOrAlias(Schema writer, Schema.Field field) {
    Schema.Field source = writer.getField(field.name());
    if (source != null) {
      return Option.of(source);
    }
    return Option.fromJavaOptional(field.aliases().stream().map(writer::getField).filter(Objects::nonNull).findFirst());
  }

  /**
   * Same-position rename: positional decoding carries a value across a rename, so an unmatched reader
   * field takes the writer field at its position when both records have the same number of fields, no
   * reader field claims that writer field by name or alias, and the types are compatible. With a
   * different number of fields an unmatched reader field gets its default, or fails when it has none.
   */
  private static Option<Schema.Field> findPositionalFallback(Schema writer, Schema reader, Schema.Field field) {
    if (writer.getFields().size() != reader.getFields().size()) {
      return Option.empty();
    }
    Schema.Field candidate = writer.getFields().get(field.pos());
    boolean claimed = reader.getFields().stream()
        .anyMatch(other -> other.name().equals(candidate.name()) || other.aliases().contains(candidate.name()));
    if (claimed || SchemaCompatibility.checkReaderWriterCompatibility(field.schema(), candidate.schema()).getType()
        != SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
      return Option.empty();
    }
    return Option.of(candidate);
  }

  /**
   * Encodes a record built in memory before Java serialization, which writes the bytes but not the
   * transient record.
   */
  protected Object writeReplace() throws ObjectStreamException {
    getRecordBytes();
    return this;
  }

  @Override
  public void write(Kryo kryo, Output output) {
    byte[] bytes = getRecordBytes();
    output.writeInt(bytes.length);
    output.writeBytes(bytes);
    kryo.writeClassAndObject(output, orderingVal);
    output.writeBoolean(isDeletedRecord);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int length = input.readInt();
    this.record = null;
    this.writerSchema = null;
    this.recordBytes = input.readBytes(length);
    this.orderingVal = (Comparable) kryo.readClassAndObject(input);
    this.isDeletedRecord = input.readBoolean();
  }
}
