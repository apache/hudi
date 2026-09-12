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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.Getter;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Base class for all AVRO record based payloads, that can be ordered based on a field.
 *
 * <p>Retains the writer schema across serialization and projections. Named fields and aliases take
 * precedence over the legacy same-position rename convention used by older realtime readers.
 */
public abstract class BaseAvroPayload implements Serializable, KryoSerializable {
  // Preserve Java serialization compatibility with payloads written before writer-schema retention.
  private static final long serialVersionUID = 4076216714695518773L;

  private static final String KRYO_WRITE_LEGACY_FORMAT = BaseAvroPayload.class.getName() + ".writeLegacyFormat";

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

  // The schema of recordBytes must remain independent of the last requested projection.
  private transient Schema writerSchema;

  private static final LoadingCache<Schema, String> SCHEMA_STRINGS = Caffeine.newBuilder()
      .maximumSize(1024).build(Schema::toString);
  private static final LoadingCache<String, Schema> PARSED_SCHEMAS = Caffeine.newBuilder()
      .maximumSize(1024).build(json -> new Schema.Parser().parse(json));

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
    if (writerSchema == null) {
      // Legacy serialized payloads did not include a writer schema.
      writerSchema = schema;
    }
    Map<String, String> renames = new HashMap<>();
    collectRenames(writerSchema, schema, "", renames, new HashSet<>());
    if (renames.isEmpty()) {
      record = HoodieAvroUtils.bytesToAvro(bytes, writerSchema, schema);
    } else {
      GenericRecord original = HoodieAvroUtils.bytesToAvro(bytes, writerSchema);
      record = HoodieAvroUtils.rewriteRecordWithNewSchema(original, schema, renames);
    }
    return Option.of(record);
  }

  private static void collectRenames(Schema writer, Schema reader, String prefix,
                                     Map<String, String> renames, Set<Pair<Schema, Schema>> visiting) {
    if (writer.equals(reader)) {
      return;
    }
    Pair<Schema, Schema> pair = Pair.of(writer, reader);
    if (!visiting.add(pair)) {
      return;
    }
    try {
      if (writer.getType() == Schema.Type.UNION || reader.getType() == Schema.Type.UNION) {
        for (Schema w : writer.getType() == Schema.Type.UNION ? writer.getTypes() : java.util.Collections.singletonList(writer)) {
          for (Schema r : reader.getType() == Schema.Type.UNION ? reader.getTypes() : java.util.Collections.singletonList(reader)) {
            boolean sameRecordBranch = w.getType() != Schema.Type.RECORD || r.getType() != Schema.Type.RECORD
                || w.getFullName().equals(r.getFullName()) || r.getAliases().contains(w.getFullName())
                || (writer.getType() != Schema.Type.UNION || writer.getTypes().stream().filter(s -> s.getType() == Schema.Type.RECORD).count() == 1)
                && (reader.getType() != Schema.Type.UNION || reader.getTypes().stream().filter(s -> s.getType() == Schema.Type.RECORD).count() == 1);
            if (w.getType() == r.getType() && sameRecordBranch) {
              collectRenames(w, r, prefix, renames, visiting);
            }
          }
        }
      } else if (writer.getType() == Schema.Type.RECORD && reader.getType() == Schema.Type.RECORD) {
        for (Schema.Field field : reader.getFields()) {
          Schema.Field source = writer.getField(field.name());
          if (source == null) {
            for (String alias : field.aliases()) {
              if (writer.getField(alias) != null) {
                source = writer.getField(alias);
                break;
              }
            }
          }
          // Preserve the historical rename contract only for a defaulted field replacing a removed
          // field at the same position. Added fields and named projections must never shift values.
          if (source == null && field.defaultVal() != null
              && writer.getFields().size() == reader.getFields().size()) {
            Schema.Field candidate = writer.getFields().get(field.pos());
            if (reader.getField(candidate.name()) == null) {
              source = candidate;
            }
          }
          if (source == null) {
            if (field.defaultVal() == null) {
              throw new AvroTypeException("Field '" + prefix + field.name() + "' has no writer field or default");
            }
            continue;
          }
          if (!source.name().equals(field.name())) {
            renames.put(prefix + field.name(), source.name());
          }
          collectRenames(source.schema(), field.schema(), prefix + field.name() + ".", renames, visiting);
        }
      } else if (writer.getType() == Schema.Type.ARRAY && reader.getType() == Schema.Type.ARRAY) {
        collectRenames(writer.getElementType(), reader.getElementType(), prefix + "element.", renames, visiting);
      } else if (writer.getType() == Schema.Type.MAP && reader.getType() == Schema.Type.MAP) {
        collectRenames(writer.getValueType(), reader.getValueType(), prefix + "value.", renames, visiting);
      }
    } finally {
      visiting.remove(pair);
    }
  }

  private void writeObject(ObjectOutputStream output) throws IOException {
    getRecordBytes();
    output.defaultWriteObject();
    output.writeObject(writerSchema == null ? null : SCHEMA_STRINGS.get(writerSchema));
  }

  private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException {
    input.defaultReadObject();
    try {
      String schemaJson = (String) input.readObject();
      writerSchema = schemaJson == null ? null : PARSED_SCHEMAS.get(schemaJson);
    } catch (OptionalDataException e) {
      if (!e.eof) {
        throw e;
      }
      // Java-serialized payloads from before schema retention contain only the default fields.
    }
  }

  /**
   * Selects the legacy payload format for a transport that supplies the writer schema separately.
   * Other Kryo instances retain a self-contained writer schema by default. The setting survives
   * graph resets and does not affect Java serialization or other Kryo instances.
   */
  public static void useLegacyKryoFormat(Kryo kryo) {
    kryo.getContext().put(KRYO_WRITE_LEGACY_FORMAT, Boolean.TRUE);
  }

  @Override
  public void write(Kryo kryo, Output output) {
    byte[] bytes = getRecordBytes();
    if (Boolean.TRUE.equals(kryo.getContext().get(KRYO_WRITE_LEGACY_FORMAT))) {
      output.writeInt(bytes.length);
    } else {
      // Negative lengths distinguish schema-bearing payloads from the legacy non-negative format.
      output.writeInt(-bytes.length - 1);
      output.writeString(writerSchema == null ? null : SCHEMA_STRINGS.get(writerSchema));
    }
    output.writeBytes(bytes);
    kryo.writeClassAndObject(output, orderingVal);
    output.writeBoolean(isDeletedRecord);
  }

  @Override
  public void read(Kryo kryo, Input input) {
    int length = input.readInt();
    this.record = null;
    this.writerSchema = null;
    if (length < 0) {
      length = -length - 1;
      String schemaJson = input.readString();
      this.writerSchema = schemaJson == null ? null : PARSED_SCHEMAS.get(schemaJson);
    }
    this.recordBytes = input.readBytes(length);
    this.orderingVal = (Comparable) kryo.readClassAndObject(input);
    this.isDeletedRecord = input.readBoolean();
  }
}
