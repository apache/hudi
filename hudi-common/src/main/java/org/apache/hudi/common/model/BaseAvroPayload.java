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
import org.apache.hudi.exception.HoodieIOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.Getter;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Base class for all AVRO record based payloads, that can be ordered based on a field.
 *
 * <p>Retains the writer schema across serialization and projections. Named fields and aliases take
 * precedence over the legacy same-position rename convention used by older realtime readers.
 */
public abstract class BaseAvroPayload implements Serializable, KryoSerializable {
  // Preserve Java serialization compatibility with payloads written before writer-schema retention.
  private static final long serialVersionUID = 4076216714695518773L;

  private static final String COMPRESSED_SCHEMA_PREFIX = "gzip:";

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
  private static final LoadingCache<Schema, String> KRYO_SCHEMA_STRINGS = Caffeine.newBuilder()
      .maximumSize(1024).build(BaseAvroPayload::encodeKryoSchema);
  private static final LoadingCache<String, Schema> PARSED_SCHEMAS = Caffeine.newBuilder()
      .maximumSize(1024).build(json -> new Schema.Parser().parse(decodeKryoSchema(json)));

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
      // Legacy bytes have no schema. Preserve positional decoding until a supplied schema
      // consumes the complete record; a prefix projection must not become the writer schema.
      BinaryDecoder decoder = HoodieAvroUtils.getBinaryDecoder(bytes, 0, bytes.length);
      record = new GenericDatumReader<GenericRecord>(schema).read(null, decoder);
      if (decoder.isEnd()) {
        writerSchema = schema;
      }
      return Option.of(record);
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
        collectUnionRenames(writer, reader, prefix, renames, visiting);
      } else if (writer.getType() == Schema.Type.RECORD && reader.getType() == Schema.Type.RECORD) {
        collectRecordRenames(writer, reader, prefix, renames, visiting);
      } else if (writer.getType() == Schema.Type.ARRAY && reader.getType() == Schema.Type.ARRAY) {
        collectRenames(writer.getElementType(), reader.getElementType(), prefix + "element.", renames, visiting);
      } else if (writer.getType() == Schema.Type.MAP && reader.getType() == Schema.Type.MAP) {
        collectRenames(writer.getValueType(), reader.getValueType(), prefix + "value.", renames, visiting);
      }
    } finally {
      visiting.remove(pair);
    }
  }

  private static void collectUnionRenames(Schema writer, Schema reader, String prefix,
                                          Map<String, String> renames, Set<Pair<Schema, Schema>> visiting) {
    for (Schema writerBranch : writer.getType() == Schema.Type.UNION ? writer.getTypes() : Collections.singletonList(writer)) {
      for (Schema readerBranch : reader.getType() == Schema.Type.UNION ? reader.getTypes() : Collections.singletonList(reader)) {
        if (writerBranch.getType() == readerBranch.getType() && isSameRecordBranch(writerBranch, readerBranch, writer, reader)) {
          collectRenames(writerBranch, readerBranch, prefix, renames, visiting);
        }
      }
    }
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

  private static void collectRecordRenames(Schema writer, Schema reader, String prefix,
                                           Map<String, String> renames, Set<Pair<Schema, Schema>> visiting) {
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
      // Keep the historical same-position rename only for compatible, unclaimed fields.
      // An incompatible drop/add must use its default; aliases must not duplicate source values.
      if (source == null && field.defaultVal() != null
          && writer.getFields().size() == reader.getFields().size()) {
        Schema.Field candidate = writer.getFields().get(field.pos());
        boolean claimed = reader.getFields().stream()
            .anyMatch(other -> other.name().equals(candidate.name()) || other.aliases().contains(candidate.name()));
        if (!claimed && SchemaCompatibility.checkReaderWriterCompatibility(field.schema(), candidate.schema()).getType()
            == SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE) {
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
  }

  // A spill entry must be readable without earlier entries or another thread's Kryo cache.
  // Compress once per cached schema instead of replacing the schema with a process-local ID.
  private static String encodeKryoSchema(Schema schema) {
    String json = SCHEMA_STRINGS.get(schema);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(bytes)) {
      gzip.write(json.getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new HoodieIOException("Failed to compress payload writer schema", e);
    }
    String compressed = COMPRESSED_SCHEMA_PREFIX + Base64.getEncoder().encodeToString(bytes.toByteArray());
    return compressed.length() < json.length() ? compressed : json;
  }

  private static String decodeKryoSchema(String encoded) {
    if (!encoded.startsWith(COMPRESSED_SCHEMA_PREFIX)) {
      return encoded;
    }
    byte[] compressed = Base64.getDecoder().decode(encoded.substring(COMPRESSED_SCHEMA_PREFIX.length()));
    try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(compressed));
         ByteArrayOutputStream bytes = new ByteArrayOutputStream()) {
      byte[] buffer = new byte[4096];
      int length;
      while ((length = gzip.read(buffer)) != -1) {
        bytes.write(buffer, 0, length);
      }
      return new String(bytes.toByteArray(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to decompress payload writer schema", e);
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
      // The schema string is either JSON or gzip: followed by Base64-encoded compressed JSON.
      output.writeInt(-bytes.length - 1);
      output.writeString(writerSchema == null ? null : KRYO_SCHEMA_STRINGS.get(writerSchema));
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
