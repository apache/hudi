/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.table.log.block;

import org.apache.hudi.common.util.Option;

import org.apache.avro.AvroTypeException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;

import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads records written in a writer schema as records of a reader schema that keeps a subset of the writer's fields,
 * each with the writer's own field schema. The fields the reader drops are skipped by a plan compiled once from the
 * writer schema, without the per-field {@code GenericDatumReader.skip} calls of Avro's resolving reader. The records
 * read are equal to the ones {@code new GenericDatumReader<>(writerSchema, readerSchema, data)} reads.
 */
final class AvroProjectionReader {

  /**
   * Reads one value of a schema from a decoder, or skips it when the value is not kept.
   */
  @FunctionalInterface
  private interface FieldReader {
    Object read(Decoder decoder) throws IOException;
  }

  private final GenericData data;
  private final Schema readerSchema;
  private final FieldReader[] writerFields;
  // Reader position of each writer field, or -1 for a field the reader drops
  private final int[] readerPositions;

  private AvroProjectionReader(GenericData data, Schema readerSchema, FieldReader[] writerFields, int[] readerPositions) {
    this.data = data;
    this.readerSchema = readerSchema;
    this.writerFields = writerFields;
    this.readerPositions = readerPositions;
  }

  static Option<AvroProjectionReader> create(Schema writerSchema, Schema readerSchema) {
    return create(writerSchema, readerSchema, GenericData.get());
  }

  /**
   * A reader of {@code readerSchema} records from {@code writerSchema} records in the {@code data} model, when the
   * reader schema is a strict subset of the writer's record fields, each with an identical field schema and no aliases,
   * and no dropped field has a recursive type; empty otherwise, when Avro's schema resolution must read the records.
   */
  static Option<AvroProjectionReader> create(Schema writerSchema, Schema readerSchema, GenericData data) {
    if (writerSchema.getType() != Schema.Type.RECORD || readerSchema.getType() != Schema.Type.RECORD
        || readerSchema.getFields().size() >= writerSchema.getFields().size()) {
      return Option.empty();
    }
    for (Schema.Field readerField : readerSchema.getFields()) {
      Schema.Field writerField = writerSchema.getField(readerField.name());
      if (writerField == null || !readerField.aliases().isEmpty() || !writerField.schema().equals(readerField.schema())) {
        return Option.empty();
      }
    }
    List<Schema.Field> fields = writerSchema.getFields();
    FieldReader[] writerFields = new FieldReader[fields.size()];
    int[] readerPositions = new int[fields.size()];
    Map<Schema, Boolean> visiting = new IdentityHashMap<>();
    visiting.put(writerSchema, Boolean.TRUE);
    for (int i = 0; i < fields.size(); i++) {
      Schema.Field readerField = readerSchema.getField(fields.get(i).name());
      if (readerField == null) {
        writerFields[i] = skipper(fields.get(i).schema(), visiting);
        if (writerFields[i] == null) {
          return Option.empty();
        }
        readerPositions[i] = -1;
      } else {
        writerFields[i] = reader(readerField.schema(), data);
        readerPositions[i] = readerField.pos();
      }
    }
    return Option.of(new AvroProjectionReader(data, readerSchema, writerFields, readerPositions));
  }

  IndexedRecord read(Decoder decoder) throws IOException {
    IndexedRecord record = (IndexedRecord) data.newRecord(null, readerSchema);
    for (int i = 0; i < writerFields.length; i++) {
      Object value = writerFields[i].read(decoder);
      if (readerPositions[i] >= 0) {
        record.put(readerPositions[i], value);
      }
    }
    return record;
  }

  /**
   * Reads a kept value as {@code GenericDatumReader} does. Values with a logical type conversion registered in the
   * model, strings of a non-default string type, records, arrays and maps are read by Avro's reader itself.
   */
  private static FieldReader reader(Schema schema, GenericData data) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType != null && data.getConversionFor(logicalType) != null) {
      return avroReader(schema, data);
    }
    switch (schema.getType()) {
      case NULL:
        return decoder -> {
          decoder.readNull();
          return null;
        };
      case BOOLEAN:
        return Decoder::readBoolean;
      case INT:
        return Decoder::readInt;
      case LONG:
        return Decoder::readLong;
      case FLOAT:
        return Decoder::readFloat;
      case DOUBLE:
        return Decoder::readDouble;
      case STRING: {
        String stringType = schema.getProp(GenericData.STRING_PROP);
        if (stringType == null) {
          return decoder -> decoder.readString(null);
        }
        return GenericData.StringType.String.name().equals(stringType) ? Decoder::readString : avroReader(schema, data);
      }
      case BYTES:
        return decoder -> decoder.readBytes(null);
      case FIXED: {
        int size = schema.getFixedSize();
        return decoder -> {
          GenericFixed fixed = (GenericFixed) data.createFixed(null, schema);
          decoder.readFixed(fixed.bytes(), 0, size);
          return fixed;
        };
      }
      case ENUM: {
        List<String> symbols = schema.getEnumSymbols();
        return decoder -> data.createEnum(symbols.get(decoder.readEnum()), schema);
      }
      case UNION: {
        List<Schema> types = schema.getTypes();
        FieldReader[] branches = new FieldReader[types.size()];
        for (int i = 0; i < branches.length; i++) {
          branches[i] = reader(types.get(i), data);
        }
        return decoder -> branches[decoder.readIndex()].read(decoder);
      }
      default:
        return avroReader(schema, data);
    }
  }

  private static FieldReader avroReader(Schema schema, GenericData data) {
    GenericDatumReader<Object> reader = new GenericDatumReader<>(schema, schema, data);
    return decoder -> reader.read(null, decoder);
  }

  /**
   * Skips a dropped value as {@code GenericDatumReader.skip} does; null for a recursive schema.
   */
  private static FieldReader skipper(Schema schema, Map<Schema, Boolean> visiting) {
    switch (schema.getType()) {
      case NULL:
        return decoder -> {
          decoder.readNull();
          return null;
        };
      case BOOLEAN:
        return decoder -> {
          decoder.readBoolean();
          return null;
        };
      case INT:
        return decoder -> {
          decoder.readInt();
          return null;
        };
      case LONG:
        return decoder -> {
          decoder.readLong();
          return null;
        };
      case FLOAT:
        return decoder -> {
          decoder.skipFixed(Float.BYTES);
          return null;
        };
      case DOUBLE:
        return decoder -> {
          decoder.skipFixed(Double.BYTES);
          return null;
        };
      case STRING:
        return decoder -> {
          decoder.skipString();
          return null;
        };
      case BYTES:
        return decoder -> {
          decoder.skipBytes();
          return null;
        };
      case FIXED: {
        int size = schema.getFixedSize();
        return decoder -> {
          decoder.skipFixed(size);
          return null;
        };
      }
      case ENUM: {
        int symbols = schema.getEnumSymbols().size();
        return decoder -> {
          int index = decoder.readEnum();
          if (index < 0 || index >= symbols) {
            throw new AvroTypeException("Enumeration out of range: must be in [0, " + symbols + "), but received " + index);
          }
          return null;
        };
      }
      case UNION: {
        List<Schema> types = schema.getTypes();
        FieldReader[] branches = new FieldReader[types.size()];
        for (int i = 0; i < branches.length; i++) {
          branches[i] = skipper(types.get(i), visiting);
          if (branches[i] == null) {
            return null;
          }
        }
        return decoder -> branches[decoder.readIndex()].read(decoder);
      }
      case RECORD: {
        if (visiting.put(schema, Boolean.TRUE) != null) {
          return null;
        }
        List<Schema.Field> fields = schema.getFields();
        FieldReader[] skippers = new FieldReader[fields.size()];
        for (int i = 0; i < skippers.length; i++) {
          skippers[i] = skipper(fields.get(i).schema(), visiting);
          if (skippers[i] == null) {
            return null;
          }
        }
        visiting.remove(schema);
        return decoder -> {
          for (FieldReader skipper : skippers) {
            skipper.read(decoder);
          }
          return null;
        };
      }
      case ARRAY: {
        FieldReader element = skipper(schema.getElementType(), visiting);
        if (element == null) {
          return null;
        }
        return decoder -> {
          // skipArray skips any block written with its byte size whole and returns the item count of the next block, 0 at the end
          for (long items = decoder.skipArray(); items != 0; items = decoder.skipArray()) {
            for (long item = 0; item < items; item++) {
              element.read(decoder);
            }
          }
          return null;
        };
      }
      case MAP: {
        FieldReader value = skipper(schema.getValueType(), visiting);
        if (value == null) {
          return null;
        }
        return decoder -> {
          for (long entries = decoder.skipMap(); entries != 0; entries = decoder.skipMap()) {
            for (long entry = 0; entry < entries; entry++) {
              decoder.skipString();
              value.read(decoder);
            }
          }
          return null;
        };
      }
      default:
        return null;
    }
  }
}
