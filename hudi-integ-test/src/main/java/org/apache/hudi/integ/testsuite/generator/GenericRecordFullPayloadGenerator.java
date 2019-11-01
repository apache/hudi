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

package org.apache.hudi.integ.testsuite.generator;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.collection.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a GenericRecord payload generator that generates full generic records {@link GenericRecord}.
 * Every field of a generic record created using this generator contains a random value.
 */
public class GenericRecordFullPayloadGenerator implements Serializable {

  public static final int DEFAULT_PAYLOAD_SIZE = 1024 * 10; // 10 KB
  private static Logger log = LoggerFactory.getLogger(GenericRecordFullPayloadGenerator.class);
  protected final Random random = new Random();
  // The source schema used to generate a payload
  private final transient Schema baseSchema;
  // Used to validate a generic record
  private final transient GenericData genericData = new GenericData();
  // Number of more bytes to add based on the estimated full record payload size and min payload size
  private int numberOfBytesToAdd;
  // If more elements should be packed to meet the minPayloadSize
  private boolean shouldAddMore;
  // How many complex fields have we visited that can help us pack more entries and increase the size of the record
  private int numberOfComplexFields;
  // The size of a full record where every field of a generic record created contains 1 random value
  private int estimatedFullPayloadSize;
  // LogicalTypes in Avro 1.8.2
  private static final String DECIMAL = "decimal";
  private static final String UUID_NAME = "uuid";
  private static final String DATE = "date";
  private static final String TIME_MILLIS = "time-millis";
  private static final String TIME_MICROS = "time-micros";
  private static final String TIMESTAMP_MILLIS = "timestamp-millis";
  private static final String TIMESTAMP_MICROS = "timestamp-micros";

  public GenericRecordFullPayloadGenerator(Schema schema) {
    this(schema, DEFAULT_PAYLOAD_SIZE);
  }

  public GenericRecordFullPayloadGenerator(Schema schema, int minPayloadSize) {
    Pair<Integer, Integer> sizeInfo = new GenericRecordFullPayloadSizeEstimator(schema)
        .typeEstimateAndNumComplexFields();
    this.estimatedFullPayloadSize = sizeInfo.getLeft();
    this.numberOfComplexFields = sizeInfo.getRight();
    this.baseSchema = schema;
    this.shouldAddMore = estimatedFullPayloadSize < minPayloadSize;
    if (this.shouldAddMore) {
      this.numberOfBytesToAdd = minPayloadSize - estimatedFullPayloadSize;
      if (numberOfComplexFields < 1) {
        log.warn("The schema does not have any collections/complex fields. Cannot achieve minPayloadSize : {}",
            minPayloadSize);
      }
    }
  }

  protected static boolean isPrimitive(Schema localSchema) {
    if (localSchema.getType() != Type.ARRAY
        && localSchema.getType() != Type.MAP
        && localSchema.getType() != Type.RECORD
        && localSchema.getType() != Type.UNION) {
      return true;
    } else {
      return false;
    }
  }

  public GenericRecord getNewPayload() {
    return convert(baseSchema);
  }

  public GenericRecord getUpdatePayload(GenericRecord record, List<String> blacklistFields) {
    return randomize(record, blacklistFields);
  }

  protected GenericRecord convert(Schema schema) {
    GenericRecord result = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      result.put(f.name(), typeConvert(f.schema()));
    }
    return result;
  }

  protected GenericRecord convertPartial(Schema schema) {
    GenericRecord result = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      boolean setNull = random.nextBoolean();
      if (!setNull) {
        result.put(f.name(), typeConvert(f.schema()));
      } else {
        result.put(f.name(), null);
      }
    }
    // TODO : pack remaining bytes into a complex field
    return result;
  }

  protected GenericRecord randomize(GenericRecord record, List<String> blacklistFields) {
    for (Schema.Field f : record.getSchema().getFields()) {
      if (blacklistFields == null || !blacklistFields.contains(f.name())) {
        record.put(f.name(), typeConvert(f.schema()));
      }
    }
    return record;
  }

  private Object typeConvert(Schema schema) {
    Schema localSchema = schema;
    if (isOption(schema)) {
      localSchema = getNonNull(schema);
    }
    switch (localSchema.getType()) {
      case BOOLEAN:
        return random.nextBoolean();
      case DOUBLE:
        return random.nextDouble();
      case FLOAT:
        return random.nextFloat();
      case INT:
        return random.nextInt();
      case LONG:
        return random.nextLong();
      case STRING:
        return UUID.randomUUID().toString();
      case ENUM:
        List<String> enumSymbols = localSchema.getEnumSymbols();
        return new GenericData.EnumSymbol(localSchema, enumSymbols.get(random.nextInt(enumSymbols.size() - 1)));
      case RECORD:
        return convert(localSchema);
      case ARRAY:
        Schema elementSchema = localSchema.getElementType();
        List listRes = new ArrayList();
        if (isPrimitive(elementSchema) && this.shouldAddMore) {
          int numEntriesToAdd = numEntriesToAdd(elementSchema);
          while (numEntriesToAdd > 0) {
            listRes.add(typeConvert(elementSchema));
            numEntriesToAdd--;
          }
        } else {
          listRes.add(typeConvert(elementSchema));
        }
        return listRes;
      case MAP:
        Schema valueSchema = localSchema.getValueType();
        Map<String, Object> mapRes = new HashMap<String, Object>();
        if (isPrimitive(valueSchema) && this.shouldAddMore) {
          int numEntriesToAdd = numEntriesToAdd(valueSchema);
          while (numEntriesToAdd > 0) {
            mapRes.put(UUID.randomUUID().toString(), typeConvert(valueSchema));
            numEntriesToAdd--;
          }
        } else {
          mapRes.put(UUID.randomUUID().toString(), typeConvert(valueSchema));
        }
        return mapRes;
      case BYTES:
        return ByteBuffer.wrap(UUID.randomUUID().toString().getBytes(Charset.defaultCharset()));
      case FIXED:
        return generateFixedType(localSchema);
      default:
        throw new IllegalArgumentException(
            "Cannot handle type: " + localSchema.getType());
    }
  }

  private Object generateFixedType(Schema localSchema) {
    // TODO: Need to implement valid data generation for fixed type
    GenericFixed genericFixed = new GenericData.Fixed(localSchema);
    switch (localSchema.getLogicalType().getName()) {
      case UUID_NAME:
        ((Fixed) genericFixed).bytes(UUID.randomUUID().toString().getBytes());
        return genericFixed;
      case DECIMAL:
        return genericFixed;
      case DATE:
        return genericFixed;
      case TIME_MILLIS:
        return genericFixed;
      default:
        throw new IllegalArgumentException(
            "Cannot handle type: " + localSchema.getLogicalType());
    }
  }

  public boolean validate(GenericRecord record) {
    return genericData.validate(baseSchema, record);
  }

  protected boolean isOption(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION)
        && schema.getTypes().size() == 2
        && (schema.getTypes().get(0).getType().equals(Schema.Type.NULL)
        || schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
  }

  protected Schema getNonNull(Schema schema) {
    List<Schema> types = schema.getTypes();
    return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }

  public int getEstimatedFullPayloadSize() {
    return estimatedFullPayloadSize;
  }

  private int getSize(Schema elementSchema) {
    switch (elementSchema.getType()) {
      case BOOLEAN:
        return 1;
      case DOUBLE:
        return Double.BYTES;
      case FLOAT:
        return Float.BYTES;
      case INT:
        return Integer.BYTES;
      case LONG:
        return Long.BYTES;
      case STRING:
        return UUID.randomUUID().toString().length();
      case ENUM:
        return 1;
      case BYTES:
        return UUID.randomUUID().toString().length();
      case FIXED:
        return elementSchema.getFixedSize();
      default:
        throw new RuntimeException("Unknown type " + elementSchema.getType());
    }
  }

  private int numEntriesToAdd(Schema elementSchema) {
    // Find the size of the primitive data type in bytes
    int primitiveDataTypeSize = getSize(elementSchema);
    int numEntriesToAdd = numberOfBytesToAdd / primitiveDataTypeSize;
    // If more than 10 entries are being added for this same complex field and there are still more complex fields to
    // be visited in the schema, reduce the number of entries to add by a factor of 10 to allow for other complex
    // fields to pack some entries
    if (numEntriesToAdd % 10 > 0 && this.numberOfComplexFields > 1) {
      numEntriesToAdd = numEntriesToAdd / 10;
      numberOfBytesToAdd -= numEntriesToAdd * primitiveDataTypeSize;
      this.shouldAddMore = true;
    } else {
      this.numberOfBytesToAdd = 0;
      this.shouldAddMore = false;
    }
    this.numberOfComplexFields -= 1;
    return numEntriesToAdd;
  }
}

