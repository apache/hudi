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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Fixed;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * This is a GenericRecord payload generator that generates full generic records {@link GenericRecord}. Every field of a generic record created using this generator contains a random value.
 */
public class GenericRecordFullPayloadGenerator implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(GenericRecordFullPayloadGenerator.class);
  public static final int DEFAULT_PAYLOAD_SIZE = 1024 * 10; // 10 KB
  public static final int DEFAULT_NUM_DATE_PARTITIONS = 50;
  public static final String DEFAULT_HOODIE_IS_DELETED_COL = "_hoodie_is_deleted";
  public static final int DEFAULT_START_PARTITION = 0;

  protected final Random random = new Random();
  // The source schema used to generate a payload
  private final transient Schema baseSchema;
  // Used to validate a generic record
  private final transient GenericData genericData = new GenericData();
  // The index of partition for which records are being generated
  private int partitionIndex = 0;
  // The size of a full record where every field of a generic record created contains 1 random value
  private int estimatedFullPayloadSize;
  // Number of extra entries to add in a complex/collection field to achieve the desired record size
  Map<String, Integer> extraEntriesMap = new HashMap<>();
  // Start partition - default 0
  private int startPartition = DEFAULT_START_PARTITION;

  // The number of unique dates to create
  private int numDatePartitions = DEFAULT_NUM_DATE_PARTITIONS;
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

  public GenericRecordFullPayloadGenerator(Schema schema, int minPayloadSize,
                                           int numDatePartitions, int startPartition) {
    this(schema, minPayloadSize);
    this.numDatePartitions = numDatePartitions;
    this.startPartition = startPartition;
  }

  public GenericRecordFullPayloadGenerator(Schema schema, int minPayloadSize) {
    Pair<Integer, Integer> sizeInfo = new GenericRecordFullPayloadSizeEstimator(schema)
        .typeEstimateAndNumComplexFields();
    this.estimatedFullPayloadSize = sizeInfo.getLeft();
    this.baseSchema = schema;
    if (estimatedFullPayloadSize < minPayloadSize) {
      int numberOfComplexFields = sizeInfo.getRight();
      if (numberOfComplexFields < 1) {
        LOG.warn("The schema does not have any collections/complex fields. Cannot achieve minPayloadSize : {}",
            minPayloadSize);
      }
      determineExtraEntriesRequired(numberOfComplexFields, minPayloadSize - estimatedFullPayloadSize);
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

  /**
   * Create a new {@link GenericRecord} with random value according to given schema.
   *
   * @return {@link GenericRecord} with random value
   */
  public GenericRecord getNewPayload() {
    return getNewPayload(baseSchema);
  }

  protected GenericRecord getNewPayload(Schema schema) {
    return create(schema, null);
  }

  /**
   * Create a new {@link GenericRecord} with random value according to given schema.
   *
   * Long fields which are specified within partitionPathFieldNames are constrained to the value of the partition for which records are being generated.
   *
   * @return {@link GenericRecord} with random value
   */
  public GenericRecord getNewPayload(Set<String> partitionPathFieldNames) {
    return create(baseSchema, partitionPathFieldNames);
  }

  public GenericRecord getNewPayloadWithTimestamp(String tsFieldName) {
    return updateTimestamp(create(baseSchema, null), tsFieldName);
  }

  public GenericRecord getUpdatePayloadWithTimestamp(GenericRecord record, Set<String> blacklistFields,
                                                     String tsFieldName) {
    return updateTimestamp(randomize(record, blacklistFields), tsFieldName);
  }

  protected GenericRecord create(Schema schema, Set<String> partitionPathFieldNames) {
    GenericRecord result = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      if (f.name().equals(DEFAULT_HOODIE_IS_DELETED_COL)) {
        result.put(f.name(), false);
      } else {
        if (isPartialLongField(f, partitionPathFieldNames)) {
          // This is a long field used as partition field. Set it to seconds since epoch.
          long value = TimeUnit.SECONDS.convert(partitionIndex, TimeUnit.DAYS);
          result.put(f.name(), (long) value);
        } else {
          result.put(f.name(), typeConvert(f));
        }
      }
    }
    return result;
  }

  /**
   * Return true if this is a partition field of type long which should be set to the partition index.
   */
  private boolean isPartialLongField(Schema.Field field, Set<String> partitionPathFieldNames) {
    if ((partitionPathFieldNames == null) || !partitionPathFieldNames.contains(field.name())) {
      return false;
    }

    Schema fieldSchema = field.schema();
    if (isOption(fieldSchema)) {
      fieldSchema = getNonNull(fieldSchema);
    }

    return fieldSchema.getType() == org.apache.avro.Schema.Type.LONG;
  }

  /**
   * Update a given {@link GenericRecord} with random value. The fields in {@code blacklistFields} will not be updated.
   *
   * @param record GenericRecord to update
   * @param blacklistFields Fields whose value should not be touched
   * @return The updated {@link GenericRecord}
   */
  public GenericRecord getUpdatePayload(GenericRecord record, Set<String> blacklistFields) {
    return randomize(record, blacklistFields);
  }

  /**
   * Create a new {@link GenericRecord} with random values. Not all the fields have value, it is random, and its value is random too.
   *
   * @param schema Schema to create with.
   * @return A {@link GenericRecord} with random value.
   */
  protected GenericRecord convertPartial(Schema schema) {
    GenericRecord result = new GenericData.Record(schema);
    for (Schema.Field f : schema.getFields()) {
      if (f.name().equals(DEFAULT_HOODIE_IS_DELETED_COL)) {
        result.put(f.name(), false);
      } else {
        boolean setNull = random.nextBoolean();
        if (!setNull) {
          result.put(f.name(), typeConvert(f));
        } else {
          result.put(f.name(), null);
        }
      }
    }
    // TODO : pack remaining bytes into a complex field
    return result;
  }

  /**
   * Set random value to {@link GenericRecord} according to the schema type of field. The field in blacklist will not be set.
   *
   * @param record GenericRecord to randomize.
   * @param blacklistFields blacklistFields where the filed will not be randomized.
   * @return Randomized GenericRecord.
   */
  protected GenericRecord randomize(GenericRecord record, Set<String> blacklistFields) {
    for (Schema.Field f : record.getSchema().getFields()) {
      if (f.name().equals(DEFAULT_HOODIE_IS_DELETED_COL)) {
        record.put(f.name(), false);
      } else if (blacklistFields == null || !blacklistFields.contains(f.name())) {
        record.put(f.name(), typeConvert(f));
      }
    }
    return record;
  }

  /**
   * Set _hoodie_is_deleted column value to true.
   *
   * @param record GenericRecord to delete.
   * @return GenericRecord representing deleted record.
   */
  protected GenericRecord generateDeleteRecord(GenericRecord record) {
    record.put(DEFAULT_HOODIE_IS_DELETED_COL, true);
    return record;
  }

  /**
   * Generate random value according to their type.
   */
  private Object typeConvert(Schema.Field field) {
    Schema fieldSchema = field.schema();
    if (isOption(fieldSchema)) {
      fieldSchema = getNonNull(fieldSchema);
    }
    if (fieldSchema.getName().equals(DEFAULT_HOODIE_IS_DELETED_COL)) {
      return false;
    }

    switch (fieldSchema.getType()) {
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
        List<String> enumSymbols = fieldSchema.getEnumSymbols();
        return new GenericData.EnumSymbol(fieldSchema, enumSymbols.get(random.nextInt(enumSymbols.size() - 1)));
      case RECORD:
        return getNewPayload(fieldSchema);
      case ARRAY:
        Schema.Field elementField = new Schema.Field(field.name(), fieldSchema.getElementType(), "", null);
        List listRes = new ArrayList();
        int numEntriesToAdd = extraEntriesMap.getOrDefault(field.name(), 1);
        while (numEntriesToAdd-- > 0) {
          listRes.add(typeConvert(elementField));
        }
        return listRes;
      case MAP:
        Schema.Field valueField = new Schema.Field(field.name(), fieldSchema.getValueType(), "", null);
        Map<String, Object> mapRes = new HashMap<String, Object>();
        numEntriesToAdd = extraEntriesMap.getOrDefault(field.name(), 1);
        while (numEntriesToAdd > 0) {
          mapRes.put(UUID.randomUUID().toString(), typeConvert(valueField));
          numEntriesToAdd--;
        }
        return mapRes;
      case BYTES:
        return ByteBuffer.wrap(UUID.randomUUID().toString().getBytes(Charset.defaultCharset()));
      case FIXED:
        return generateFixedType(fieldSchema);
      default:
        throw new IllegalArgumentException("Cannot handle type: " + fieldSchema.getType());
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

  /**
   * Validate whether the record match schema.
   *
   * @param record Record to validate.
   * @return True if matches.
   */
  public boolean validate(GenericRecord record) {
    return genericData.validate(baseSchema, record);
  }

  /*
   * Generates a sequential timestamp (daily increment), and updates the timestamp field of the record.
   * Note: When generating records, number of records to be generated must be more than numDatePartitions * parallelism,
   * to guarantee that at least numDatePartitions are created.
   */
  @VisibleForTesting
  public GenericRecord updateTimestamp(GenericRecord record, String fieldName) {
    long delta = TimeUnit.SECONDS.convert((partitionIndex++ % numDatePartitions) + startPartition, TimeUnit.DAYS);
    record.put(fieldName, delta);
    return record;
  }

  /**
   * Check whether a schema is option. return true if it match the follows: 1. Its type is Type.UNION 2. Has two types 3. Has a NULL type.
   */
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

  /**
   * Method help to calculate the number of entries to add.
   *
   * @return Number of entries to add
   */
  private void determineExtraEntriesRequired(int numberOfComplexFields, int numberOfBytesToAdd) {
    for (Schema.Field f : baseSchema.getFields()) {
      Schema elementSchema = f.schema();
      // Find the size of the primitive data type in bytes
      int primitiveDataTypeSize = 0;
      if (elementSchema.getType() == Type.ARRAY && isPrimitive(elementSchema.getElementType())) {
        primitiveDataTypeSize = getSize(elementSchema.getElementType());
      } else if (elementSchema.getType() == Type.MAP && isPrimitive(elementSchema.getValueType())) {
        primitiveDataTypeSize = getSize(elementSchema.getValueType());
      } else {
        continue;
      }

      int numEntriesToAdd = numberOfBytesToAdd / primitiveDataTypeSize;
      // If more than 10 entries are being added for this same complex field and there are still more complex fields to
      // be visited in the schema, reduce the number of entries to add by a factor of 10 to allow for other complex
      // fields to pack some entries
      if (numEntriesToAdd > 10 && numberOfComplexFields > 1) {
        numEntriesToAdd = 10;
        numberOfBytesToAdd -= numEntriesToAdd * primitiveDataTypeSize;
      } else {
        numberOfBytesToAdd = 0;
      }

      extraEntriesMap.put(f.name(), numEntriesToAdd);

      numberOfComplexFields -= 1;
      if (numberOfBytesToAdd <= 0) {
        break;
      }
    }
  }
}

