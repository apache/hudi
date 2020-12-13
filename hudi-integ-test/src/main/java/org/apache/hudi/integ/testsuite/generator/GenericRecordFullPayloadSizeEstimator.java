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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.collection.Pair;

/**
 * This is a GenericRecord payload estimator estimates the size of a full generic record {@link GenericRecord}.
 * A full record is defined as "Every field of a generic record created contains 1 random value"
 */
public class GenericRecordFullPayloadSizeEstimator implements Serializable {

  private final transient Schema baseSchema;

  // This variable is used to track the number of complex/collection fields with primitive data types at their leaf.
  // This is used to figure out how many entries can be packed in such a collection field to meet the min payload
  // size requested
  private final transient AtomicInteger counter = new AtomicInteger(0);

  public GenericRecordFullPayloadSizeEstimator(Schema schema) {
    this.baseSchema = schema;
  }

  public Pair<Integer, Integer> typeEstimateAndNumComplexFields() {
    int size = estimate(baseSchema);
    return Pair.of(size, counter.get());
  }

  /**
   * This method estimates the size of the payload if all entries of this payload were populated with one value.
   * For eg. A primitive data type such as String will be populated with {@link UUID} so the length if 36 bytes
   * whereas a complex data type such as an Array of type Int, will be populated with exactly 1 Integer value.
   */
  protected int estimate(Schema schema) {
    long size = 0;
    for (Schema.Field f : schema.getFields()) {
      size += typeEstimate(f.schema());
    }
    return (int) size;
  }

  /**
   * Estimate the size of a given schema according to their type.
   *
   * @param schema schema to estimate.
   * @return Size of the given schema.
   */
  private long typeEstimate(Schema schema) {
    Schema localSchema = schema;
    if (isOption(schema)) {
      localSchema = getNonNull(schema);
    }
    switch (localSchema.getType()) {
      case BOOLEAN:
        return 1;
      case DOUBLE:
        return 8;
      case FLOAT:
        return 4;
      case INT:
        return 4;
      case LONG:
        return 8;
      case STRING:
        return UUID.randomUUID().toString().length();
      case ENUM:
        return 1;
      case RECORD:
        return estimate(localSchema);
      case ARRAY:
        if (GenericRecordFullPayloadGenerator.isPrimitive(localSchema.getElementType())) {
          counter.addAndGet(1);
        }
        Schema elementSchema = localSchema.getElementType();
        return typeEstimate(elementSchema);
      case MAP:
        if (GenericRecordFullPayloadGenerator.isPrimitive(localSchema.getValueType())) {
          counter.addAndGet(1);
        }
        Schema valueSchema = localSchema.getValueType();
        return UUID.randomUUID().toString().length() + typeEstimate(valueSchema);
      case BYTES:
        return UUID.randomUUID().toString().length();
      case FIXED:
        return localSchema.getFixedSize();
      default:
        throw new IllegalArgumentException(
            "Cannot handle type: " + localSchema.getType());
    }
  }

  protected boolean isOption(Schema schema) {
    return schema.getType().equals(Schema.Type.UNION)
        && schema.getTypes().size() == 2
        && (schema.getTypes().get(0).getType().equals(Schema.Type.NULL)
        || schema.getTypes().get(1).getType().equals(Schema.Type.NULL));
  }

  /**
   * Get the nonNull Schema of a given UNION Schema.
   *
   * @param schema
   * @return
   */
  protected Schema getNonNull(Schema schema) {
    List<Schema> types = schema.getTypes();
    return types.get(0).getType().equals(Schema.Type.NULL) ? types.get(1) : types.get(0);
  }

}

