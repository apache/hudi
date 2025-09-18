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

package org.apache.hudi.avro;

import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.common.util.SizeEstimator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link SizeEstimator} for Avro {@link BufferedRecord}, which estimates the size of
 * Avro record excluding the internal {@link Schema}.
 */
public class AvroRecordSizeEstimator implements SizeEstimator<BufferedRecord<IndexedRecord>> {
  private final Map<Integer, Long> sizeOfSchemaMap;

  public AvroRecordSizeEstimator(Schema recordSchema) {
    this.sizeOfSchemaMap = new HashMap<>();
    this.sizeOfSchemaMap.put(recordSchema.getFields().size(), ObjectSizeCalculator.getObjectSize(recordSchema));
  }

  @Override
  public long sizeEstimate(BufferedRecord<IndexedRecord> record) {
    long sizeOfRecord = ObjectSizeCalculator.getObjectSize(record);
    // generated record do not contain Schema field, so do not need minus size of Schema.
    if (record.getRecord() instanceof SpecificRecord) {
      return sizeOfRecord;
    }
    // do not contain size of Avro schema as the schema is reused among records
    Schema recordSchema = record.getRecord().getSchema();
    long sizeOfSchema = sizeOfSchemaMap.computeIfAbsent(recordSchema.getFields().size(), arity -> ObjectSizeCalculator.getObjectSize(recordSchema));
    return sizeOfRecord - sizeOfSchema + 8;
  }
}
