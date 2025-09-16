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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.ObjectSizeCalculator;
import org.apache.hudi.common.util.SizeEstimator;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of {@link SizeEstimator} for Avro {@link BufferedRecord}, which estimates the size of
 * Avro record excluding the internal {@link Schema}.
 */
public class AvroRecordSizeEstimator implements SizeEstimator<BufferedRecord<IndexedRecord>> {
  private final long sizeOfSchema;
  private final long sizeOfSchemaWithoutMeta;

  public AvroRecordSizeEstimator(Schema recordSchema) {
    sizeOfSchema = ObjectSizeCalculator.getObjectSize(recordSchema);
    if (recordSchema.getFields().get(0).name().equals(HoodieRecord.COMMIT_TIME_METADATA_FIELD)) {
      sizeOfSchemaWithoutMeta = ObjectSizeCalculator.getObjectSize(schemaWithoutMeta(recordSchema));
    } else {
      sizeOfSchemaWithoutMeta = sizeOfSchema;
    }
  }

  private Schema schemaWithoutMeta(Schema originalSchema) {
    List<Schema.Field> originalFields = originalSchema.getFields();
    List<Schema.Field> remainingFields = new ArrayList<>();
    int startIndex = originalSchema.getFields().get(5).name().equals(HoodieRecord.OPERATION_METADATA_FIELD) ? 6 : 5;
    for (int i = startIndex; i < originalFields.size(); i++) {
      Schema.Field originalField = originalFields.get(i);
      Schema.Field copiedField = new Schema.Field(
          originalField.name(),
          originalField.schema(),
          originalField.doc(),
          originalField.defaultVal(),
          originalField.order()
      );
      remainingFields.add(copiedField);
    }
    Schema newSchema = Schema.createRecord(
        originalSchema.getName() + "_Subset",
        originalSchema.getDoc(),
        originalSchema.getNamespace(),
        false
    );
    newSchema.setFields(remainingFields);
    return newSchema;
  }

  @Override
  public long sizeEstimate(BufferedRecord<IndexedRecord> record) {
    long sizeOfRecord = ObjectSizeCalculator.getObjectSize(record);
    // generated record do not contain Schema field, so do not need minus size of Schema.
    if (record.getRecord() instanceof SpecificRecord) {
      return sizeOfRecord;
    }
    // do not contain size of Avro schema as the schema is reused among records
    long toReturn =  sizeOfRecord - sizeOfSchema + 8;
    if (toReturn < 0) {
       return sizeOfRecord - sizeOfSchemaWithoutMeta + 8;
    } else {
      return toReturn;
    }
  }
}
