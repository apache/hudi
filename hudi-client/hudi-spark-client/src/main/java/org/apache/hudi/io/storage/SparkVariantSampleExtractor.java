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

package org.apache.hudi.io.storage;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.io.storage.VariantShreddingInferenceFileWriter.VariantSampleExtractor;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Properties;

/**
 * Extracts variant binaries from {@code HoodieRecordType.SPARK} records, whose data is an
 * {@link InternalRow} aligned to the writer {@link StructType} (the same contract the wrapped
 * file writer relies on).
 */
public class SparkVariantSampleExtractor implements VariantSampleExtractor {

  private final int[] ordinals;

  public SparkVariantSampleExtractor(List<String> columnNames, StructType structType) {
    this.ordinals = new int[columnNames.size()];
    for (int i = 0; i < columnNames.size(); i++) {
      this.ordinals[i] = fieldIndexOf(structType, columnNames.get(i));
    }
  }

  @Override
  public VariantSample[] extract(HoodieRecord record, HoodieSchema schema, Properties props) {
    VariantSample[] out = new VariantSample[ordinals.length];
    Object data = record.getData();
    if (!(data instanceof InternalRow)) {
      // Delete payload: no data to sample.
      return out;
    }
    InternalRow row = (InternalRow) data;
    for (int i = 0; i < ordinals.length; i++) {
      if (ordinals[i] >= 0) {
        out[i] = SparkAdapterSupport$.MODULE$.sparkAdapter().extractVariantBinary(row, ordinals[i]);
      }
    }
    return out;
  }

  private static int fieldIndexOf(StructType structType, String name) {
    StructField[] fields = structType.fields();
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].name().equals(name)) {
        return i;
      }
    }
    return -1;
  }
}
