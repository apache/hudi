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

package org.apache.hudi.avro;

import org.apache.hudi.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.VariantShreddingInferenceFileWriter.VariantSampleExtractor;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Extracts variant binaries from Avro-backed ({@code HoodieRecordType.AVRO}) records, where an
 * unshredded variant column materializes as a {@code {metadata: bytes, value: bytes}} record.
 */
public class AvroVariantSampleExtractor implements VariantSampleExtractor {

  private final List<String> columnNames;

  public AvroVariantSampleExtractor(List<String> columnNames) {
    this.columnNames = columnNames;
  }

  @Override
  public VariantSample[] extract(HoodieRecord record, HoodieSchema schema, Properties props) throws IOException {
    VariantSample[] out = new VariantSample[columnNames.size()];
    Option<HoodieAvroIndexedRecord> indexed = record.toIndexedRecord(schema, props);
    if (!indexed.isPresent()) {
      // Delete payload: no data to sample.
      return out;
    }
    IndexedRecord data = indexed.get().getData();
    Schema recordSchema = data.getSchema();
    for (int i = 0; i < columnNames.size(); i++) {
      // Lookup by name on the record's own schema: per-call schemas can differ from the
      // writer schema (e.g. merge handles passing the table schema variant).
      Schema.Field field = recordSchema.getField(columnNames.get(i));
      if (field == null) {
        continue;
      }
      Object value = data.get(field.pos());
      if (value instanceof GenericRecord) {
        GenericRecord variant = (GenericRecord) value;
        byte[] valueBytes = copyBytes(variant, HoodieSchema.Variant.VARIANT_VALUE_FIELD);
        byte[] metadataBytes = copyBytes(variant, HoodieSchema.Variant.VARIANT_METADATA_FIELD);
        if (valueBytes != null && metadataBytes != null) {
          out[i] = new VariantSample(valueBytes, metadataBytes);
        }
      }
    }
    return out;
  }

  private static byte[] copyBytes(GenericRecord variant, String fieldName) {
    Schema.Field field = variant.getSchema().getField(fieldName);
    if (field == null) {
      return null;
    }
    Object value = variant.get(field.pos());
    if (value instanceof ByteBuffer) {
      ByteBuffer buffer = ((ByteBuffer) value).duplicate();
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return bytes;
    }
    if (value instanceof byte[]) {
      byte[] bytes = (byte[]) value;
      return Arrays.copyOf(bytes, bytes.length);
    }
    return null;
  }
}
