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

package org.apache.hudi.common.testutils.reader;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieDeleteRecord;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.model.HoodieRecordMerger.DEFAULT_MERGER_STRATEGY_UUID;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;

public class HoodieTestReaderContext extends HoodieReaderContext<IndexedRecord> {
  private Option<HoodieRecordMerger> customMerger;
  private Option<String> payloadClass;

  public HoodieTestReaderContext(
      Option<HoodieRecordMerger> customMerger,
      Option<String> payloadClass) {
    this.customMerger = customMerger;
    this.payloadClass = payloadClass;
  }

  @Override
  public ClosableIterator<IndexedRecord> getFileRecordIterator(
      StoragePath filePath,
      long start,
      long length,
      Schema dataSchema,
      Schema requiredSchema,
      HoodieStorage storage
  ) throws IOException {
    HoodieAvroFileReader reader = (HoodieAvroFileReader) HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO).getFileReader(new HoodieConfig(),
            filePath, HoodieFileFormat.PARQUET, Option.empty());
    return reader.getIndexedRecordIterator(dataSchema, requiredSchema);
  }

  @Override
  public IndexedRecord convertAvroRecord(IndexedRecord record) {
    return record;
  }

  @Override
  public HoodieRecordMerger getRecordMerger(String mergerStrategy) {
    // Utilize the custom merger if provided.
    if (customMerger.isPresent()) {
      return customMerger.get();
    }

    // Otherwise.
    switch (mergerStrategy) {
      case DEFAULT_MERGER_STRATEGY_UUID:
        return new HoodieAvroRecordMerger();
      default:
        throw new HoodieException(
            "The merger strategy UUID is not supported: " + mergerStrategy);
    }
  }

  @Override
  public Object getValue(IndexedRecord record, Schema schema, String fieldName) {
    return getFieldValueFromIndexedRecord(record, schema, fieldName);
  }

  @Override
  public String getRecordKey(IndexedRecord record, Schema schema) {
    return getFieldValueFromIndexedRecord(record, schema, ROW_KEY).toString();
  }

  @Override
  public HoodieRecord constructHoodieRecord(
      Option<IndexedRecord> recordOpt,
      Map<String, Object> metadataMap
  ) {
    String appliedPayloadClass =
        payloadClass.isPresent()
            ? payloadClass.get()
            : DefaultHoodieRecordPayload.class.getName();
    if (!recordOpt.isPresent()) {
      return SpillableMapUtils.generateEmptyPayload(
          (String) metadataMap.get(INTERNAL_META_RECORD_KEY),
          (String) metadataMap.get(INTERNAL_META_PARTITION_PATH),
          (Comparable<?>) metadataMap.get(INTERNAL_META_ORDERING_FIELD),
          appliedPayloadClass);
    }
    return new HoodieAvroIndexedRecord(recordOpt.get());
  }

  @Override
  public IndexedRecord seal(IndexedRecord record) {
    Schema schema = record.getSchema();
    GenericRecordBuilder builder = new GenericRecordBuilder(schema);
    for (Schema.Field field : schema.getFields()) {
      builder.set(field, record.get(field.pos()));
    }
    return builder.build();
  }

  @Override
  public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator,
                                                               Schema skeletonRequiredSchema,
                                                               ClosableIterator<IndexedRecord> dataFileIterator,
                                                               Schema dataRequiredSchema) {
    return null;
  }

  @Override
  public UnaryOperator<IndexedRecord> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    if (!renamedColumns.isEmpty()) {
      throw new UnsupportedOperationException("Schema evolution is not supported for the test reader context");
    }
    Map<String, Integer> fromFields = IntStream.range(0, from.getFields().size())
        .boxed()
        .collect(Collectors.toMap(
            i -> from.getFields().get(i).name(), i -> i));
    Map<String, Integer> toFields = IntStream.range(0, to.getFields().size())
        .boxed()
        .collect(Collectors.toMap(
            i -> to.getFields().get(i).name(), i -> i));

    // Check if source schema contains all fields from target schema.
    List<Schema.Field> missingFields = to.getFields().stream()
        .filter(f -> !fromFields.containsKey(f.name())).collect(Collectors.toList());
    if (!missingFields.isEmpty()) {
      throw new HoodieException("There are some fields missing in source schema: "
          + missingFields);
    }

    // Build the mapping from source schema to target schema.
    Map<Integer, Integer> fieldMap = toFields.entrySet().stream()
        .filter(e -> fromFields.containsKey(e.getKey()))
        .collect(Collectors.toMap(
            e -> fromFields.get(e.getKey()), Map.Entry::getValue));

    // Do the transformation.
    return record -> {
      IndexedRecord outputRecord = new GenericData.Record(to);
      for (int i = 0; i < from.getFields().size(); i++) {
        if (!fieldMap.containsKey(i)) {
          continue;
        }
        int j = fieldMap.get(i);
        outputRecord.put(j, record.get(i));
      }
      return outputRecord;
    };
  }

  @Override
  public Comparable castValue(Comparable value, Schema.Type newType) {
    Schema newSchema = Schema.create(newType);
    Schema oldType;
    if (value instanceof Integer) {
      oldType = Schema.create(Schema.Type.INT);
    } else if (value instanceof Long) {
      oldType = Schema.create(Schema.Type.LONG);
    } else if (value instanceof Float) {
      oldType = Schema.create(Schema.Type.FLOAT);
    } else if (value instanceof Double) {
      oldType = Schema.create(Schema.Type.DOUBLE);
    } else if (value instanceof String) {
      oldType = Schema.create(Schema.Type.STRING);
    } else {
      throw new UnsupportedOperationException("Cast from " + value.getClass() + " to " + newType + " is not supported");
    }
    return (Comparable) HoodieAvroUtils.rewritePrimaryType(value, oldType, newSchema);
  }

  @Override
  public IndexedRecord constructRawDeleteRecord(Map<String, Object> metadata) {
    return new HoodieDeleteRecord(
        (String) metadata.get(INTERNAL_META_RECORD_KEY),
        (String) metadata.get(INTERNAL_META_PARTITION_PATH),
        metadata.get(INTERNAL_META_ORDERING_FIELD));
  }

  private Object getFieldValueFromIndexedRecord(
      IndexedRecord record,
      Schema recordSchema,
      String fieldName
  ) {
    Schema.Field field = recordSchema.getField(fieldName);
    int pos = field.pos();
    return record.get(pos);
  }
}
