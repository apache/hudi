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

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.OverwriteWithLatestMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.storage.HoodieAvroFileReader;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * An implementation of {@link HoodieReaderContext} that reads data from the base files as {@link IndexedRecord}.
 * This implementation does not rely on a specific engine and can be used in any JVM environment as a result.
 */
public class HoodieAvroReaderContext extends HoodieReaderContext<IndexedRecord> {
  private final String payloadClass;

  public HoodieAvroReaderContext(
      StorageConfiguration<?> storageConfiguration,
      HoodieTableConfig tableConfig) {
    super(storageConfiguration, tableConfig);
    this.payloadClass = tableConfig.getPayloadClass();
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
  public GenericRecord convertToAvroRecord(IndexedRecord record, Schema schema) {
    return (GenericRecord) record;
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(new HoodieAvroRecordMerger());
      case COMMIT_TIME_ORDERING:
        return Option.of(new OverwriteWithLatestMerger());
      case CUSTOM:
      default:
        Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.JAVA, mergeImplClasses, mergeStrategyId);
        if (recordMerger.isEmpty()) {
          throw new IllegalArgumentException("No valid merger implementation set for `"
              + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
        }
        return recordMerger;
    }
  }

  @Override
  public Object getValue(IndexedRecord record, Schema schema, String fieldName) {
    return getFieldValueFromIndexedRecord(record, schema, fieldName);
  }

  @Override
  public HoodieRecord<IndexedRecord> constructHoodieRecord(BufferedRecord<IndexedRecord> bufferedRecord) {
    if (bufferedRecord.isDelete()) {
      return SpillableMapUtils.generateEmptyPayload(
          bufferedRecord.getRecordKey(),
          partitionPath,
          bufferedRecord.getOrderingValue(),
          payloadClass);
    }
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
    return new HoodieAvroIndexedRecord(hoodieKey, bufferedRecord.getRecord());
  }

  @Override
  public IndexedRecord seal(IndexedRecord record) {
    return record;
  }

  @Override
  public IndexedRecord toBinaryRow(Schema avroSchema, IndexedRecord record) {
    return record;
  }

  @Override
  public ClosableIterator<IndexedRecord> mergeBootstrapReaders(ClosableIterator<IndexedRecord> skeletonFileIterator,
                                                               Schema skeletonRequiredSchema,
                                                               ClosableIterator<IndexedRecord> dataFileIterator,
                                                               Schema dataRequiredSchema,
                                                               List<Pair<String, Object>> partitionFieldAndValues) {
    return new BootstrapIterator(skeletonFileIterator, skeletonRequiredSchema, dataFileIterator, dataRequiredSchema, partitionFieldAndValues);
  }

  @Override
  public UnaryOperator<IndexedRecord> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    if (!renamedColumns.isEmpty()) {
      throw new UnsupportedOperationException("Column renaming is not supported for the HoodieAvroReaderContext");
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

  private Object getFieldValueFromIndexedRecord(
      IndexedRecord record,
      Schema recordSchema,
      String fieldName) {
    Schema currentSchema = recordSchema;
    IndexedRecord currentRecord = record;
    String[] path = fieldName.split("\\.");
    for (int i = 0; i < path.length; i++) {
      Schema.Field field = currentSchema.getField(path[i]);
      if (field == null) {
        return null;
      }
      Object value = currentRecord.get(field.pos());
      if (i == path.length - 1) {
        return value;
      }
      currentSchema = field.schema();
      currentRecord = (IndexedRecord) value;
    }
    return null;
  }

  /**
   * Iterator that traverses the skeleton file and the base file in tandem.
   * The iterator will only extract the fields requested in the provided schemas.
   */
  private static class BootstrapIterator implements ClosableIterator<IndexedRecord> {
    private final ClosableIterator<IndexedRecord> skeletonFileIterator;
    private final Schema skeletonRequiredSchema;
    private final ClosableIterator<IndexedRecord> dataFileIterator;
    private final Schema dataRequiredSchema;
    private final Schema mergedSchema;
    private final int skeletonFields;
    private final int[] partitionFieldPositions;
    private final Object[] partitionValues;

    public BootstrapIterator(ClosableIterator<IndexedRecord> skeletonFileIterator, Schema skeletonRequiredSchema,
                             ClosableIterator<IndexedRecord> dataFileIterator, Schema dataRequiredSchema,
                             List<Pair<String, Object>> partitionFieldAndValues) {
      this.skeletonFileIterator = skeletonFileIterator;
      this.skeletonRequiredSchema = skeletonRequiredSchema;
      this.dataFileIterator = dataFileIterator;
      this.dataRequiredSchema = dataRequiredSchema;
      this.mergedSchema = AvroSchemaUtils.mergeSchemas(skeletonRequiredSchema, dataRequiredSchema);
      this.skeletonFields = skeletonRequiredSchema.getFields().size();
      this.partitionFieldPositions = partitionFieldAndValues.stream().map(Pair::getLeft).map(field -> mergedSchema.getField(field).pos()).mapToInt(Integer::intValue).toArray();
      this.partitionValues = partitionFieldAndValues.stream().map(Pair::getValue).toArray();
    }

    @Override
    public void close() {
      skeletonFileIterator.close();
      dataFileIterator.close();
    }

    @Override
    public boolean hasNext() {
      checkState(dataFileIterator.hasNext() == skeletonFileIterator.hasNext(),
          "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!");
      return skeletonFileIterator.hasNext();
    }

    @Override
    public IndexedRecord next() {
      IndexedRecord skeletonRecord = skeletonFileIterator.next();
      IndexedRecord dataRecord = dataFileIterator.next();
      GenericRecord mergedRecord = new GenericData.Record(mergedSchema);

      for (Schema.Field skeletonField : skeletonRequiredSchema.getFields()) {
        Schema.Field sourceField = skeletonRecord.getSchema().getField(skeletonField.name());
        mergedRecord.put(skeletonField.pos(), skeletonRecord.get(sourceField.pos()));
      }
      for (Schema.Field dataField : dataRequiredSchema.getFields()) {
        Schema.Field sourceField = dataRecord.getSchema().getField(dataField.name());
        mergedRecord.put(dataField.pos() + skeletonFields, dataRecord.get(sourceField.pos()));
      }
      for (int i = 0; i < partitionFieldPositions.length; i++) {
        if (mergedRecord.get(partitionFieldPositions[i]) == null) {
          mergedRecord.put(partitionFieldPositions[i], partitionValues[i]);
        }
      }
      return mergedRecord;
    }
  }
}
