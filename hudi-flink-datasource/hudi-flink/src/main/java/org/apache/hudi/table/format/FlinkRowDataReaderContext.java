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

package org.apache.hudi.table.format;

import org.apache.hudi.client.model.BootstrapRowData;
import org.apache.hudi.client.model.CommitTimeFlinkRecordMerger;
import org.apache.hudi.client.model.EventTimeFlinkRecordMerger;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.FileGroupReaderSchemaHandler;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.io.storage.HoodieIOFactory;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.util.AvroToRowDataConverters;
import org.apache.hudi.util.RowDataAvroQueryContexts;
import org.apache.hudi.util.RowDataUtils;
import org.apache.hudi.util.RowProjection;
import org.apache.hudi.util.SchemaEvolvingRowDataProjection;
import org.apache.hudi.util.RecordKeyToRowDataConverter;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;

/**
 * Implementation of {@link HoodieReaderContext} to read {@link RowData}s from base files or
 * log files with Flink parquet reader.
 */
public class FlinkRowDataReaderContext extends HoodieReaderContext<RowData> {
  private final List<ExpressionPredicates.Predicate> predicates;
  private final Supplier<InternalSchemaManager> internalSchemaManager;
  private final boolean utcTimezone;
  private final HoodieTableConfig tableConfig;
  // the converter is used to create a RowData contains primary key fields only
  // for DELETE cases, it'll not be initialized if primary key semantics is lost.
  // For e.g, if the pk fields are [a, b] but user only select a, then the pk
  // semantics is lost.
  private RecordKeyToRowDataConverter recordKeyRowConverter;

  public FlinkRowDataReaderContext(
      StorageConfiguration<?> storageConfiguration,
      Supplier<InternalSchemaManager> internalSchemaManager,
      List<ExpressionPredicates.Predicate> predicates,
      HoodieTableConfig tableConfig,
      Option<InstantRange> instantRangeOpt) {
    super(storageConfiguration, tableConfig, instantRangeOpt, Option.empty());
    this.tableConfig = tableConfig;
    this.internalSchemaManager = internalSchemaManager;
    this.predicates = predicates;
    this.utcTimezone = getStorageConfiguration().getBoolean(
        FlinkOptions.READ_UTC_TIMEZONE.key(),
        FlinkOptions.READ_UTC_TIMEZONE.defaultValue());
  }

  @Override
  public ClosableIterator<RowData> getFileRecordIterator(
      StoragePath filePath,
      long start,
      long length,
      Schema dataSchema,
      Schema requiredSchema,
      HoodieStorage storage) throws IOException {
    boolean isLogFile = FSUtils.isLogFile(filePath);
    // disable schema evolution in fileReader if it's log file, since schema evolution for log file is handled in `FileGroupRecordBuffer`
    InternalSchemaManager schemaManager = isLogFile ? InternalSchemaManager.DISABLED : internalSchemaManager.get();

    HoodieRowDataParquetReader rowDataParquetReader =
        (HoodieRowDataParquetReader) HoodieIOFactory.getIOFactory(storage)
            .getReaderFactory(HoodieRecord.HoodieRecordType.FLINK)
            .getFileReader(tableConfig, filePath, HoodieFileFormat.PARQUET, Option.empty());
    DataType rowType = RowDataAvroQueryContexts.fromAvroSchema(dataSchema).getRowType();
    return rowDataParquetReader.getRowDataIterator(schemaManager, rowType, requiredSchema, predicates);
  }

  @Override
  public void setSchemaHandler(FileGroupReaderSchemaHandler<RowData> schemaHandler) {
    super.setSchemaHandler(schemaHandler);

    Option<String[]> recordKeysOpt = tableConfig.getRecordKeyFields();
    if (recordKeysOpt.isEmpty()) {
      return;
    }
    // primary key semantic is lost if not all primary key fields are included in the request schema.
    boolean pkSemanticLost = Arrays.stream(recordKeysOpt.get()).anyMatch(k -> schemaHandler.getRequestedSchema().getField(k) == null);
    if (pkSemanticLost) {
      return;
    }
    // get primary key field position in required schema.
    Schema requiredSchema = schemaHandler.getRequiredSchema();
    int[] pkFieldsPos = Arrays.stream(recordKeysOpt.get())
        .map(k -> Option.ofNullable(requiredSchema.getField(k)).map(Schema.Field::pos).orElse(-1))
        .mapToInt(Integer::intValue)
        .toArray();
    recordKeyRowConverter = new RecordKeyToRowDataConverter(
        pkFieldsPos, (RowType) RowDataAvroQueryContexts.fromAvroSchema(requiredSchema).getRowType().getLogicalType());
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(EventTimeFlinkRecordMerger.INSTANCE);
      case COMMIT_TIME_ORDERING:
        return Option.of(CommitTimeFlinkRecordMerger.INSTANCE);
      default:
        Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.FLINK, mergeImplClasses, mergeStrategyId);
        if (recordMerger.isEmpty()) {
          throw new HoodieValidationException("No valid flink merger implementation set for `"
              + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
        }
        return recordMerger;
    }
  }

  @Override
  public Object getValue(RowData record, Schema schema, String fieldName) {
    RowDataAvroQueryContexts.FieldQueryContext fieldQueryContext =
        RowDataAvroQueryContexts.fromAvroSchema(schema, utcTimezone).getFieldQueryContext(fieldName);
    if (fieldQueryContext == null) {
      return null;
    } else {
      return fieldQueryContext.getFieldGetter().getFieldOrNull(record);
    }
  }

  @Override
  public String getMetaFieldValue(RowData record, int pos) {
    return record.getString(pos).toString();
  }

  @Override
  public HoodieRecord<RowData> constructHoodieRecord(BufferedRecord<RowData> bufferedRecord) {
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
    // delete record
    if (bufferedRecord.isDelete()) {
      return new HoodieEmptyRecord<>(hoodieKey, HoodieOperation.DELETE, bufferedRecord.getOrderingValue(), HoodieRecord.HoodieRecordType.FLINK);
    }
    RowData rowData = bufferedRecord.getRecord();
    HoodieOperation operation = HoodieOperation.fromValue(rowData.getRowKind().toByteValue());
    return new HoodieFlinkRecord(hoodieKey, operation, bufferedRecord.getOrderingValue(), rowData);
  }

  @Override
  public RowData mergeWithEngineRecord(Schema schema,
                                       Map<Integer, Object> updateValues,
                                       BufferedRecord<RowData> baseRecord) {
    GenericRowData genericRowData = new GenericRowData(schema.getFields().size());
    for (Schema.Field field : schema.getFields()) {
      int pos = field.pos();
      if (updateValues.containsKey(pos)) {
        genericRowData.setField(pos, updateValues.get(pos));
      } else {
        genericRowData.setField(pos, getValue(baseRecord.getRecord(), schema, field.name()));
      }
    }
    return genericRowData;
  }

  @Override
  public Comparable getOrderingValue(
      RowData record,
      Schema schema,
      List<String> orderingFieldNames) {
    if (orderingFieldNames.isEmpty()) {
      return OrderingValues.getDefault();
    }
    return OrderingValues.create(orderingFieldNames, field -> {
      if (schema.getField(field) == null) {
        return OrderingValues.getDefault();
      }
      RowDataAvroQueryContexts.FieldQueryContext context = RowDataAvroQueryContexts.fromAvroSchema(schema, utcTimezone).getFieldQueryContext(field);
      Comparable finalOrderingVal = (Comparable) context.getValAsJava(record, false);
      return finalOrderingVal;
    });
  }

  @Override
  public RowData seal(RowData rowData) {
    if (rowData instanceof BinaryRowData) {
      return ((BinaryRowData) rowData).copy();
    }
    return rowData;
  }

  @Override
  public RowData toBinaryRow(Schema avroSchema, RowData record) {
    if (record instanceof BinaryRowData) {
      return record;
    }
    RowDataSerializer rowDataSerializer = RowDataAvroQueryContexts.getRowDataSerializer(avroSchema);
    return rowDataSerializer.toBinaryRow(record);
  }

  @Override
  public ClosableIterator<RowData> mergeBootstrapReaders(
      ClosableIterator<RowData> skeletonFileIterator,
      Schema skeletonRequiredSchema,
      ClosableIterator<RowData> dataFileIterator,
      Schema dataRequiredSchema,
      List<Pair<String, Object>> partitionFieldAndValues) {
    Map<Integer, Object> partitionOrdinalToValues = partitionFieldAndValues.stream()
        .collect(Collectors.toMap(
            pair -> dataRequiredSchema.getField(pair.getKey()).pos(),
            Pair::getValue));
    return new ClosableIterator<RowData>() {
      final JoinedRowData joinedRow = new JoinedRowData();
      @Override
      public boolean hasNext() {
        boolean skeletonHasNext = skeletonFileIterator.hasNext();
        boolean dataHasNext = dataFileIterator.hasNext();
        ValidationUtils.checkArgument(skeletonHasNext == dataHasNext,
            "Bootstrap data-file iterator and skeleton-file iterator have to be in-sync!");
        return skeletonHasNext;
      }

      @Override
      public RowData next() {
        RowData skeletonRow = skeletonFileIterator.next();
        RowData dataRow = appendPartitionFields(dataFileIterator.next());
        joinedRow.setRowKind(dataRow.getRowKind());
        joinedRow.replace(skeletonRow, dataRow);
        return joinedRow;
      }

      private RowData appendPartitionFields(RowData dataRow) {
        if (partitionFieldAndValues.isEmpty()) {
          return dataRow;
        }
        return new BootstrapRowData(dataRow, partitionOrdinalToValues);
      }

      @Override
      public void close() {
        skeletonFileIterator.close();
        dataFileIterator.close();
      }
    };
  }

  /**
   * Creates a function that will reorder records of schema "from" to schema of "to".
   * It's possible there exist fields in `to` schema, but not in `from` schema because of schema
   * evolution.
   *
   * @param from           the schema of records to be passed into UnaryOperator
   * @param to             the schema of records produced by UnaryOperator
   * @param renamedColumns map of renamed columns where the key is the new name from the query and
   *                       the value is the old name that exists in the file
   * @return a function that takes in a record and returns the record with reordered columns
   */
  @Override
  public UnaryOperator<RowData> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    RowType fromType = (RowType) RowDataAvroQueryContexts.fromAvroSchema(from).getRowType().getLogicalType();
    RowType toType =  (RowType) RowDataAvroQueryContexts.fromAvroSchema(to).getRowType().getLogicalType();
    RowProjection rowProjection = SchemaEvolvingRowDataProjection.instance(fromType, toType, renamedColumns);
    return rowProjection::project;
  }

  @Override
  public GenericRecord convertToAvroRecord(RowData record, Schema schema) {
    return (GenericRecord) RowDataAvroQueryContexts.fromAvroSchema(schema).getRowDataToAvroConverter().convert(schema, record);
  }

  @Override
  public RowData getDeleteRow(RowData record, String recordKey) {
    if (record != null) {
      return record;
    }
    // don't need to emit record key row if primary key semantic is lost
    if (recordKeyRowConverter == null) {
      return null;
    }
    RowData recordKeyRow = recordKeyRowConverter.convert(recordKey);
    recordKeyRow.setRowKind(RowKind.DELETE);
    return recordKeyRow;
  }

  @Override
  public RowData convertAvroRecord(IndexedRecord avroRecord) {
    Schema recordSchema = avroRecord.getSchema();
    AvroToRowDataConverters.AvroToRowDataConverter converter = RowDataAvroQueryContexts.fromAvroSchema(recordSchema, utcTimezone).getAvroToRowDataConverter();
    RowData rowData = (RowData) converter.convert(avroRecord);
    Schema.Field operationField = recordSchema.getField(HoodieRecord.OPERATION_METADATA_FIELD);
    if (operationField != null) {
      HoodieOperation operation = HoodieOperation.fromName(rowData.getString(operationField.pos()).toString());
      rowData.setRowKind(RowKind.fromByteValue(operation.getValue()));
    }
    return rowData;
  }

  @Override
  public Comparable convertValueToEngineType(Comparable value) {
    return (Comparable) RowDataUtils.convertValueToFlinkType(value);
  }
}
