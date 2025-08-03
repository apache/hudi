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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.DefaultJavaTypeConverter;
import org.apache.hudi.common.util.JavaTypeConverter;
import org.apache.hudi.common.util.LocalAvroSchemaCache;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.KeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;

/**
 * Record context provides the APIs for record related operations. Record context is associated with
 * a corresponding {@link HoodieReaderContext} and is used for getting field values from a record,
 * transforming a record etc.
 */
public abstract class RecordContext<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Option<SerializableBiFunction<T, Schema, String>> recordKeyExtractor;  // for encoding and decoding schemas to the spillable map
  private final LocalAvroSchemaCache localAvroSchemaCache = LocalAvroSchemaCache.getInstance();

  protected JavaTypeConverter typeConverter;
  protected String partitionPath = null;

  protected RecordContext(HoodieTableConfig tableConfig, boolean shouldUseMetaFields) {
    this.typeConverter = new DefaultJavaTypeConverter();
    this.recordKeyExtractor = shouldUseMetaFields ? metadataKeyExtractor() : virtualKeyExtractor(tableConfig.getRecordKeyFields());
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public T extractDataFromRecord(HoodieRecord record, Schema schema, Properties properties) {
    try {
      if (record.getData() instanceof HoodieRecordPayload) {
        HoodieRecordPayload payload = (HoodieRecordPayload) record.getData();
        return (T) payload.getIndexedRecord(schema, properties).map(value -> convertAvroRecord((IndexedRecord) value)).orElse(null);
      }
      return (T) record.getData();
    } catch (IOException e) {
      throw new HoodieException("Failed to extract data from record: " + record, e);
    }
  }

  /**
   * Gets the schema encoded in the buffered record {@code BufferedRecord}.
   *
   * @param record {@link BufferedRecord} object with engine-specific type
   *
   * @return The avro schema if it is encoded in the metadata map, else null
   */
  public Schema getSchemaFromBufferRecord(BufferedRecord<T> record) {
    return decodeAvroSchema(record.getSchemaId());
  }

  /**
   * Encodes the given avro schema for efficient serialization.
   */
  public Integer encodeAvroSchema(Schema schema) {
    return this.localAvroSchemaCache.cacheSchema(schema);
  }

  /**
   * Decodes the avro schema with given version ID.
   */
  @Nullable
  public Schema decodeAvroSchema(Object versionId) {
    return this.localAvroSchemaCache.getSchema((Integer) versionId).orElse(null);
  }

  /**
   * Constructs a new {@link HoodieRecord} based on the given buffered record {@link BufferedRecord}.
   *
   * @param bufferedRecord  The {@link BufferedRecord} object with engine-specific row
   * @return A new instance of {@link HoodieRecord}.
   */
  public abstract HoodieRecord<T> constructHoodieRecord(BufferedRecord<T> bufferedRecord);

  /**
   * Constructs a new Engine based record based on a given schema, base record and update values.
   *
   * @param schema           The schema of the new record.
   * @param updateValues     The map recording field index and its corresponding update value.
   * @param baseRecord       The record based on which the engine record is built.
   * @return A new instance of engine record type {@link T}.
   */
  public abstract T mergeWithEngineRecord(Schema schema,
                                          Map<Integer, Object> updateValues,
                                          BufferedRecord<T> baseRecord);

  public JavaTypeConverter getTypeConverter() {
    return typeConverter;
  }

  /**
   * Gets the record key in String.
   *
   * @param record The record in engine-specific type.
   * @param schema The Avro schema of the record.
   * @return The record key in String.
   */
  public String getRecordKey(T record, Schema schema) {
    return recordKeyExtractor.orElseThrow(() -> new IllegalArgumentException("No record keys specified and meta fields are not populated"))
        .apply(record, schema);
  }

  /**
   * Gets the field value.
   *
   * @param record    The record in engine-specific type.
   * @param schema    The Avro schema of the record.
   * @param fieldName The field name. A dot separated string if a nested field.
   * @return The field value.
   */
  public abstract Object getValue(T record, Schema schema, String fieldName);

  /**
   * Get value of metadata field in a more efficient way than #getValue.
   *
   * @param record The record in engine-specific type.
   * @param pos    The position of the metadata field.
   *
   * @return The value for the target metadata field.
   */
  public abstract String getMetaFieldValue(T record, int pos);

  /**
   * Returns the value to a type representation in a specific engine.
   * <p>
   * This can be overridden by the reader context implementation on a specific engine to handle
   * engine-specific field type system.  For example, Spark uses {@code UTF8String} to represent
   * {@link String} field values, so we need to convert the values to {@code UTF8String} type
   * in Spark for proper value comparison.
   *
   * @param value {@link Comparable} value to be converted.
   *
   * @return the converted value in a type representation in a specific engine.
   */
  public Comparable convertValueToEngineType(Comparable value) {
    return value;
  }

  /**
   * Converts the ordering value to the specific engine type.
   */
  public final Comparable convertOrderingValueToEngineType(Comparable value) {
    return value instanceof ArrayComparable
        ? ((ArrayComparable) value).apply(comparable -> convertValueToEngineType(comparable))
        : convertValueToEngineType(value);
  }

  /**
   * Converts an Avro record, e.g., serialized in the log files, to an engine-specific record.
   *
   * @param avroRecord The Avro record.
   * @return An engine-specific record in Type {@link T}.
   */
  public abstract T convertAvroRecord(IndexedRecord avroRecord);

  public abstract GenericRecord convertToAvroRecord(T record, Schema schema);

  /**
   * There are two cases to handle:
   * 1). Return the delete record if it's not null;
   * 2). otherwise fills an empty row with record key fields and returns.
   *
   * <p>For case2, when `emitDelete` is true for FileGroup reader and payload for DELETE record is empty,
   * a record key row is emitted to downstream to delete data from storage by record key with the best effort.
   * Returns null if the primary key semantics been lost: the requested schema does not include all the record key fields.
   *
   * @param record    delete record
   * @param recordKey record key
   *
   * @return Engine specific row which contains record key fields.
   */
  @Nullable
  public abstract T getDeleteRow(T record, String recordKey);

  public boolean isDeleteRecord(T record, DeleteContext deleteContext) {
    return isBuiltInDeleteRecord(record, deleteContext) || isDeleteHoodieOperation(record, deleteContext)
        || isCustomDeleteRecord(record, deleteContext);
  }

  /**
   * Check if the value of column "_hoodie_is_deleted" is true.
   */
  private boolean isBuiltInDeleteRecord(T record, DeleteContext deleteContext) {
    if (!deleteContext.getCustomDeleteMarkerKeyValue().isPresent()) {
      return false;
    }
    Object columnValue = getValue(record, deleteContext.getReaderSchema(), HOODIE_IS_DELETED_FIELD);
    return columnValue != null && getTypeConverter().castToBoolean(columnValue);
  }

  /**
   * Check if a record is a DELETE/UPDATE_BEFORE marked by the '_hoodie_operation' field.
   */
  private boolean isDeleteHoodieOperation(T record, DeleteContext deleteContext) {
    int hoodieOperationPos = deleteContext.getHoodieOperationPos();
    if (hoodieOperationPos < 0) {
      return false;
    }
    String operationVal = getMetaFieldValue(record, hoodieOperationPos);
    if (operationVal == null) {
      return false;
    }
    HoodieOperation operation = HoodieOperation.fromName(operationVal);
    return HoodieOperation.isDelete(operation) || HoodieOperation.isUpdateBefore(operation);
  }

  /**
   * Check if a record is a DELETE marked by a custom delete marker.
   */
  private boolean isCustomDeleteRecord(T record, DeleteContext deleteContext) {
    if (deleteContext.getCustomDeleteMarkerKeyValue().isEmpty()) {
      return false;
    }
    Pair<String, String> markerKeyValue = deleteContext.getCustomDeleteMarkerKeyValue().get();
    Object deleteMarkerValue = getValue(record, deleteContext.getReaderSchema(), markerKeyValue.getLeft());
    return deleteMarkerValue != null
        && markerKeyValue.getRight().equals(deleteMarkerValue.toString());
  }

  /**
   * Gets the ordering value in particular type.
   *
   * @param record             An option of record.
   * @param schema             The Avro schema of the record.
   * @param orderingFieldNames name of the ordering field
   * @return The ordering value.
   */
  public Comparable getOrderingValue(T record,
                                     Schema schema,
                                     List<String> orderingFieldNames) {
    if (orderingFieldNames.isEmpty()) {
      return OrderingValues.getDefault();
    }

    return OrderingValues.create(orderingFieldNames, field -> {
      Object value = getValue(record, schema, field);
      // API getDefaultOrderingValue is only used inside Comparables constructor
      return value != null ? convertValueToEngineType((Comparable) value) : OrderingValues.getDefault();
    });
  }

  /**
   * Gets the ordering value in particular type.
   *
   * @param record             An option of record.
   * @param schema             The Avro schema of the record.
   * @param orderingFieldNames names of the ordering fields
   * @return The ordering value.
   */
  public Comparable getOrderingValue(T record,
                                     Schema schema,
                                     String[] orderingFieldNames) {
    if (orderingFieldNames.length == 0) {
      return OrderingValues.getDefault();
    }

    return OrderingValues.create(orderingFieldNames, field -> {
      Object value = getValue(record, schema, field);
      // API getDefaultOrderingValue is only used inside Comparables constructor
      return value != null ? convertValueToEngineType((Comparable) value) : OrderingValues.getDefault();
    });
  }

  /**
   * Extracts the record position value from the record itself.
   *
   * @return the record position in the base file.
   */
  public long extractRecordPosition(T record, Schema schema, String fieldName, long providedPositionIfNeeded) {
    if (supportsParquetRowIndex()) {
      Object position = getValue(record, schema, fieldName);
      if (position != null) {
        return (long) position;
      } else {
        throw new IllegalStateException("Record position extraction failed");
      }
    }
    return providedPositionIfNeeded;
  }

  public boolean supportsParquetRowIndex() {
    return false;
  }

  private Option<SerializableBiFunction<T, Schema, String>> metadataKeyExtractor() {
    return Option.of((record, schema) -> getValue(record, schema, RECORD_KEY_METADATA_FIELD).toString());
  }

  private Option<SerializableBiFunction<T, Schema, String>> virtualKeyExtractor(Option<String[]> recordKeyFieldsOpt) {
    return recordKeyFieldsOpt.map(recordKeyFields ->
        (record, schema) -> {
          BiFunction<String, Integer, String> valueFunction = (recordKeyField, index) -> {
            Object result = getValue(record, schema, recordKeyField);
            return result != null ? result.toString() : null;
          };
          return KeyGenerator.constructRecordKey(recordKeyFields, valueFunction);
        });
  }
}
