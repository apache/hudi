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
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.DeleteContext;
import org.apache.hudi.common.util.JavaTypeConverter;
import org.apache.hudi.common.util.LocalAvroSchemaCache;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.keygen.KeyGenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.UnaryOperator;

import static org.apache.hudi.common.model.HoodieRecord.HOODIE_IS_DELETED_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.util.OrderingValues.isCommitTimeOrderingValue;

/**
 * Record context provides the APIs for record related operations. Record context is associated with
 * a corresponding {@link HoodieReaderContext} and is used for getting field values from a record,
 * transforming a record etc.
 */
public abstract class RecordContext<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  private final SerializableBiFunction<T, Schema, String> recordKeyExtractor;
  // for encoding and decoding schemas to the spillable map
  private final LocalAvroSchemaCache localAvroSchemaCache = LocalAvroSchemaCache.getInstance();

  protected final JavaTypeConverter typeConverter;
  protected String partitionPath;

  protected RecordContext(HoodieTableConfig tableConfig, JavaTypeConverter typeConverter) {
    this.typeConverter = typeConverter;
    this.recordKeyExtractor = tableConfig.populateMetaFields() ? metadataKeyExtractor() : virtualKeyExtractor(tableConfig.getRecordKeyFields()
        .orElseThrow(() -> new IllegalArgumentException("No record keys specified and meta fields are not populated")));
  }

  /**
   * Constructs an instance of {@link RecordContext} that provides field accessor methods but cannot compute the record key for records.
   */
  protected RecordContext(JavaTypeConverter typeConverter) {
    this.recordKeyExtractor = (record, schema) -> {
      throw new UnsupportedOperationException("Record key extractor is not initialized");
    };
    this.typeConverter = typeConverter;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

  public T extractDataFromRecord(HoodieRecord record, Schema schema, Properties properties) {
    return (T) record.getData();
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
   * Constructs a new {@link HoodieRecord} based on the given buffered record {@link BufferedRecord} and the provided partition path.
   * Use this method when the partition path is not consistent for all usages of the RecordContext instance.
   *
   * @param bufferedRecord The {@link BufferedRecord} object with engine-specific row
   * @param partitionPath The partition path of the record
   * @return A new instance of {@link HoodieRecord}.
   */
  public abstract HoodieRecord<T> constructHoodieRecord(BufferedRecord<T> bufferedRecord, String partitionPath);

  /**
   * Constructs a new {@link HoodieRecord} based on the given buffered record {@link BufferedRecord}.
   *
   * @param bufferedRecord The {@link BufferedRecord} object with engine-specific row
   * @return A new instance of {@link HoodieRecord}.
   */
  public HoodieRecord<T> constructHoodieRecord(BufferedRecord<T> bufferedRecord) {
    return constructHoodieRecord(bufferedRecord, partitionPath);
  }

  /**
   * Constructs a {@link HoodieRecord} that will be used as the record written out to storage.
   * This allows customization of the record construction logic for each engine for any required optimizations.
   * The implementation defaults to calling {@link #constructHoodieRecord(BufferedRecord)}.
   * @param bufferedRecord the {@link BufferedRecord} object to transform
   * @return a new instance of {@link HoodieRecord} that will be written out to storage
   */
  public HoodieRecord<T> constructFinalHoodieRecord(BufferedRecord<T> bufferedRecord) {
    return constructHoodieRecord(bufferedRecord, partitionPath);
  }

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

  /**
   * Construct a new Engine record with given record schema and all field values.
   *
   * @param recordSchema the schema of the record
   * @param fieldValues  the values of all fields
   * @return A new instance of Engine record.
   */
  public abstract T constructEngineRecord(Schema recordSchema, Object[] fieldValues);

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
    return recordKeyExtractor.apply(record, schema);
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

  public Comparable convertPartitionValueToEngineType(Comparable value) {
    return convertValueToEngineType(value);
  }

  /**
   * Converts the ordering value to the specific engine type.
   */
  public Comparable convertOrderingValueToEngineType(Comparable value) {
    return value instanceof ArrayComparable
        ? ((ArrayComparable) value).apply(this::convertValueToEngineType)
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
   * Fills an empty row with record key fields and returns.
   *
   * <p>When `emitDelete` is true for FileGroup reader and payload for DELETE record is empty,
   * a record key row is emitted to downstream to delete data from storage by record key with the best effort.
   * Returns null if the primary key semantics been lost: the requested schema does not include all the record key fields.
   *
   * @param recordKey record key
   *
   * @return Engine specific row which contains record key fields.
   */
  @Nullable
  public abstract T getDeleteRow(String recordKey);

  public boolean isDeleteRecord(T record, DeleteContext deleteContext) {
    return isBuiltInDeleteRecord(record, deleteContext) || isDeleteHoodieOperation(record, deleteContext)
        || isCustomDeleteRecord(record, deleteContext);
  }

  /**
   * Check if the value of column "_hoodie_is_deleted" is true.
   */
  private boolean isBuiltInDeleteRecord(T record, DeleteContext deleteContext) {
    if (!deleteContext.hasBuiltInDeleteField()) {
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
   * Seals the engine-specific record to make sure the data referenced in memory do not change.
   *
   * @param record The record.
   * @return The record containing the same data that do not change in memory over time.
   */
  public abstract T seal(T record);

  /**
   * Converts engine specific row into binary format.
   *
   * @param avroSchema The avro schema of the row
   * @param record     The engine row
   *
   * @return row in binary format
   */
  public abstract T toBinaryRow(Schema avroSchema, T record);

  /**
   * Creates a function that will reorder records of schema "from" to schema of "to"
   * all fields in "to" must be in "from", but not all fields in "from" must be in "to"
   *
   * @param from           the schema of records to be passed into UnaryOperator
   * @param to             the schema of records produced by UnaryOperator
   * @param renamedColumns map of renamed columns where the key is the new name from the query and
   *                       the value is the old name that exists in the file
   * @return a function that takes in a record and returns the record with reordered columns
   */
  public abstract UnaryOperator<T> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns);

  public final UnaryOperator<T> projectRecord(Schema from, Schema to) {
    return projectRecord(from, to, Collections.emptyMap());
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
   * Gets the ordering value from given delete record.
   *
   * @param deleteRecord The delete record
   *
   * @return The ordering value.
   */
  public Comparable getOrderingValue(DeleteRecord deleteRecord) {
    Comparable orderingValue = deleteRecord.getOrderingValue();
    return isCommitTimeOrderingValue(orderingValue)
        ? OrderingValues.getDefault()
        : convertOrderingValueToEngineType(orderingValue);
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
    if (orderingFieldNames == null || orderingFieldNames.length == 0) {
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

  private SerializableBiFunction<T, Schema, String> metadataKeyExtractor() {
    return (record, schema) -> getValue(record, schema, RECORD_KEY_METADATA_FIELD).toString();
  }

  private SerializableBiFunction<T, Schema, String> virtualKeyExtractor(String[] recordKeyFields) {
    if (recordKeyFields.length == 1) {
      // there might be consistency for record key encoding when partition fields are multiple for cow merging,
      // currently the incoming records are using the keys from HoodieRecord which utilities the write config and by default encodes the field name with the value
      // while here the field names are ignored, this function would be used to extract record keys from old base file.
      return (record, schema) -> {
        Object result = getValue(record, schema, recordKeyFields[0]);
        if (result == null) {
          throw new HoodieKeyException("recordKey cannot be null");
        }
        return result.toString();
      };
    }
    return (record, schema) -> {
      BiFunction<String, Integer, String> valueFunction = (recordKeyField, index) -> {
        Object result = getValue(record, schema, recordKeyField);
        return result != null ? result.toString() : null;
      };
      return KeyGenerator.constructRecordKey(recordKeyFields, valueFunction);
    };
  }
}
