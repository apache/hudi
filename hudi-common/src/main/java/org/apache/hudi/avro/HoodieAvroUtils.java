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

import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversions;
import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonDecoder;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Set;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.UNION;
import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.resolveUnionSchema;

/**
 * Helper class to do common stuff across Avro.
 */
public class HoodieAvroUtils {

  private static final ThreadLocal<BinaryEncoder> BINARY_ENCODER = ThreadLocal.withInitial(() -> null);
  private static final ThreadLocal<BinaryDecoder> BINARY_DECODER = ThreadLocal.withInitial(() -> null);

  private static final long MILLIS_PER_DAY = 86400000L;

  //Export for test
  public static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

  // As per https://avro.apache.org/docs/current/spec.html#names
  private static final String INVALID_AVRO_CHARS_IN_NAMES = "[^A-Za-z0-9_]";
  private static final String INVALID_AVRO_FIRST_CHAR_IN_NAMES = "[^A-Za-z_]";
  private static final String MASK_FOR_INVALID_CHARS_IN_NAMES = "__";

  // All metadata fields are optional strings.
  public static final Schema METADATA_FIELD_SCHEMA = createNullableSchema(Schema.Type.STRING);

  public static final Schema RECORD_KEY_SCHEMA = initRecordKeySchema();

  /**
   * TODO serialize other type of record.
   */
  public static Option<byte[]> recordToBytes(HoodieRecord record, Schema schema) throws IOException {
    return Option.of(HoodieAvroUtils.indexedRecordToBytes((IndexedRecord) record.toIndexedRecord(schema, new Properties()).get()));
  }

  /**
   * Convert a given avro record to bytes.
   */
  public static byte[] avroToBytes(GenericRecord record) {
    return indexedRecordToBytes(record);
  }

  public static <T extends IndexedRecord> byte[] indexedRecordToBytes(T record) {
    GenericDatumWriter<T> writer = new GenericDatumWriter<>(record.getSchema(), ConvertingGenericData.INSTANCE);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, BINARY_ENCODER.get());
      BINARY_ENCODER.set(encoder);
      writer.write(record, encoder);
      encoder.flush();
      return out.toByteArray();
    } catch (IOException e) {
      throw new HoodieIOException("Cannot convert GenericRecord to bytes", e);
    }
  }

  /**
   * Convert a given avro record to json and return the encoded bytes.
   *
   * @param record The GenericRecord to convert
   * @param pretty Whether to pretty-print the json output
   */
  public static byte[] avroToJson(GenericRecord record, boolean pretty) throws IOException {
    DatumWriter<Object> writer = new GenericDatumWriter<>(record.getSchema());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out, pretty);
    writer.write(record, jsonEncoder);
    jsonEncoder.flush();
    return out.toByteArray();
  }

  /**
   * Convert serialized bytes back into avro record.
   */
  public static GenericRecord bytesToAvro(byte[] bytes, Schema schema) throws IOException {
    return bytesToAvro(bytes, schema, schema);
  }

  /**
   * Convert serialized bytes back into avro record.
   */
  public static GenericRecord bytesToAvro(byte[] bytes, Schema writerSchema, Schema readerSchema) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, BINARY_DECODER.get());
    BINARY_DECODER.set(decoder);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema, readerSchema);
    return reader.read(null, decoder);
  }

  /**
   * Convert json bytes back into avro record.
   */
  public static GenericRecord jsonBytesToAvro(byte[] bytes, Schema schema) throws IOException {
    ByteArrayInputStream bio = new ByteArrayInputStream(bytes);
    JsonDecoder jsonDecoder = DecoderFactory.get().jsonDecoder(schema, bio);
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    return reader.read(null, jsonDecoder);
  }

  public static boolean isMetadataField(String fieldName) {
    return HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION.contains(fieldName);
  }

  public static Schema createHoodieWriteSchema(Schema originalSchema) {
    return HoodieAvroUtils.addMetadataFields(originalSchema);
  }

  public static Schema createHoodieWriteSchema(String originalSchema) {
    return createHoodieWriteSchema(new Schema.Parser().parse(originalSchema));
  }

  /**
   * Adds the Hoodie metadata fields to the given schema.
   *
   * @param schema The schema
   */
  public static Schema addMetadataFields(Schema schema) {
    return addMetadataFields(schema, false);
  }

  /**
   * Adds the Hoodie metadata fields to the given schema.
   *
   * @param schema             The schema
   * @param withOperationField Whether to include the '_hoodie_operation' field
   */
  public static Schema addMetadataFields(Schema schema, boolean withOperationField) {
    List<Schema.Field> parentFields = new ArrayList<>();

    Schema.Field commitTimeField =
        new Schema.Field(HoodieRecord.COMMIT_TIME_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field commitSeqnoField =
        new Schema.Field(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field recordKeyField =
        new Schema.Field(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field partitionPathField =
        new Schema.Field(HoodieRecord.PARTITION_PATH_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field fileNameField =
        new Schema.Field(HoodieRecord.FILENAME_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);

    parentFields.add(commitTimeField);
    parentFields.add(commitSeqnoField);
    parentFields.add(recordKeyField);
    parentFields.add(partitionPathField);
    parentFields.add(fileNameField);

    if (withOperationField) {
      final Schema.Field operationField =
          new Schema.Field(HoodieRecord.OPERATION_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
      parentFields.add(operationField);
    }

    for (Schema.Field field : schema.getFields()) {
      if (!isMetadataField(field.name())) {
        Schema.Field newField = new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
        for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
          newField.addProp(prop.getKey(), prop.getValue());
        }
        parentFields.add(newField);
      }
    }

    Schema mergedSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    mergedSchema.setFields(parentFields);
    return mergedSchema;
  }

  public static Schema removeMetadataFields(Schema schema) {
    return removeFields(schema, HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION);
  }

  public static Schema removeFields(Schema schema, Set<String> fieldsToRemove) {
    List<Schema.Field> filteredFields = schema.getFields()
        .stream()
        .filter(field -> !fieldsToRemove.contains(field.name()))
        .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
        .collect(Collectors.toList());
    Schema filteredSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), false);
    filteredSchema.setFields(filteredFields);
    return filteredSchema;
  }

  public static String addMetadataColumnTypes(String hiveColumnTypes) {
    return "string,string,string,string,string," + hiveColumnTypes;
  }

  private static Schema initRecordKeySchema() {
    Schema.Field recordKeyField =
        new Schema.Field(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema recordKeySchema = Schema.createRecord("HoodieRecordKey", "", "", false);
    recordKeySchema.setFields(Collections.singletonList(recordKeyField));
    return recordKeySchema;
  }

  public static Schema getRecordKeySchema() {
    return RECORD_KEY_SCHEMA;
  }

  /**
   * Fetch schema for record key and partition path.
   */
  public static Schema getRecordKeyPartitionPathSchema() {
    List<Schema.Field> toBeAddedFields = new ArrayList<>();
    Schema recordSchema = Schema.createRecord("HoodieRecordKey", "", "", false);

    Schema.Field recordKeyField =
        new Schema.Field(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field partitionPathField =
        new Schema.Field(HoodieRecord.PARTITION_PATH_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);

    toBeAddedFields.add(recordKeyField);
    toBeAddedFields.add(partitionPathField);
    recordSchema.setFields(toBeAddedFields);
    return recordSchema;
  }

  /**
   * Fetch schema for record key and partition path.
   */
  public static Schema getSchemaForFields(Schema fileSchema, List<String> fields) {
    List<Schema.Field> toBeAddedFields = new ArrayList<>();
    Schema recordSchema = Schema.createRecord("HoodieRecordKey", "", "", false);

    for (Schema.Field schemaField : fileSchema.getFields()) {
      if (fields.contains(schemaField.name())) {
        toBeAddedFields.add(new Schema.Field(schemaField.name(), schemaField.schema(), schemaField.doc(), schemaField.defaultVal()));
      }
    }
    recordSchema.setFields(toBeAddedFields);
    return recordSchema;
  }

  public static GenericRecord addHoodieKeyToRecord(GenericRecord record, String recordKey, String partitionPath,
                                                   String fileName) {
    record.put(HoodieRecord.FILENAME_METADATA_FIELD, fileName);
    record.put(HoodieRecord.PARTITION_PATH_METADATA_FIELD, partitionPath);
    record.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recordKey);
    return record;
  }

  public static GenericRecord addOperationToRecord(GenericRecord record, HoodieOperation operation) {
    record.put(HoodieRecord.OPERATION_METADATA_FIELD, operation.getName());
    return record;
  }

  /**
   * Adds the Hoodie commit metadata into the provided Generic Record.
   */
  public static GenericRecord addCommitMetadataToRecord(GenericRecord record, String instantTime, String commitSeqno) {
    record.put(HoodieRecord.COMMIT_TIME_METADATA_FIELD, instantTime);
    record.put(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, commitSeqno);
    return record;
  }

  public static GenericRecord stitchRecords(GenericRecord left, GenericRecord right, Schema stitchedSchema) {
    GenericRecord result = new Record(stitchedSchema);
    for (Schema.Field f : left.getSchema().getFields()) {
      result.put(f.name(), left.get(f.name()));
    }
    for (Schema.Field f : right.getSchema().getFields()) {
      result.put(f.name(), right.get(f.name()));
    }
    return result;
  }

  /**
   * Given an Avro record with a given schema, rewrites it into the new schema while setting fields only from the new
   * schema.
   *
   * NOTE: This method is rewriting every record's field that is record itself recursively. It's
   *       caller's responsibility to make sure that no unnecessary re-writing occurs (by preemptively
   *       checking whether the record does require re-writing to adhere to the new schema)
   *
   * NOTE: Here, the assumption is that you cannot go from an evolved schema (schema with (N) fields)
   *       to an older schema (schema with (N-1) fields). All fields present in the older record schema MUST be present in the
   *       new schema and the default/existing values are carried over.
   *
   * This particular method does the following:
   * <ol>
   *   <li>Create a new empty GenericRecord with the new schema.</li>
   *   <li>For GenericRecord, copy over the data from the old schema to the new schema or set default values for all
   *   fields of this transformed schema</li>
   *   <li>For SpecificRecord, hoodie_metadata_fields have a special treatment (see below)</li>
   * </ol>
   *
   * For SpecificRecord we ignore Hudi Metadata fields, because for code generated
   * avro classes (HoodieMetadataRecord), the avro record is a SpecificBaseRecord type instead of a GenericRecord.
   * SpecificBaseRecord throws null pointer exception for record.get(name) if name is not present in the schema of the
   * record (which happens when converting a SpecificBaseRecord without hoodie_metadata_fields to a new record with it).
   * In this case, we do NOT set the defaults for the hoodie_metadata_fields explicitly, instead, the new record assumes
   * the default defined in the avro schema itself.
   * TODO: See if we can always pass GenericRecord instead of SpecificBaseRecord in some cases.
   */
  public static GenericRecord rewriteRecord(GenericRecord oldRecord, Schema newSchema) {
    GenericRecord newRecord = new GenericData.Record(newSchema);
    boolean isSpecificRecord = oldRecord instanceof SpecificRecordBase;
    for (Schema.Field f : newSchema.getFields()) {
      if (!(isSpecificRecord && isMetadataField(f.name()))) {
        copyOldValueOrSetDefault(oldRecord, newRecord, f);
      }
    }

    if (!ConvertingGenericData.INSTANCE.validate(newSchema, newRecord)) {
      throw new SchemaCompatibilityException(
          "Unable to validate the rewritten record " + oldRecord + " against schema " + newSchema);
    }

    return newRecord;
  }

  public static GenericRecord rewriteRecordWithMetadata(GenericRecord genericRecord, Schema newSchema, String fileName) {
    GenericRecord newRecord = new GenericData.Record(newSchema);
    for (Schema.Field f : newSchema.getFields()) {
      copyOldValueOrSetDefault(genericRecord, newRecord, f);
    }
    // do not preserve FILENAME_METADATA_FIELD
    newRecord.put(HoodieRecord.FILENAME_META_FIELD_ORD, fileName);
    if (!GenericData.get().validate(newSchema, newRecord)) {
      throw new SchemaCompatibilityException(
          "Unable to validate the rewritten record " + genericRecord + " against schema " + newSchema);
    }
    return newRecord;
  }

  // TODO Unify the logical of rewriteRecordWithMetadata and rewriteEvolutionRecordWithMetadata, and delete this function.
  public static GenericRecord rewriteEvolutionRecordWithMetadata(GenericRecord genericRecord, Schema newSchema, String fileName) {
    GenericRecord newRecord = HoodieAvroUtils.rewriteRecordWithNewSchema(genericRecord, newSchema, new HashMap<>());
    // do not preserve FILENAME_METADATA_FIELD
    newRecord.put(HoodieRecord.FILENAME_META_FIELD_ORD, fileName);
    return newRecord;
  }

  /**
   * Converts list of {@link GenericRecord} provided into the {@link GenericRecord} adhering to the
   * provided {@code newSchema}.
   * <p>
   * To better understand conversion rules please check {@link #rewriteRecord(GenericRecord, Schema)}
   */
  public static List<GenericRecord> rewriteRecords(List<GenericRecord> records, Schema newSchema) {
    return records.stream().map(r -> rewriteRecord(r, newSchema)).collect(Collectors.toList());
  }

  /**
   * Given an Avro record and list of columns to remove, this method removes the list of columns from
   * the given avro record using rewriteRecord method.
   * <p>
   * To better understand how it removes please check {@link #rewriteRecord(GenericRecord, Schema)}
   */
  public static GenericRecord removeFields(GenericRecord record, Set<String> fieldsToRemove) {
    Schema newSchema = removeFields(record.getSchema(), fieldsToRemove);
    return rewriteRecord(record, newSchema);
  }

  private static void copyOldValueOrSetDefault(GenericRecord oldRecord, GenericRecord newRecord, Schema.Field field) {
    Schema oldSchema = oldRecord.getSchema();
    Object fieldValue = oldSchema.getField(field.name()) == null ? null : oldRecord.get(field.name());

    if (fieldValue != null) {
      // In case field's value is a nested record, we have to rewrite it as well
      Object newFieldValue;
      if (fieldValue instanceof GenericRecord) {
        GenericRecord record = (GenericRecord) fieldValue;
        newFieldValue = rewriteRecord(record, resolveUnionSchema(field.schema(), record.getSchema().getFullName()));
      } else {
        newFieldValue = fieldValue;
      }
      newRecord.put(field.name(), newFieldValue);
    } else if (field.defaultVal() instanceof JsonProperties.Null) {
      newRecord.put(field.name(), null);
    } else {
      newRecord.put(field.name(), field.defaultVal());
    }
  }

  /**
   * Generate a reader schema off the provided writeSchema, to just project out the provided columns.
   */
  public static Schema generateProjectionSchema(Schema originalSchema, List<String> fieldNames) {
    Map<String, Field> schemaFieldsMap = originalSchema.getFields().stream()
        .map(r -> Pair.of(r.name().toLowerCase(), r)).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    List<Schema.Field> projectedFields = new ArrayList<>();
    for (String fn : fieldNames) {
      Schema.Field field = schemaFieldsMap.get(fn.toLowerCase());
      if (field == null) {
        throw new HoodieException("Field " + fn + " not found in log schema. Query cannot proceed! "
            + "Derived Schema Fields: " + new ArrayList<>(schemaFieldsMap.keySet()));
      } else {
        projectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()));
      }
    }

    Schema projectedSchema = Schema.createRecord(originalSchema.getName(), originalSchema.getDoc(),
        originalSchema.getNamespace(), originalSchema.isError());
    projectedSchema.setFields(projectedFields);
    return projectedSchema;
  }

  /**
   * Obtain the root-level field name of a full field name, possibly a nested field.
   * For example, given "a.b.c", the output is "a"; given "a", the output is "a".
   *
   * @param fieldName The field name.
   * @return Root-level field name
   */
  public static String getRootLevelFieldName(String fieldName) {
    return fieldName.split("\\.")[0];
  }

  /**
   * Obtain value of the provided field as string, denoted by dot notation. e.g: a.b.c
   */
  public static String getNestedFieldValAsString(GenericRecord record, String fieldName, boolean returnNullIfNotFound, boolean consistentLogicalTimestampEnabled) {
    Object obj = getNestedFieldVal(record, fieldName, returnNullIfNotFound, consistentLogicalTimestampEnabled);
    return StringUtils.objToString(obj);
  }

  /**
   * Obtain value of the provided field, denoted by dot notation. e.g: a.b.c
   */
  public static Object getNestedFieldVal(GenericRecord record, String fieldName, boolean returnNullIfNotFound, boolean consistentLogicalTimestampEnabled) {
    String[] parts = fieldName.split("\\.");
    GenericRecord valueNode = record;
    int i = 0;
    try {
      for (; i < parts.length; i++) {
        String part = parts[i];
        Object val = valueNode.get(part);
        if (val == null) {
          break;
        }

        // return, if last part of name
        if (i == parts.length - 1) {
          Schema fieldSchema = valueNode.getSchema().getField(part).schema();
          return convertValueForSpecificDataTypes(fieldSchema, val, consistentLogicalTimestampEnabled);
        } else {
          // VC: Need a test here
          if (!(val instanceof GenericRecord)) {
            throw new HoodieException("Cannot find a record at part value :" + part);
          }
          valueNode = (GenericRecord) val;
        }
      }
    } catch (AvroRuntimeException e) {
      // Since avro 1.10, arvo will throw AvroRuntimeException("Not a valid schema field: " + key)
      // rather than return null like the previous version if if record doesn't contain this key.
      // So when returnNullIfNotFound is true, catch this exception.
      if (!returnNullIfNotFound) {
        throw e;
      }
    }

    if (returnNullIfNotFound) {
      return null;
    } else if (valueNode.getSchema().getField(parts[i]) == null) {
      throw new HoodieException(
          fieldName + "(Part -" + parts[i] + ") field not found in record. Acceptable fields were :"
              + valueNode.getSchema().getFields().stream().map(Field::name).collect(Collectors.toList()));
    } else {
      throw new HoodieException("The value of " + parts[i] + " can not be null");
    }
  }

  /**
   * Get schema for the given field and record. Field can be nested, denoted by dot notation. e.g: a.b.c
   *
   * @param record    - record containing the value of the given field
   * @param fieldName - name of the field
   * @return
   */
  public static Schema getNestedFieldSchemaFromRecord(GenericRecord record, String fieldName) {
    String[] parts = fieldName.split("\\.");
    GenericRecord valueNode = record;
    int i = 0;
    for (; i < parts.length; i++) {
      String part = parts[i];
      Object val = valueNode.get(part);

      if (i == parts.length - 1) {
        return resolveNullableSchema(valueNode.getSchema().getField(part).schema());
      } else {
        if (!(val instanceof GenericRecord)) {
          throw new HoodieException("Cannot find a record at part value :" + part);
        }
        valueNode = (GenericRecord) val;
      }
    }
    throw new HoodieException("Failed to get schema. Not a valid field name: " + fieldName);
  }


  /**
   * Get schema for the given field and write schema. Field can be nested, denoted by dot notation. e.g: a.b.c
   * Use this method when record is not available. Otherwise, prefer to use {@link #getNestedFieldSchemaFromRecord(GenericRecord, String)}
   *
   * @param writeSchema - write schema of the record
   * @param fieldName   -  name of the field
   * @return
   */
  public static Schema getNestedFieldSchemaFromWriteSchema(Schema writeSchema, String fieldName) {
    String[] parts = fieldName.split("\\.");
    int i = 0;
    for (; i < parts.length; i++) {
      String part = parts[i];
      Schema schema = writeSchema.getField(part).schema();

      if (i == parts.length - 1) {
        return resolveNullableSchema(schema);
      }
    }
    throw new HoodieException("Failed to get schema. Not a valid field name: " + fieldName);
  }

  /**
   * Returns the string value of the given record {@code rec} and field {@code fieldName}.
   * The field and value both could be missing.
   *
   * @param rec       The record
   * @param fieldName The field name
   * @return the string form of the field
   * or empty if the schema does not contain the field name or the value is null
   */
  public static Option<String> getNullableValAsString(GenericRecord rec, String fieldName) {
    Schema.Field field = rec.getSchema().getField(fieldName);
    String fieldVal = field == null ? null : StringUtils.objToString(rec.get(field.pos()));
    return Option.ofNullable(fieldVal);
  }

  /**
   * This method converts values for fields with certain Avro/Parquet data types that require special handling.
   *
   * @param fieldSchema avro field schema
   * @param fieldValue  avro field value
   * @return field value either converted (for certain data types) or as it is.
   */
  public static Object convertValueForSpecificDataTypes(Schema fieldSchema, Object fieldValue, boolean consistentLogicalTimestampEnabled) {
    if (fieldSchema == null) {
      return fieldValue;
    }

    if (fieldSchema.getType() == Schema.Type.UNION) {
      for (Schema schema : fieldSchema.getTypes()) {
        if (schema.getType() != Schema.Type.NULL) {
          return convertValueForAvroLogicalTypes(schema, fieldValue, consistentLogicalTimestampEnabled);
        }
      }
    }
    return convertValueForAvroLogicalTypes(fieldSchema, fieldValue, consistentLogicalTimestampEnabled);
  }

  /**
   * This method converts values for fields with certain Avro Logical data types that require special handling.
   * <p>
   * Logical Date Type is converted to actual Date value instead of Epoch Integer which is how it is
   * represented/stored in parquet.
   * <p>
   * Decimal Data Type is converted to actual decimal value instead of bytes/fixed which is how it is
   * represented/stored in parquet.
   *
   * @param fieldSchema avro field schema
   * @param fieldValue  avro field value
   * @return field value either converted (for certain data types) or as it is.
   */
  private static Object convertValueForAvroLogicalTypes(Schema fieldSchema, Object fieldValue, boolean consistentLogicalTimestampEnabled) {
    if (fieldSchema.getLogicalType() == LogicalTypes.date()) {
      return LocalDate.ofEpochDay(Long.parseLong(fieldValue.toString()));
    } else if (fieldSchema.getLogicalType() == LogicalTypes.timestampMillis() && consistentLogicalTimestampEnabled) {
      return new Timestamp(Long.parseLong(fieldValue.toString()));
    } else if (fieldSchema.getLogicalType() == LogicalTypes.timestampMicros() && consistentLogicalTimestampEnabled) {
      return new Timestamp(Long.parseLong(fieldValue.toString()) / 1000);
    } else if (fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
      Decimal dc = (Decimal) fieldSchema.getLogicalType();
      DecimalConversion decimalConversion = new DecimalConversion();
      if (fieldSchema.getType() == Schema.Type.FIXED) {
        return decimalConversion.fromFixed((GenericFixed) fieldValue, fieldSchema,
            LogicalTypes.decimal(dc.getPrecision(), dc.getScale()));
      } else if (fieldSchema.getType() == Schema.Type.BYTES) {
        ByteBuffer byteBuffer = (ByteBuffer) fieldValue;
        BigDecimal convertedValue = decimalConversion.fromBytes(byteBuffer, fieldSchema,
            LogicalTypes.decimal(dc.getPrecision(), dc.getScale()));
        byteBuffer.rewind();
        return convertedValue;
      }
    }
    return fieldValue;
  }

  public static Schema getNullSchema() {
    return Schema.create(Schema.Type.NULL);
  }

  /**
   * Sanitizes Name according to Avro rule for names.
   * Removes characters other than the ones mentioned in https://avro.apache.org/docs/current/spec.html#names .
   *
   * @param name input name
   * @return sanitized name
   */
  public static String sanitizeName(String name) {
    if (name.substring(0, 1).matches(INVALID_AVRO_FIRST_CHAR_IN_NAMES)) {
      name = name.replaceFirst(INVALID_AVRO_FIRST_CHAR_IN_NAMES, MASK_FOR_INVALID_CHARS_IN_NAMES);
    }
    return name.replaceAll(INVALID_AVRO_CHARS_IN_NAMES, MASK_FOR_INVALID_CHARS_IN_NAMES);
  }

  /**
   * Gets record column values into one object.
   *
   * @param record  Hoodie record.
   * @param columns Names of the columns to get values.
   * @param schema  {@link Schema} instance.
   * @return Column value if a single column, or concatenated String values by comma.
   */
  public static Object getRecordColumnValues(HoodieAvroRecord record,
                                             String[] columns,
                                             Schema schema, boolean consistentLogicalTimestampEnabled) {
    try {
      GenericRecord genericRecord = (GenericRecord) record.toIndexedRecord(schema, new Properties()).get();
      if (columns.length == 1) {
        return HoodieAvroUtils.getNestedFieldVal(genericRecord, columns[0], true, consistentLogicalTimestampEnabled);
      } else {
        // TODO this is inefficient, instead we can simply return array of Comparable
        StringBuilder sb = new StringBuilder();
        for (String col : columns) {
          sb.append(HoodieAvroUtils.getNestedFieldValAsString(genericRecord, col, true, consistentLogicalTimestampEnabled));
        }

        return sb.toString();
      }
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read record with key:" + record.getKey(), e);
    }
  }

  /**
   * Gets record column values into one object.
   *
   * @param record  Hoodie record.
   * @param columns Names of the columns to get values.
   * @param schema  {@link SerializableSchema} instance.
   * @return Column value if a single column, or concatenated String values by comma.
   */
  public static Object getRecordColumnValues(HoodieAvroRecord record,
                                             String[] columns,
                                             SerializableSchema schema, boolean consistentLogicalTimestampEnabled) {
    return getRecordColumnValues(record, columns, schema.get(), consistentLogicalTimestampEnabled);
  }

  /**
   * Given a avro record with a given schema, rewrites it into the new schema while setting fields only from the new schema.
   * support deep rewrite for nested record.
   * This particular method does the following things :
   * a) Create a new empty GenericRecord with the new schema.
   * b) For GenericRecord, copy over the data from the old schema to the new schema or set default values for all fields of this transformed schema
   *
   * @param oldRecord oldRecord to be rewritten
   * @param newSchema newSchema used to rewrite oldRecord
   * @param renameCols a map store all rename cols, (k, v)-> (colNameFromNewSchema, colNameFromOldSchema)
   * @return newRecord for new Schema
   */
  public static GenericRecord rewriteRecordWithNewSchema(IndexedRecord oldRecord, Schema newSchema, Map<String, String> renameCols) {
    Object newRecord = rewriteRecordWithNewSchema(oldRecord, oldRecord.getSchema(), newSchema, renameCols, new LinkedList<>());
    return (GenericData.Record) newRecord;
  }

  /**
   * Given a avro record with a given schema, rewrites it into the new schema while setting fields only from the new schema.
   * support deep rewrite for nested record and adjust rename operation.
   * This particular method does the following things :
   * a) Create a new empty GenericRecord with the new schema.
   * b) For GenericRecord, copy over the data from the old schema to the new schema or set default values for all fields of this transformed schema
   *
   * @param oldRecord oldRecord to be rewritten
   * @param oldAvroSchema old avro schema.
   * @param newSchema newSchema used to rewrite oldRecord
   * @param renameCols a map store all rename cols, (k, v)-> (colNameFromNewSchema, colNameFromOldSchema)
   * @param fieldNames track the full name of visited field when we travel new schema.
   * @return newRecord for new Schema
   */
  private static Object rewriteRecordWithNewSchema(Object oldRecord, Schema oldAvroSchema, Schema newSchema, Map<String, String> renameCols, Deque<String> fieldNames) {
    if (oldRecord == null) {
      return null;
    }
    // try to get real schema for union type
    Schema oldSchema = getActualSchemaFromUnion(oldAvroSchema, oldRecord);
    switch (newSchema.getType()) {
      case RECORD:
        ValidationUtils.checkArgument(oldRecord instanceof IndexedRecord, "cannot rewrite record with different type");
        IndexedRecord indexedRecord = (IndexedRecord) oldRecord;
        List<Schema.Field> fields = newSchema.getFields();
        GenericData.Record newRecord = new GenericData.Record(newSchema);
        for (int i = 0; i < fields.size(); i++) {
          Schema.Field field = fields.get(i);
          String fieldName = field.name();
          fieldNames.push(fieldName);
          if (oldSchema.getField(field.name()) != null) {
            Schema.Field oldField = oldSchema.getField(field.name());
            newRecord.put(i, rewriteRecordWithNewSchema(indexedRecord.get(oldField.pos()), oldField.schema(), fields.get(i).schema(), renameCols, fieldNames));
          } else {
            String fieldFullName = createFullName(fieldNames);
            String fieldNameFromOldSchema = renameCols.getOrDefault(fieldFullName, "");
            // deal with rename
            if (oldSchema.getField(field.name()) == null && oldSchema.getField(fieldNameFromOldSchema) != null) {
              // find rename
              Schema.Field oldField = oldSchema.getField(fieldNameFromOldSchema);
              newRecord.put(i, rewriteRecordWithNewSchema(indexedRecord.get(oldField.pos()), oldField.schema(), fields.get(i).schema(), renameCols, fieldNames));
            } else {
              // deal with default value
              if (fields.get(i).defaultVal() instanceof JsonProperties.Null) {
                newRecord.put(i, null);
              } else {
                newRecord.put(i, fields.get(i).defaultVal());
              }
            }
          }
          fieldNames.pop();
        }
        return newRecord;
      case ARRAY:
        ValidationUtils.checkArgument(oldRecord instanceof Collection, "cannot rewrite record with different type");
        Collection array = (Collection)oldRecord;
        List<Object> newArray = new ArrayList();
        fieldNames.push("element");
        for (Object element : array) {
          newArray.add(rewriteRecordWithNewSchema(element, oldSchema.getElementType(), newSchema.getElementType(), renameCols, fieldNames));
        }
        fieldNames.pop();
        return newArray;
      case MAP:
        ValidationUtils.checkArgument(oldRecord instanceof Map, "cannot rewrite record with different type");
        Map<Object, Object> map = (Map<Object, Object>) oldRecord;
        Map<Object, Object> newMap = new HashMap<>();
        fieldNames.push("value");
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          newMap.put(entry.getKey(), rewriteRecordWithNewSchema(entry.getValue(), oldSchema.getValueType(), newSchema.getValueType(), renameCols, fieldNames));
        }
        fieldNames.pop();
        return newMap;
      case UNION:
        return rewriteRecordWithNewSchema(oldRecord, getActualSchemaFromUnion(oldSchema, oldRecord), getActualSchemaFromUnion(newSchema, oldRecord), renameCols, fieldNames);
      default:
        return rewritePrimaryType(oldRecord, oldSchema, newSchema);
    }
  }

  public static String createFullName(Deque<String> fieldNames) {
    String result = "";
    if (!fieldNames.isEmpty()) {
      List<String> parentNames = new ArrayList<>();
      fieldNames.descendingIterator().forEachRemaining(parentNames::add);
      result = parentNames.stream().collect(Collectors.joining("."));
    }
    return result;
  }

  private static Object rewritePrimaryType(Object oldValue, Schema oldSchema, Schema newSchema) {
    Schema realOldSchema = oldSchema;
    if (realOldSchema.getType() == UNION) {
      realOldSchema = getActualSchemaFromUnion(oldSchema, oldValue);
    }
    if (realOldSchema.getType() == newSchema.getType()) {
      switch (realOldSchema.getType()) {
        case NULL:
        case BOOLEAN:
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case BYTES:
        case STRING:
          return oldValue;
        case FIXED:
          // fixed size and name must match:
          if (!SchemaCompatibility.schemaNameEquals(realOldSchema, newSchema) || realOldSchema.getFixedSize() != newSchema.getFixedSize()) {
            // deal with the precision change for decimalType
            if (realOldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
              final byte[] bytes;
              bytes = ((GenericFixed) oldValue).bytes();
              LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) realOldSchema.getLogicalType();
              BigDecimal bd = new BigDecimal(new BigInteger(bytes), decimal.getScale()).setScale(((LogicalTypes.Decimal) newSchema.getLogicalType()).getScale());
              return DECIMAL_CONVERSION.toFixed(bd, newSchema, newSchema.getLogicalType());
            }
          } else {
            return oldValue;
          }
          return oldValue;
        default:
          throw new AvroRuntimeException("Unknown schema type: " + newSchema.getType());
      }
    } else {
      return rewritePrimaryTypeWithDiffSchemaType(oldValue, realOldSchema, newSchema);
    }
  }

  private static Object rewritePrimaryTypeWithDiffSchemaType(Object oldValue, Schema oldSchema, Schema newSchema) {
    switch (newSchema.getType()) {
      case NULL:
      case BOOLEAN:
        break;
      case INT:
        if (newSchema.getLogicalType() == LogicalTypes.date() && oldSchema.getType() == Schema.Type.STRING) {
          return fromJavaDate(java.sql.Date.valueOf(oldValue.toString()));
        }
        break;
      case LONG:
        if (oldSchema.getType() == Schema.Type.INT) {
          return ((Integer) oldValue).longValue();
        }
        break;
      case FLOAT:
        if ((oldSchema.getType() == Schema.Type.INT)
                || (oldSchema.getType() == Schema.Type.LONG)) {
          return oldSchema.getType() == Schema.Type.INT ? ((Integer) oldValue).floatValue() : ((Long) oldValue).floatValue();
        }
        break;
      case DOUBLE:
        if (oldSchema.getType() == Schema.Type.FLOAT) {
          // java float cannot convert to double directly, deal with float precision change
          return Double.valueOf(oldValue + "");
        } else if (oldSchema.getType() == Schema.Type.INT) {
          return ((Integer) oldValue).doubleValue();
        } else if (oldSchema.getType() == Schema.Type.LONG) {
          return ((Long) oldValue).doubleValue();
        }
        break;
      case BYTES:
        if (oldSchema.getType() == Schema.Type.STRING) {
          return (oldValue.toString()).getBytes(StandardCharsets.UTF_8);
        }
        break;
      case STRING:
        if (oldSchema.getType() == Schema.Type.BYTES) {
          return String.valueOf(((byte[]) oldValue));
        }
        if (oldSchema.getLogicalType() == LogicalTypes.date()) {
          return toJavaDate((Integer) oldValue).toString();
        }
        if (oldSchema.getType() == Schema.Type.INT
                || oldSchema.getType() == Schema.Type.LONG
                || oldSchema.getType() == Schema.Type.FLOAT
                || oldSchema.getType() == Schema.Type.DOUBLE) {
          return oldValue.toString();
        }
        if (oldSchema.getType() == Schema.Type.FIXED && oldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
          final byte[] bytes;
          bytes = ((GenericFixed) oldValue).bytes();
          LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) oldSchema.getLogicalType();
          BigDecimal bd = new BigDecimal(new BigInteger(bytes), decimal.getScale());
          return bd.toString();
        }
        break;
      case FIXED:
        // deal with decimal Type
        if (newSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
          // TODO: support more types
          if (oldSchema.getType() == Schema.Type.STRING
                  || oldSchema.getType() == Schema.Type.DOUBLE
                  || oldSchema.getType() == Schema.Type.INT
                  || oldSchema.getType() == Schema.Type.LONG
                  || oldSchema.getType() == Schema.Type.FLOAT) {
            LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) newSchema.getLogicalType();
            BigDecimal bigDecimal = null;
            if (oldSchema.getType() == Schema.Type.STRING) {
              bigDecimal = new java.math.BigDecimal(oldValue.toString())
                      .setScale(decimal.getScale());
            } else {
              // Due to Java, there will be precision problems in direct conversion, we should use string instead of use double
              bigDecimal = new java.math.BigDecimal(oldValue.toString())
                      .setScale(decimal.getScale());
            }
            return DECIMAL_CONVERSION.toFixed(bigDecimal, newSchema, newSchema.getLogicalType());
          }
        }
        break;
      default:
    }
    throw new AvroRuntimeException(String.format("cannot support rewrite value for schema type: %s since the old schema type is: %s", newSchema, oldSchema));
  }

  /**
   * convert days to Date
   *
   * NOTE: This method could only be used in tests
   *
   * @VisibleForTesting
   */
  public static java.sql.Date toJavaDate(int days) {
    LocalDate date = LocalDate.ofEpochDay(days);
    ZoneId defaultZoneId = ZoneId.systemDefault();
    ZonedDateTime zonedDateTime = date.atStartOfDay(defaultZoneId);
    return new java.sql.Date(zonedDateTime.toInstant().toEpochMilli());
  }

  /**
   * convert Date to days
   *
   * NOTE: This method could only be used in tests
   *
   * @VisibleForTesting
   */
  public static int fromJavaDate(Date date) {
    long millisUtc = date.getTime();
    long millisLocal = millisUtc + TimeZone.getDefault().getOffset(millisUtc);
    int julianDays = Math.toIntExact(Math.floorDiv(millisLocal, MILLIS_PER_DAY));
    return julianDays;
  }

  private static Schema getActualSchemaFromUnion(Schema schema, Object data) {
    Schema actualSchema;
    if (!schema.getType().equals(UNION)) {
      return schema;
    }
    if (schema.getTypes().size() == 2
            && schema.getTypes().get(0).getType() == Schema.Type.NULL) {
      actualSchema = schema.getTypes().get(1);
    } else if (schema.getTypes().size() == 2
            && schema.getTypes().get(1).getType() == Schema.Type.NULL) {
      actualSchema = schema.getTypes().get(0);
    } else if (schema.getTypes().size() == 1) {
      actualSchema = schema.getTypes().get(0);
    } else {
      // deal complex union. this should not happened in hoodie,
      // since flink/spark do not write this type.
      int i = GenericData.get().resolveUnion(schema, data);
      actualSchema = schema.getTypes().get(i);
    }
    return actualSchema;
  }

  /**
   * Given avro records, rewrites them with new schema.
   *
   * @param oldRecords oldRecords to be rewrite
   * @param newSchema newSchema used to rewrite oldRecord
   * @param renameCols a map store all rename cols, (k, v)-> (colNameFromNewSchema, colNameFromOldSchema)
   * @return a iterator of rewrote GeneriRcords
   */
  public static Iterator<GenericRecord> rewriteRecordWithNewSchema(Iterator<GenericRecord> oldRecords, Schema newSchema, Map<String, String> renameCols) {
    if (oldRecords == null || newSchema == null) {
      return Collections.emptyIterator();
    }
    return new Iterator<GenericRecord>() {
      @Override
      public boolean hasNext() {
        return oldRecords.hasNext();
      }

      @Override
      public GenericRecord next() {
        return rewriteRecordWithNewSchema(oldRecords.next(), newSchema, renameCols);
      }
    };
  }

  public static GenericRecord rewriteRecordDeep(GenericRecord oldRecord, Schema newSchema) {
    return rewriteRecordWithNewSchema(oldRecord, newSchema, Collections.EMPTY_MAP);
  }
}
