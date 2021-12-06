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
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.avro.Conversions.DecimalConversion;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
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
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Helper class to do common stuff across Avro.
 */
public class HoodieAvroUtils {

  private static ThreadLocal<BinaryEncoder> reuseEncoder = ThreadLocal.withInitial(() -> null);

  private static ThreadLocal<BinaryDecoder> reuseDecoder = ThreadLocal.withInitial(() -> null);

  // As per https://avro.apache.org/docs/current/spec.html#names
  private static String INVALID_AVRO_CHARS_IN_NAMES = "[^A-Za-z0-9_]";
  private static String INVALID_AVRO_FIRST_CHAR_IN_NAMES = "[^A-Za-z_]";
  private static String MASK_FOR_INVALID_CHARS_IN_NAMES = "__";

  // All metadata fields are optional strings.
  public static final Schema METADATA_FIELD_SCHEMA =
      Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)));

  public static final Schema RECORD_KEY_SCHEMA = initRecordKeySchema();

  /**
   * Convert a given avro record to bytes.
   */
  public static byte[] avroToBytes(GenericRecord record) {
    return indexedRecordToBytes(record);
  }

  public static <T extends IndexedRecord> byte[] indexedRecordToBytes(T record) {
    GenericDatumWriter<T> writer = new GenericDatumWriter<>(record.getSchema());
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, reuseEncoder.get());
      reuseEncoder.set(encoder);
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
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, reuseDecoder.get());
    reuseDecoder.set(decoder);
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
    return HoodieRecord.COMMIT_TIME_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.COMMIT_SEQNO_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.RECORD_KEY_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.PARTITION_PATH_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.FILENAME_METADATA_FIELD.equals(fieldName)
        || HoodieRecord.OPERATION_METADATA_FIELD.equals(fieldName);
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
   * @param schema The schema
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

  public static Schema removeFields(Schema schema, List<String> fieldsToRemove) {
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

    for (Schema.Field schemaField: fileSchema.getFields()) {
      if (fields.contains(schemaField.name())) {
        toBeAddedFields.add(new Schema.Field(schemaField.name(), schemaField.schema(), schemaField.doc(), schemaField.defaultValue()));
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
   * Add null fields to passed in schema. Caller is responsible for ensuring there is no duplicates. As different query
   * engines have varying constraints regarding treating the case-sensitivity of fields, its best to let caller
   * determine that.
   *
   * @param schema Passed in schema
   * @param newFieldNames Null Field names to be added
   */
  public static Schema appendNullSchemaFields(Schema schema, List<String> newFieldNames) {
    List<Field> newFields = schema.getFields().stream()
        .map(field -> new Field(field.name(), field.schema(), field.doc(), field.defaultVal())).collect(Collectors.toList());
    for (String newField : newFieldNames) {
      newFields.add(new Schema.Field(newField, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE));
    }
    Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());
    newSchema.setFields(newFields);
    return newSchema;
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
   * Given a avro record with a given schema, rewrites it into the new schema while setting fields only from the new
   * schema.
   * NOTE: Here, the assumption is that you cannot go from an evolved schema (schema with (N) fields)
   * to an older schema (schema with (N-1) fields). All fields present in the older record schema MUST be present in the
   * new schema and the default/existing values are carried over.
   * This particular method does the following things :
   * a) Create a new empty GenericRecord with the new schema.
   * b) For GenericRecord, copy over the data from the old schema to the new schema or set default values for all fields of this
   * transformed schema
   * c) For SpecificRecord, hoodie_metadata_fields have a special treatment. This is done because for code generated
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
      if (!isSpecificRecord) {
        copyOldValueOrSetDefault(oldRecord, newRecord, f);
      } else if (!isMetadataField(f.name())) {
        copyOldValueOrSetDefault(oldRecord, newRecord, f);
      }
    }
    if (!GenericData.get().validate(newSchema, newRecord)) {
      throw new SchemaCompatibilityException(
          "Unable to validate the rewritten record " + oldRecord + " against schema " + newSchema);
    }
    return newRecord;
  }

  private static void copyOldValueOrSetDefault(GenericRecord oldRecord, GenericRecord newRecord, Schema.Field f) {
    // cache the result of oldRecord.get() to save CPU expensive hash lookup
    Schema oldSchema = oldRecord.getSchema();
    Object fieldValue = oldSchema.getField(f.name()) == null ? null : oldRecord.get(f.name());
    if (fieldValue == null) {
      if (f.defaultVal() instanceof JsonProperties.Null) {
        newRecord.put(f.name(), null);
      } else {
        newRecord.put(f.name(), f.defaultVal());
      }
    } else {
      newRecord.put(f.name(), fieldValue);
    }
  }

  public static byte[] compress(String text) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      OutputStream out = new DeflaterOutputStream(baos);
      out.write(text.getBytes(StandardCharsets.UTF_8));
      out.close();
    } catch (IOException e) {
      throw new HoodieIOException("IOException while compressing text " + text, e);
    }
    return baos.toByteArray();
  }

  public static String decompress(byte[] bytes) {
    InputStream in = new InflaterInputStream(new ByteArrayInputStream(bytes));
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      byte[] buffer = new byte[8192];
      int len;
      while ((len = in.read(buffer)) > 0) {
        baos.write(buffer, 0, len);
      }
      return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new HoodieIOException("IOException while decompressing text", e);
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
   * Obtain value of the provided field as string, denoted by dot notation. e.g: a.b.c
   */
  public static String getNestedFieldValAsString(GenericRecord record, String fieldName, boolean returnNullIfNotFound) {
    Object obj = getNestedFieldVal(record, fieldName, returnNullIfNotFound);
    return StringUtils.objToString(obj);
  }

  /**
   * Obtain value of the provided field, denoted by dot notation. e.g: a.b.c
   */
  public static Object getNestedFieldVal(GenericRecord record, String fieldName, boolean returnNullIfNotFound) {
    String[] parts = fieldName.split("\\.");
    GenericRecord valueNode = record;
    int i = 0;
    for (; i < parts.length; i++) {
      String part = parts[i];
      Object val = valueNode.get(part);
      if (val == null) {
        break;
      }

      // return, if last part of name
      if (i == parts.length - 1) {
        Schema fieldSchema = valueNode.getSchema().getField(part).schema();
        return convertValueForSpecificDataTypes(fieldSchema, val);
      } else {
        // VC: Need a test here
        if (!(val instanceof GenericRecord)) {
          throw new HoodieException("Cannot find a record at part value :" + part);
        }
        valueNode = (GenericRecord) val;
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
   * Returns the string value of the given record {@code rec} and field {@code fieldName}.
   * The field and value both could be missing.
   *
   * @param rec The record
   * @param fieldName The field name
   *
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
   * @param fieldValue avro field value
   * @return field value either converted (for certain data types) or as it is.
   */
  public static Object convertValueForSpecificDataTypes(Schema fieldSchema, Object fieldValue) {
    if (fieldSchema == null) {
      return fieldValue;
    }

    if (fieldSchema.getType() == Schema.Type.UNION) {
      for (Schema schema : fieldSchema.getTypes()) {
        if (schema.getType() != Schema.Type.NULL) {
          return convertValueForAvroLogicalTypes(schema, fieldValue);
        }
      }
    }
    return convertValueForAvroLogicalTypes(fieldSchema, fieldValue);
  }

  /**
   * This method converts values for fields with certain Avro Logical data types that require special handling.
   *
   * Logical Date Type is converted to actual Date value instead of Epoch Integer which is how it is
   * represented/stored in parquet.
   *
   * Decimal Data Type is converted to actual decimal value instead of bytes/fixed which is how it is
   * represented/stored in parquet.
   *
   * @param fieldSchema avro field schema
   * @param fieldValue avro field value
   * @return field value either converted (for certain data types) or as it is.
   */
  private static Object convertValueForAvroLogicalTypes(Schema fieldSchema, Object fieldValue) {
    if (fieldSchema.getLogicalType() == LogicalTypes.date()) {
      return LocalDate.ofEpochDay(Long.parseLong(fieldValue.toString()));
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
  public static Object getRecordColumnValues(HoodieRecord<? extends HoodieRecordPayload> record,
                                             String[] columns,
                                             Schema schema) {
    try {
      GenericRecord genericRecord = (GenericRecord) record.getData().getInsertValue(schema).get();
      if (columns.length == 1) {
        return HoodieAvroUtils.getNestedFieldVal(genericRecord, columns[0], true);
      } else {
        StringBuilder sb = new StringBuilder();
        for (String col : columns) {
          sb.append(HoodieAvroUtils.getNestedFieldValAsString(genericRecord, col, true));
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
  public static Object getRecordColumnValues(HoodieRecord<? extends HoodieRecordPayload> record,
                                             String[] columns,
                                             SerializableSchema schema) {
    return getRecordColumnValues(record, columns, schema.get());
  }
}
