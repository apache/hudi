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

import org.apache.hudi.avro.model.ArrayWrapper;
import org.apache.hudi.avro.model.BooleanWrapper;
import org.apache.hudi.avro.model.BytesWrapper;
import org.apache.hudi.avro.model.DateWrapper;
import org.apache.hudi.avro.model.DecimalWrapper;
import org.apache.hudi.avro.model.DoubleWrapper;
import org.apache.hudi.avro.model.FloatWrapper;
import org.apache.hudi.avro.model.IntWrapper;
import org.apache.hudi.avro.model.LocalDateWrapper;
import org.apache.hudi.avro.model.LongWrapper;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.avro.model.TimestampMicrosWrapper;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.SpillableMapUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.ArrayComparable;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.SchemaCompatibilityException;
import org.apache.hudi.util.Lazy;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversions;
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
import org.apache.avro.util.Utf8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Type.ARRAY;
import static org.apache.avro.Schema.Type.MAP;
import static org.apache.avro.Schema.Type.UNION;
import static org.apache.hudi.avro.AvroSchemaUtils.createNewSchemaFromFieldsWithReference;
import static org.apache.hudi.avro.AvroSchemaUtils.createNullableSchema;
import static org.apache.hudi.avro.AvroSchemaUtils.isNullable;
import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.common.util.DateTimeUtils.instantToMicros;
import static org.apache.hudi.common.util.DateTimeUtils.microsToInstant;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.tryUpcastDecimal;

/**
 * Helper class to do common stuff across Avro.
 */
public class HoodieAvroUtils {

  public static final String AVRO_VERSION = Schema.class.getPackage().getImplementationVersion();

  private static final ThreadLocal<BinaryEncoder> BINARY_ENCODER = ThreadLocal.withInitial(() -> null);
  private static final ThreadLocal<BinaryDecoder> BINARY_DECODER = ThreadLocal.withInitial(() -> null);

  private static final Conversions.DecimalConversion AVRO_DECIMAL_CONVERSION = new Conversions.DecimalConversion();
  /**
   * NOTE: PLEASE READ CAREFULLY
   * <p>
   * In Avro 1.10 generated builders rely on {@code SpecificData.getForSchema} invocation that in turn
   * does use reflection to load the code-gen'd class corresponding to the Avro record model. This has
   * serious adverse effects in terms of performance when gets executed on the hot-path (both, in terms
   * of runtime and efficiency).
   * <p>
   * To work this around instead of using default code-gen'd builder invoking {@code SpecificData.getForSchema},
   * we instead rely on overloaded ctor accepting another instance of the builder: {@code Builder(Builder)},
   * which bypasses such invocation. Following corresponding builder's stubs are statically initialized
   * to be used exactly for that purpose.
   * <p>
   * You can find more details in HUDI-3834.
   */
  private static final Lazy<StringWrapper.Builder> STRING_WRAPPER_BUILDER_STUB = Lazy.lazily(StringWrapper::newBuilder);
  private static final Lazy<BytesWrapper.Builder> BYTES_WRAPPER_BUILDER_STUB = Lazy.lazily(BytesWrapper::newBuilder);
  private static final Lazy<DoubleWrapper.Builder> DOUBLE_WRAPPER_BUILDER_STUB = Lazy.lazily(DoubleWrapper::newBuilder);
  private static final Lazy<FloatWrapper.Builder> FLOAT_WRAPPER_BUILDER_STUB = Lazy.lazily(FloatWrapper::newBuilder);
  private static final Lazy<LongWrapper.Builder> LONG_WRAPPER_BUILDER_STUB = Lazy.lazily(LongWrapper::newBuilder);
  private static final Lazy<IntWrapper.Builder> INT_WRAPPER_BUILDER_STUB = Lazy.lazily(IntWrapper::newBuilder);
  private static final Lazy<BooleanWrapper.Builder> BOOLEAN_WRAPPER_BUILDER_STUB = Lazy.lazily(BooleanWrapper::newBuilder);
  private static final Lazy<TimestampMicrosWrapper.Builder> TIMESTAMP_MICROS_WRAPPER_BUILDER_STUB = Lazy.lazily(TimestampMicrosWrapper::newBuilder);
  private static final Lazy<DecimalWrapper.Builder> DECIMAL_WRAPPER_BUILDER_STUB = Lazy.lazily(DecimalWrapper::newBuilder);
  private static final Lazy<DateWrapper.Builder> DATE_WRAPPER_BUILDER_STUB = Lazy.lazily(DateWrapper::newBuilder);
  private static final Lazy<LocalDateWrapper.Builder> LOCAL_DATE_WRAPPER_BUILDER_STUB = Lazy.lazily(LocalDateWrapper::newBuilder);
  private static final Lazy<ArrayWrapper.Builder> ARRAY_WRAPPER_BUILDER_STUB = Lazy.lazily(ArrayWrapper::newBuilder);

  private static final long MILLIS_PER_DAY = 86400000L;

  //Export for test
  public static final Conversions.DecimalConversion DECIMAL_CONVERSION = new Conversions.DecimalConversion();

  // As per https://avro.apache.org/docs/current/spec.html#names
  private static final Pattern INVALID_AVRO_CHARS_IN_NAMES_PATTERN = Pattern.compile("[^A-Za-z0-9_]");
  private static final Pattern INVALID_AVRO_FIRST_CHAR_IN_NAMES_PATTERN = Pattern.compile("[^A-Za-z_]");
  private static final String MASK_FOR_INVALID_CHARS_IN_NAMES = "__";
  private static final Properties PROPERTIES = new Properties();

  // All metadata fields are optional strings.
  public static final Schema METADATA_FIELD_SCHEMA = createNullableSchema(Schema.Type.STRING);

  public static final Schema RECORD_KEY_SCHEMA = initRecordKeySchema();

  /**
   * TODO serialize other type of record.
   */
  public static Option<byte[]> recordToBytes(HoodieRecord record, Schema schema) throws IOException {
    return Option.of(HoodieAvroUtils.indexedRecordToBytesStream(record.toIndexedRecord(schema, new Properties()).get().getData()).toByteArray());
  }

  /**
   * Convert a given avro record to bytes.
   */
  public static byte[] avroToBytes(IndexedRecord record) {
    return indexedRecordToBytesStream(record).toByteArray();
  }

  /**
   * Convert a given avro record to bytes.
   */
  public static ByteArrayOutputStream avroToBytesStream(IndexedRecord record) {
    return indexedRecordToBytesStream(record);
  }

  public static <T extends IndexedRecord> ByteArrayOutputStream indexedRecordToBytesStream(T record) {
    GenericDatumWriter<T> writer = new GenericDatumWriter<>(record.getSchema(), ConvertingGenericData.INSTANCE);
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, BINARY_ENCODER.get());
      BINARY_ENCODER.set(encoder);
      writer.write(record, encoder);
      encoder.flush();
      return out;
    } catch (IOException e) {
      throw new HoodieIOException("Cannot convert GenericRecord to bytes", e);
    }
  }

  /**
   * Convert a given avro record to json and return the string
   *
   * @param record The GenericRecord to convert
   * @param pretty Whether to pretty-print the json output
   */
  public static String avroToJsonString(GenericRecord record, boolean pretty) throws IOException {
    return avroToJsonHelper(record, pretty).toString();
  }

  /**
   * Convert a given avro record to a JSON string. If the record contents are invalid, return the record.toString().
   * Use this method over {@link HoodieAvroUtils#avroToJsonString} when simply trying to print the record contents without any guarantees around their correctness.
   *
   * @param record The GenericRecord to convert
   * @return a JSON string
   */
  public static String safeAvroToJsonString(GenericRecord record) {
    try {
      return avroToJsonString(record, false);
    } catch (Exception e) {
      return record.toString();
    }
  }

  /**
   * Convert a given avro record to json and return the encoded bytes.
   *
   * @param record The GenericRecord to convert
   * @param pretty Whether to pretty-print the json output
   */
  public static byte[] avroToJson(GenericRecord record, boolean pretty) throws IOException {
    return avroToJsonHelper(record, pretty).toByteArray();
  }

  private static ByteArrayOutputStream avroToJsonHelper(GenericRecord record, boolean pretty) throws IOException {
    DatumWriter<Object> writer = new GenericDatumWriter<>(record.getSchema());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonEncoder jsonEncoder = EncoderFactory.get().jsonEncoder(record.getSchema(), out, pretty);
    writer.write(record, jsonEncoder);
    jsonEncoder.flush();
    return out;
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
  public static GenericRecord bytesToAvro(byte[] bytes, Schema writerSchema, Schema readerSchema)
      throws IOException {
    return bytesToAvro(bytes, 0, bytes.length, writerSchema, readerSchema);
  }

  /**
   * Convert serialized bytes back into avro record.
   */
  public static GenericRecord bytesToAvro(byte[] bytes, int offset, int length, Schema writerSchema,
                                          Schema readerSchema) throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(
        bytes, offset, length, BINARY_DECODER.get());
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

  public static boolean isTypeNumeric(Schema.Type type) {
    return type == Schema.Type.INT || type == Schema.Type.LONG || type == Schema.Type.FLOAT || type == Schema.Type.DOUBLE;
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

  public static Schema createHoodieWriteSchema(String originalSchema, boolean withOperationField) {
    return addMetadataFields(new Schema.Parser().parse(originalSchema), withOperationField);
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
    int newFieldsSize = HoodieRecord.HOODIE_META_COLUMNS.size() + (withOperationField ? 1 : 0);
    List<Schema.Field> parentFields = new ArrayList<>(schema.getFields().size() + newFieldsSize);

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
    return createNewSchemaFromFieldsWithReference(schema, parentFields);
  }

  public static boolean isSchemaNull(Schema schema) {
    return schema == null || schema.getType() == Schema.Type.NULL;
  }

  public static Schema removeMetadataFields(Schema schema) {
    if (isSchemaNull(schema)) {
      return schema;
    }
    return removeFields(schema, HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION);
  }

  public static Schema removeFields(Schema schema, Set<String> fieldsToRemove) {
    List<Schema.Field> filteredFields = schema.getFields()
        .stream()
        .filter(field -> !fieldsToRemove.contains(field.name()))
        .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
        .collect(Collectors.toList());

    return createNewSchemaFromFieldsWithReference(schema, filteredFields);
  }

  public static String addMetadataColumnTypes(String hiveColumnTypes) {
    return "string,string,string,string,string," + hiveColumnTypes;
  }

  public static Schema makeFieldNonNull(Schema schema, String fieldName, Object fieldDefaultValue) {
    ValidationUtils.checkArgument(fieldDefaultValue != null);
    List<Schema.Field> filteredFields = schema.getFields()
        .stream()
        .map(field -> {
          if (Objects.equals(field.name(), fieldName)) {
            return new Schema.Field(field.name(), AvroSchemaUtils.resolveNullableSchema(field.schema()), field.doc(), fieldDefaultValue);
          } else {
            return new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
          }
        })
        .collect(Collectors.toList());
    return createNewSchemaFromFieldsWithReference(schema, filteredFields);
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
   * Fetches projected schema given list of fields to project. The field can be nested in format `a.b.c` where a is
   * the top level field, b is at second level and so on.
   */
  public static Schema projectSchema(Schema fileSchema, List<String> fields) {
    List<LinkedList<String>> fieldPathLists = new ArrayList<>();
    for (String path : fields) {
      fieldPathLists.add(new LinkedList<>(Arrays.asList(path.split("\\."))));
    }

    Schema result = Schema.createRecord(fileSchema.getName(), fileSchema.getDoc(), fileSchema.getNamespace(), fileSchema.isError());
    result.setFields(projectFields(fileSchema, fieldPathLists));
    return result;
  }

  /**
   * Projects the requested fields in the schema. The fields can be nested in format `a.b.c`.
   *
   * @param originalSchema Schema to project from
   * @param fieldPaths List of fields. The field can be nested.
   *
   * @return List of projected schema fields
   */
  private static List<Schema.Field> projectFields(Schema originalSchema, List<LinkedList<String>> fieldPaths) {
    // We maintain a mapping of top level field to list of nested field paths which need to be projected from the field
    // If the entire field needs to be projected the list is empty
    // The map is ordered by position of the field in the original schema so that projected schema also maintains
    // the same field ordering
    Map<Field, List<LinkedList<String>>> groupedByTop = new TreeMap<>(Comparator.comparingInt(Field::pos));

    // Group paths by their current level (first element)
    // Here nested fields are considered as a path. The top level field is the first element in the path
    // second level field is the second element and so on.
    // We iterate through the input field paths and populate groupedByTop described above
    // For example if f1 is a top level field, and input field paths are f1.f2.f3 and f1.f2.f4
    // we maintain a mapping from f1 to {f2->f3, f2->f4}
    for (LinkedList<String> originalPath : fieldPaths) {
      if (originalPath.isEmpty()) {
        throw new IllegalArgumentException("Field path is empty or malformed: " + originalPath);
      }
      LinkedList<String> path = new LinkedList<>(originalPath); // Avoid mutating the original
      String head = path.poll(); // Remove first element
      Field topField = originalSchema.getField(head);
      if (topField == null) {
        throw new IllegalArgumentException("Field `" + head + "` not found in schema: " + originalSchema.getFields());
      }
      groupedByTop.compute(topField, (ignored, list) -> {
        if (path.isEmpty() || (list != null && list.isEmpty())) {
          // Case 1: where path is empty indicating the entire field needs to be included.
          // No further projection provided for that field
          // Case 2: if list is empty, it indicates that the top level field has already been selected
          // No more projection possible.
          return new ArrayList<>();
        } else {
          if (list == null) {
            // if list is null return just the path
            List<LinkedList<String>> retList = new ArrayList<>();
            retList.add(path);
            return retList;
          } else {
            // if list has elements, add path to it
            list.add(path);
            return list;
          }
        }
      });
    }

    List<Schema.Field> projectedFields = new ArrayList<>();
    for (Map.Entry<Field, List<LinkedList<String>>> entry : groupedByTop.entrySet()) {
      // For every top level field we process the child fields to include in the projected schema
      Field originalField = entry.getKey();
      List<LinkedList<String>> childPaths = entry.getValue();
      Schema originalFieldSchema = originalField.schema();
      Schema nonNullableSchema = unwrapNullable(originalFieldSchema);

      Schema projectedFieldSchema;
      if (!childPaths.isEmpty()) {
        // If child paths are present, it indicates there are nested fields which need to be projected
        if (nonNullableSchema.getType() != Schema.Type.RECORD) {
          throw new IllegalArgumentException("Cannot project nested field from non-record field '" + originalField.name()
              + "' of type: " + nonNullableSchema.getType());
        }

        // For those nested fields we make a recursive call to project fields to fetch the nested schema fields
        List<Schema.Field> nestedFields = projectFields(nonNullableSchema, childPaths);
        Schema nestedProjected = Schema.createRecord(nonNullableSchema.getName(), nonNullableSchema.getDoc(),
            nonNullableSchema.getNamespace(), nonNullableSchema.isError());
        nestedProjected.setFields(nestedFields);
        projectedFieldSchema = wrapNullable(originalFieldSchema, nestedProjected);
      } else {
        // if child field paths are empty, we need to include the top level field itself
        projectedFieldSchema = originalFieldSchema;
      }

      projectedFields.add(new Schema.Field(originalField.name(), projectedFieldSchema, originalField.doc(), originalField.defaultVal()));
    }

    return projectedFields;
  }

  /**
   * If schema is a union with ["null", X], returns X.
   * Otherwise, returns schema as-is.
   */
  public static Schema unwrapNullable(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      List<Schema> types = schema.getTypes();
      for (Schema s : types) {
        if (s.getType() != Schema.Type.NULL) {
          return s;
        }
      }
    }
    return schema;
  }

  /**
   * Wraps schema as nullable if original was a nullable union.
   */
  public static Schema wrapNullable(Schema original, Schema updated) {
    if (original.getType() == Schema.Type.UNION) {
      List<Schema> types = original.getTypes();
      if (types.stream().anyMatch(s -> s.getType() == Schema.Type.NULL)) {
        List<Schema> newUnion = new ArrayList<>();
        newUnion.add(Schema.create(Schema.Type.NULL));
        newUnion.add(updated);
        return Schema.createUnion(newUnion);
      }
    }
    return updated;
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
   * <p>
   * NOTE: This method is rewriting every record's field that is record itself recursively. It's
   * caller's responsibility to make sure that no unnecessary re-writing occurs (by preemptively
   * checking whether the record does require re-writing to adhere to the new schema)
   * <p>
   * NOTE: Here, the assumption is that you cannot go from an evolved schema (schema with (N) fields)
   * to an older schema (schema with (N-1) fields). All fields present in the older record schema MUST be present in the
   * new schema and the default/existing values are carried over.
   * <p>
   * This particular method does the following:
   * <ol>
   *   <li>Create a new empty GenericRecord with the new schema.</li>
   *   <li>For GenericRecord, copy over the data from the old schema to the new schema or set default values for all
   *   fields of this transformed schema</li>
   *   <li>For SpecificRecord, hoodie_metadata_fields have a special treatment (see below)</li>
   * </ol>
   * <p>
   * For SpecificRecord we ignore Hudi Metadata fields, because for code generated
   * avro classes (HoodieMetadataRecord), the avro record is a SpecificBaseRecord type instead of a GenericRecord.
   * SpecificBaseRecord throws null pointer exception for record.get(name) if name is not present in the schema of the
   * record (which happens when converting a SpecificBaseRecord without hoodie_metadata_fields to a new record with it).
   * In this case, we do NOT set the defaults for the hoodie_metadata_fields explicitly, instead, the new record assumes
   * the default defined in the avro schema itself.
   * TODO: See if we can always pass GenericRecord instead of SpecificBaseRecord in some cases.
   */
  public static GenericRecord rewriteRecord(GenericRecord oldRecord, Schema newSchema) {
    boolean isSpecificRecord = oldRecord instanceof SpecificRecordBase;
    Object newRecord = rewriteRecordWithNewSchemaInternal(oldRecord, oldRecord.getSchema(), newSchema, Collections.emptyMap(), new LinkedList<>(), isSpecificRecord);
    return (GenericData.Record) newRecord;
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
   * Obtain value of the provided key, which is consistent with avro before 1.10
   */
  public static Object getFieldVal(GenericRecord record, String key) {
    return getFieldVal(record, key, true);
  }

  /**
   * Obtain value of the provided key, when set returnNullIfNotFound false,
   * it is consistent with avro after 1.10
   */
  public static Object getFieldVal(GenericRecord record, String key, boolean returnNullIfNotFound) {
    Schema.Field field = record.getSchema().getField(key);
    if (field == null) {
      if (returnNullIfNotFound) {
        return null;
      } else {
        // Since avro 1.10, arvo will throw AvroRuntimeException("Not a valid schema field: " + key)
        // rather than return null like the previous version if record doesn't contain this key.
        // Here we simulate this behavior.
        throw new AvroRuntimeException("Not a valid schema field: " + key);
      }
    } else {
      return record.get(field.pos());
    }
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

    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];
      Object val;
      try {
        val = HoodieAvroUtils.getFieldVal(valueNode, part, returnNullIfNotFound);
      } catch (AvroRuntimeException e) {
        if (returnNullIfNotFound) {
          return null;
        } else {
          throw new HoodieException(
              fieldName + "(Part -" + parts[i] + ") field not found in record. Acceptable fields were :"
                  + valueNode.getSchema().getFields().stream().map(Field::name).collect(Collectors.toList()));
        }
      }

      if (i == parts.length - 1) {
        // return, if last part of name
        if (val == null) {
          return null;
        } else {
          Schema fieldSchema = valueNode.getSchema().getField(part).schema();
          return convertValueForSpecificDataTypes(fieldSchema, val, consistentLogicalTimestampEnabled);
        }
      } else {
        if (!(val instanceof GenericRecord)) {
          if (returnNullIfNotFound) {
            return null;
          } else {
            throw new HoodieException("Cannot find a record at part value :" + part);
          }
        } else {
          valueNode = (GenericRecord) val;
        }
      }
    }

    // This can only be reached if the length of parts is 0
    if (returnNullIfNotFound) {
      return null;
    } else {
      throw new HoodieException(
          fieldName + " field not found in record. Acceptable fields were :"
              + valueNode.getSchema().getFields().stream().map(Field::name).collect(Collectors.toList()));
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
    Schema currentSchema = writeSchema;
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];
      try {
        // Resolve nullable/union schema to the actual schema
        currentSchema = resolveNullableSchema(currentSchema.getField(part).schema());

        if (i == parts.length - 1) {
          // Return the schema for the final part
          return resolveNullableSchema(currentSchema);
        }
      } catch (Exception e) {
        throw new HoodieException("Failed to get schema. Not a valid field name: " + fieldName);
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
  public static Object convertValueForSpecificDataTypes(Schema fieldSchema,
                                                        Object fieldValue,
                                                        boolean consistentLogicalTimestampEnabled) {
    if (fieldSchema == null) {
      return fieldValue;
    } else if (fieldValue == null) {
      checkState(isNullable(fieldSchema));
      return null;
    }

    return convertValueForAvroLogicalTypes(resolveNullableSchema(fieldSchema), fieldValue, consistentLogicalTimestampEnabled);
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
  public static Object convertValueForAvroLogicalTypes(Schema fieldSchema, Object fieldValue, boolean consistentLogicalTimestampEnabled) {
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
    return sanitizeName(name, MASK_FOR_INVALID_CHARS_IN_NAMES);
  }

  /**
   * Sanitizes Name according to Avro rule for names.
   * Removes characters other than the ones mentioned in https://avro.apache.org/docs/current/spec.html#names .
   *
   * @param name            input name
   * @param invalidCharMask replacement for invalid characters.
   * @return sanitized name
   */
  public static String sanitizeName(String name, String invalidCharMask) {
    if (INVALID_AVRO_FIRST_CHAR_IN_NAMES_PATTERN.matcher(name.substring(0, 1)).matches()) {
      name = INVALID_AVRO_FIRST_CHAR_IN_NAMES_PATTERN.matcher(name).replaceFirst(invalidCharMask);
    }
    return INVALID_AVRO_CHARS_IN_NAMES_PATTERN.matcher(name).replaceAll(invalidCharMask);
  }

  /**
   * Gets record column values into object array.
   *
   * @param record  Hoodie record.
   * @param columns Names of the columns to get values.
   * @param schema  {@link Schema} instance.
   * @return Column value.
   */
  public static Object[] getRecordColumnValues(HoodieRecord record,
                                               String[] columns,
                                               Schema schema,
                                               boolean consistentLogicalTimestampEnabled) {
    try {
      GenericRecord genericRecord = (GenericRecord) (record.toIndexedRecord(schema, new Properties()).get()).getData();
      List<Object> list = new ArrayList<>();
      for (String col : columns) {
        list.add(HoodieAvroUtils.getNestedFieldVal(genericRecord, col, true, consistentLogicalTimestampEnabled));
      }
      return list.toArray();
    } catch (IOException e) {
      throw new HoodieIOException("Unable to read record with key:" + record.getKey(), e);
    }
  }

  /**
   * Gets record column values into object array.
   *
   * @param record  Hoodie record.
   * @param columns Names of the columns to get values.
   * @param schema  {@link Schema} instance.
   * @return Column value.
   */
  public static Object[] getSortColumnValuesWithPartitionPathAndRecordKey(HoodieRecord record,
                                                                          String[] columns,
                                                                          Schema schema,
                                                                          boolean suffixRecordKey,
                                                                          boolean consistentLogicalTimestampEnabled) {
    try {
      GenericRecord genericRecord = (GenericRecord) record.toIndexedRecord(schema, PROPERTIES).get().getData();
      int numColumns = columns.length;
      Object[] values = new Object[columns.length + 1 + (suffixRecordKey ? 1 : 0)];
      values[0] = record.getPartitionPath();
      for (int i = 0; i < columns.length; i++) {
        values[i + 1] = (HoodieAvroUtils.getNestedFieldVal(genericRecord, columns[i], true, consistentLogicalTimestampEnabled));
      }
      if (suffixRecordKey) {
        values[numColumns + 1] = (record.getRecordKey());
      }
      return values;
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
  public static Object getRecordColumnValues(HoodieRecord record,
                                             String[] columns,
                                             SerializableSchema schema, boolean consistentLogicalTimestampEnabled) {
    return getRecordColumnValues(record, columns, schema.get(), consistentLogicalTimestampEnabled);
  }

  // TODO java-doc
  public static GenericRecord rewriteRecordWithNewSchema(IndexedRecord oldRecord, Schema newSchema) {
    return rewriteRecordWithNewSchema(oldRecord, newSchema, Collections.emptyMap());
  }

  /**
   * Given a avro record with a given schema, rewrites it into the new schema while setting fields only from the new schema.
   * support deep rewrite for nested record.
   * This particular method does the following things :
   * a) Create a new empty GenericRecord with the new schema.
   * b) For GenericRecord, copy over the data from the old schema to the new schema or set default values for all fields of this transformed schema
   *
   * @param oldRecord  oldRecord to be rewritten
   * @param newSchema  newSchema used to rewrite oldRecord
   * @param renameCols a map store all rename cols, (k, v)-> (colNameFromNewSchema, colNameFromOldSchema)
   * @return newRecord for new Schema
   */
  public static GenericRecord rewriteRecordWithNewSchema(IndexedRecord oldRecord, Schema newSchema, Map<String, String> renameCols) {
    Object newRecord = rewriteRecordWithNewSchema(oldRecord, oldRecord.getSchema(), newSchema, renameCols, new LinkedList<>(), false);
    return (GenericData.Record) newRecord;
  }

  public static GenericRecord rewriteRecordWithNewSchema(IndexedRecord oldRecord, Schema newSchema, Map<String, String> renameCols, boolean validate) {
    Object newRecord = rewriteRecordWithNewSchema(oldRecord, oldRecord.getSchema(), newSchema, renameCols, new LinkedList<>(), validate);
    return (GenericData.Record) newRecord;
  }

  /**
   * Given a avro record with a given schema, rewrites it into the new schema while setting fields only from the new schema.
   * support deep rewrite for nested record and adjust rename operation.
   * This particular method does the following things :
   * a) Create a new empty GenericRecord with the new schema.
   * b) For GenericRecord, copy over the data from the old schema to the new schema or set default values for all fields of this transformed schema
   *
   * @param oldRecord     oldRecord to be rewritten
   * @param oldAvroSchema old avro schema.
   * @param newSchema     newSchema used to rewrite oldRecord
   * @param renameCols    a map store all rename cols, (k, v)-> (colNameFromNewSchema, colNameFromOldSchema)
   * @param fieldNames    track the full name of visited field when we travel new schema.
   * @return newRecord for new Schema
   */

  private static Object rewriteRecordWithNewSchema(Object oldRecord, Schema oldAvroSchema, Schema newSchema, Map<String, String> renameCols, Deque<String> fieldNames, boolean validate) {
    if (oldRecord == null) {
      return null;
    }
    if (oldAvroSchema.equals(newSchema)) {
      // there is no need to rewrite if the schema equals.
      return oldRecord;
    }
    // try to get real schema for union type
    Schema oldSchema = getActualSchemaFromUnion(oldAvroSchema, oldRecord);
    Object newRecord = rewriteRecordWithNewSchemaInternal(oldRecord, oldSchema, newSchema, renameCols, fieldNames, false);
    // validation is recursive so it only needs to be called on the original input
    if (validate && !ConvertingGenericData.INSTANCE.validate(newSchema, newRecord)) {
      throw new SchemaCompatibilityException(
          "Unable to validate the rewritten record " + oldRecord + " against schema " + newSchema);
    }
    return newRecord;
  }

  private static Object rewriteRecordWithNewSchemaInternal(Object oldRecord, Schema oldSchema, Schema newSchema, Map<String, String> renameCols, Deque<String> fieldNames, boolean skipMetadataFields) {
    switch (newSchema.getType()) {
      case RECORD:
        if (!(oldRecord instanceof IndexedRecord)) {
          throw new SchemaCompatibilityException("cannot rewrite record with different type");
        }
        IndexedRecord indexedRecord = (IndexedRecord) oldRecord;
        List<Schema.Field> fields = newSchema.getFields();
        GenericData.Record newRecord = new GenericData.Record(newSchema);
        for (int i = 0; i < fields.size(); i++) {
          Schema.Field field = fields.get(i);
          String fieldName = field.name();
          if (!skipMetadataFields || !isMetadataField(fieldName)) {
            fieldNames.push(fieldName);
            Schema.Field oldField = oldSchema.getField(field.name());
            if (oldField != null && !renameCols.containsKey(field.name())) {
              newRecord.put(i, rewriteRecordWithNewSchema(indexedRecord.get(oldField.pos()), oldField.schema(), field.schema(), renameCols, fieldNames, false));
            } else {
              String fieldFullName = createFullName(fieldNames);
              String fieldNameFromOldSchema = renameCols.get(fieldFullName);
              // deal with rename
              Schema.Field oldFieldRenamed = fieldNameFromOldSchema == null ? null : oldSchema.getField(fieldNameFromOldSchema);
              if (oldFieldRenamed != null) {
                // find rename
                newRecord.put(i, rewriteRecordWithNewSchema(indexedRecord.get(oldFieldRenamed.pos()), oldFieldRenamed.schema(), field.schema(), renameCols, fieldNames, false));
              } else {
                // deal with default value
                if (field.defaultVal() instanceof JsonProperties.Null) {
                  newRecord.put(i, null);
                } else {
                  if (!isNullable(field.schema()) && field.defaultVal() == null) {
                    throw new SchemaCompatibilityException("Field " + fieldFullName + " has no default value and is non-nullable");
                  }
                  newRecord.put(i, field.defaultVal());
                }
              }
            }
            fieldNames.pop();
          }
        }
        return newRecord;
      case ENUM:
        if (oldSchema.getType() != Schema.Type.STRING && oldSchema.getType() != Schema.Type.ENUM) {
          throw new SchemaCompatibilityException(String.format("Only ENUM or STRING type can be converted ENUM type. Schema type was %s", oldSchema.getType().getName()));
        }
        if (oldSchema.getType() == Schema.Type.STRING) {
          return new GenericData.EnumSymbol(newSchema, oldRecord);
        }
        return oldRecord;
      case ARRAY:
        if (!(oldRecord instanceof Collection)) {
          throw new SchemaCompatibilityException(String.format("Cannot rewrite %s as an array", oldRecord.getClass().getName()));
        }
        Collection array = (Collection) oldRecord;
        List<Object> newArray = new ArrayList<>(array.size());
        fieldNames.push("element");
        for (Object element : array) {
          newArray.add(rewriteRecordWithNewSchema(element, oldSchema.getElementType(), newSchema.getElementType(), renameCols, fieldNames, false));
        }
        fieldNames.pop();
        return newArray;
      case MAP:
        if (!(oldRecord instanceof Map)) {
          throw new SchemaCompatibilityException(String.format("Cannot rewrite %s as a map", oldRecord.getClass().getName()));
        }
        Map<Object, Object> map = (Map<Object, Object>) oldRecord;
        Map<Object, Object> newMap = new HashMap<>(map.size(), 1.0f);
        fieldNames.push("value");
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
          newMap.put(entry.getKey(), rewriteRecordWithNewSchema(entry.getValue(), oldSchema.getValueType(), newSchema.getValueType(), renameCols, fieldNames, false));
        }
        fieldNames.pop();
        return newMap;
      case UNION:
        return rewriteRecordWithNewSchema(oldRecord, getActualSchemaFromUnion(oldSchema, oldRecord), getActualSchemaFromUnion(newSchema, oldRecord), renameCols, fieldNames, false);
      default:
        return rewritePrimaryType(oldRecord, oldSchema, newSchema);
    }
  }

  public static String createFullName(Deque<String> fieldNames) {
    String result = "";
    if (!fieldNames.isEmpty()) {
      Iterator<String> iter = fieldNames.descendingIterator();
      result = iter.next();
      if (!iter.hasNext()) {
        return result;
      }

      StringBuilder sb = new StringBuilder();
      sb.append(result);
      while (iter.hasNext()) {
        sb.append(".");
        sb.append(iter.next());
      }
      result = sb.toString();
    }
    return result;
  }

  public static Object rewritePrimaryType(Object oldValue, Schema oldSchema, Schema newSchema) {
    if (oldSchema.getType() == newSchema.getType()) {
      switch (oldSchema.getType()) {
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
          if (oldSchema.getFixedSize() != newSchema.getFixedSize()) {
            // Check whether this is a [[Decimal]]'s precision change
            if (oldSchema.getLogicalType() instanceof Decimal) {
              final byte[] bytes;
              bytes = ((GenericFixed) oldValue).bytes();
              Decimal decimal = (Decimal) oldSchema.getLogicalType();
              BigDecimal bd = new BigDecimal(new BigInteger(bytes), decimal.getScale()).setScale(((Decimal) newSchema.getLogicalType()).getScale());
              return DECIMAL_CONVERSION.toFixed(bd, newSchema, newSchema.getLogicalType());
            } else {
              throw new HoodieAvroSchemaException("Fixed type size change is not currently supported");
            }
          }

          // For [[Fixed]] data type both size and name have to match
          //
          // NOTE: That for values wrapped into [[Union]], to make sure that reverse lookup (by
          //       full-name) is working we have to make sure that both schema's name and namespace
          //       do match
          if (Objects.equals(oldSchema.getFullName(), newSchema.getFullName())) {
            return oldValue;
          } else {
            return new GenericData.Fixed(newSchema, ((GenericFixed) oldValue).bytes());
          }

        default:
          throw new HoodieAvroSchemaException("Unknown schema type: " + newSchema.getType());
      }
    } else {
      return rewritePrimaryTypeWithDiffSchemaType(oldValue, oldSchema, newSchema);
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
          return ByteBuffer.wrap(getUTF8Bytes(oldValue.toString()));
        }
        break;
      case STRING:
        if (oldSchema.getType() == Schema.Type.ENUM) {
          return String.valueOf(oldValue);
        }
        if (oldSchema.getType() == Schema.Type.BYTES) {
          return StringUtils.fromUTF8Bytes(((ByteBuffer) oldValue).array());
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
            // due to Java, there will be precision problems in direct conversion, we should use string instead of use double
            BigDecimal bigDecimal = new java.math.BigDecimal(oldValue.toString()).setScale(decimal.getScale(), RoundingMode.HALF_UP);
            return DECIMAL_CONVERSION.toFixed(bigDecimal, newSchema, newSchema.getLogicalType());
          } else if (oldSchema.getType() == Schema.Type.BYTES) {
            return convertBytesToFixed(((ByteBuffer) oldValue).array(), newSchema);
          }
        }
        break;
      default:
    }
    throw new HoodieAvroSchemaException(String.format("cannot support rewrite value for schema type: %s since the old schema type is: %s", newSchema, oldSchema));
  }

  /**
   * bytes is the result of BigDecimal.unscaledValue().toByteArray();
   * This is also what Conversions.DecimalConversion.toBytes() outputs inside a byte buffer
   */
  public static Object convertBytesToFixed(byte[] bytes, Schema schema) {
    LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) schema.getLogicalType();
    BigDecimal bigDecimal = convertBytesToBigDecimal(bytes, decimal);
    return DECIMAL_CONVERSION.toFixed(bigDecimal, schema, decimal);
  }

  /**
   * Use this instead of DECIMAL_CONVERSION.fromBytes() because that method does not add in precision
   *
   * bytes is the result of BigDecimal.unscaledValue().toByteArray();
   * This is also what Conversions.DecimalConversion.toBytes() outputs inside a byte buffer
   */
  public static BigDecimal convertBytesToBigDecimal(byte[] value, LogicalTypes.Decimal decimal) {
    return new BigDecimal(new BigInteger(value),
        decimal.getScale(), new MathContext(decimal.getPrecision(), RoundingMode.HALF_UP));
  }

  public static boolean hasDecimalField(Schema schema) {
    return hasDecimalWithCondition(schema, unused -> true);
  }

  /**
   * Checks whether the provided schema contains a decimal with a precision less than or equal to 18,
   * which allows the decimal to be stored as int/long instead of a fixed size byte array in
   * <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md">parquet logical types</a>
   * @param schema the input schema to search
   * @return true if the schema contains a small precision decimal field and false otherwise
   */
  public static boolean hasSmallPrecisionDecimalField(Schema schema) {
    return hasDecimalWithCondition(schema, HoodieAvroUtils::isSmallPrecisionDecimalField);
  }

  private static boolean hasDecimalWithCondition(Schema schema, Function<Decimal, Boolean> condition) {
    switch (schema.getType()) {
      case RECORD:
        for (Field field : schema.getFields()) {
          if (hasDecimalWithCondition(field.schema(), condition)) {
            return true;
          }
        }
        return false;
      case ARRAY:
        return hasDecimalWithCondition(schema.getElementType(), condition);
      case MAP:
        return hasDecimalWithCondition(schema.getValueType(), condition);
      case UNION:
        return hasDecimalWithCondition(getActualSchemaFromUnion(schema, null), condition);
      default:
        if (schema.getLogicalType() instanceof LogicalTypes.Decimal) {
          Decimal decimal = (Decimal) schema.getLogicalType();
          return condition.apply(decimal);
        } else {
          return false;
        }
    }
  }

  private static boolean isSmallPrecisionDecimalField(Decimal decimal) {
    return decimal.getPrecision() <= 18;
  }

  /**
   * Checks whether the provided schema contains a list or map field.
   * @param schema input
   * @return true if a list or map is present, false otherwise
   */
  public static boolean hasListOrMapField(Schema schema) {
    switch (schema.getType()) {
      case RECORD:
        for (Field field : schema.getFields()) {
          if (hasListOrMapField(field.schema())) {
            return true;
          }
        }
        return false;
      case ARRAY:
      case MAP:
        return true;
      case UNION:
        return hasListOrMapField(getActualSchemaFromUnion(schema, null));
      default:
        return false;
    }
  }

  /**
   * Avro does not support type promotion from numbers to string. This function returns true if
   * it will be necessary to rewrite the record to support this promotion.
   * NOTE: this does not determine whether the writerSchema and readerSchema are compatible.
   * It is just trying to find if the reader expects a number to be promoted to string, as quick as possible.
   */
  public static boolean recordNeedsRewriteForExtendedAvroTypePromotion(Schema writerSchema, Schema readerSchema) {
    if (writerSchema.equals(readerSchema)) {
      return false;
    }
    switch (readerSchema.getType()) {
      case RECORD:
        for (Schema.Field field : readerSchema.getFields()) {
          Schema.Field writerField = writerSchema.getField(field.name());
          if (writerField != null) {
            if (recordNeedsRewriteForExtendedAvroTypePromotion(writerField.schema(), field.schema())) {
              return true;
            }
          }
        }
        return false;
      case ARRAY:
        if (writerSchema.getType().equals(ARRAY)) {
          return recordNeedsRewriteForExtendedAvroTypePromotion(writerSchema.getElementType(), readerSchema.getElementType());
        }
        return false;
      case MAP:
        if (writerSchema.getType().equals(MAP)) {
          return recordNeedsRewriteForExtendedAvroTypePromotion(writerSchema.getValueType(), readerSchema.getValueType());
        }
        return false;
      case UNION:
        return recordNeedsRewriteForExtendedAvroTypePromotion(getActualSchemaFromUnion(writerSchema, null), getActualSchemaFromUnion(readerSchema, null));
      case ENUM:
        return needsRewriteToString(writerSchema, true);
      case STRING:
      case BYTES:
        return needsRewriteToString(writerSchema, false);
      case DOUBLE:
        // To maintain precision, you need to convert Float -> String -> Double
        return writerSchema.getType().equals(Schema.Type.FLOAT);
      default:
        return false;
    }
  }

  /**
   * Helper for recordNeedsRewriteForExtendedAvroSchemaEvolution. Returns true if schema type is
   * int, long, float, double, or bytes because avro doesn't support evolution from those types to
   * string so some intervention is needed
   */
  private static boolean needsRewriteToString(Schema schema, boolean isEnum) {
    switch (schema.getType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BYTES:
        return true;
      case ENUM:
        return !isEnum;
      default:
        return false;
    }
  }

  /**
   * convert days to Date
   * <p>
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
   * <p>
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
    if (schema.getType() != UNION) {
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
    } else if (data == null) {
      return schema;
    } else {
      // deal complex union. this should not happen in hoodie,
      // since flink/spark do not write this type.
      int i = GenericData.get().resolveUnion(schema, data);
      actualSchema = schema.getTypes().get(i);
    }
    return actualSchema;
  }

  public static HoodieRecord createHoodieRecordFromAvro(
      IndexedRecord data,
      String payloadClass,
      String[] preCombineFields,
      Option<Pair<String, String>> simpleKeyGenFieldsOpt,
      Boolean withOperation,
      Option<String> partitionNameOp,
      Boolean populateMetaFields,
      Option<Schema> schemaWithoutMetaFields) {
    if (populateMetaFields) {
      return SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) data,
          payloadClass, preCombineFields, withOperation);
    } else if (simpleKeyGenFieldsOpt.isPresent()) {
      return SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) data,
          payloadClass, preCombineFields, simpleKeyGenFieldsOpt.get(), withOperation, partitionNameOp, schemaWithoutMetaFields);
    } else {
      return SpillableMapUtils.convertToHoodieRecordPayload((GenericRecord) data,
          payloadClass, preCombineFields, withOperation, partitionNameOp, schemaWithoutMetaFields);
    }
  }

  /**
   * Given avro records, rewrites them with new schema.
   *
   * @param oldRecords oldRecords to be rewritten
   * @param newSchema  newSchema used to rewrite oldRecord
   * @param renameCols a map store all rename cols, (k, v)-> (colNameFromNewSchema, colNameFromOldSchema)
   * @return a iterator of rewritten GenericRecords
   */
  public static Iterator<GenericRecord> rewriteRecordWithNewSchema(Iterator<GenericRecord> oldRecords, Schema newSchema, Map<String, String> renameCols, boolean validate) {
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
        return rewriteRecordWithNewSchema(oldRecords.next(), newSchema, renameCols, validate);
      }
    };
  }

  public static Iterator<GenericRecord> rewriteRecordWithNewSchema(Iterator<GenericRecord> oldRecords, Schema newSchema, Map<String, String> renameCols) {
    return rewriteRecordWithNewSchema(oldRecords, newSchema, Collections.EMPTY_MAP, false);
  }

  public static GenericRecord rewriteRecordDeep(GenericRecord oldRecord, Schema newSchema) {
    return rewriteRecordWithNewSchema(oldRecord, newSchema, Collections.EMPTY_MAP);
  }

  public static GenericRecord rewriteRecordDeep(GenericRecord oldRecord, Schema newSchema, boolean validate) {
    return rewriteRecordWithNewSchema(oldRecord, newSchema, Collections.EMPTY_MAP, validate);
  }

  public static boolean gteqAvro1_9() {
    return StringUtils.compareVersions(AVRO_VERSION, "1.9") >= 0;
  }

  public static boolean gteqAvro1_10() {
    return StringUtils.compareVersions(AVRO_VERSION, "1.10") >= 0;
  }

  /**
   * Wraps a value into Avro type wrapper.
   *
   * @param value Java value.
   * @return A wrapped value with Avro type wrapper.
   */
  public static Object wrapValueIntoAvro(Comparable<?> value) {
    if (value == null) {
      return null;
    } else if (value instanceof Date) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode value manually
      LocalDate localDate = ((Date) value).toLocalDate();
      return DateWrapper.newBuilder(DATE_WRAPPER_BUILDER_STUB.get())
          .setValue((int) localDate.toEpochDay())
          .build();
    } else if (value instanceof LocalDate) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode value manually
      LocalDate localDate = (LocalDate) value;
      return LocalDateWrapper.newBuilder(LOCAL_DATE_WRAPPER_BUILDER_STUB.get())
          .setValue((int) localDate.toEpochDay())
          .build();
    } else if (value instanceof BigDecimal) {
      Schema valueSchema = DecimalWrapper.SCHEMA$.getField("value").schema();
      BigDecimal upcastDecimal = tryUpcastDecimal((BigDecimal) value, (LogicalTypes.Decimal) valueSchema.getLogicalType());
      return DecimalWrapper.newBuilder(DECIMAL_WRAPPER_BUILDER_STUB.get())
          .setValue(AVRO_DECIMAL_CONVERSION.toBytes(upcastDecimal, valueSchema, valueSchema.getLogicalType()))
          .build();
    } else if (value instanceof Timestamp) {
      // NOTE: Due to breaking changes in code-gen b/w Avro 1.8.2 and 1.10, we can't
      //       rely on logical types to do proper encoding of the native Java types,
      //       and hereby have to encode value manually
      Instant instant = ((Timestamp) value).toInstant();
      return TimestampMicrosWrapper.newBuilder(TIMESTAMP_MICROS_WRAPPER_BUILDER_STUB.get())
          .setValue(instantToMicros(instant))
          .build();
    } else if (value instanceof Boolean) {
      return BooleanWrapper.newBuilder(BOOLEAN_WRAPPER_BUILDER_STUB.get()).setValue((Boolean) value).build();
    } else if (value instanceof Integer) {
      return IntWrapper.newBuilder(INT_WRAPPER_BUILDER_STUB.get()).setValue((Integer) value).build();
    } else if (value instanceof Long) {
      return LongWrapper.newBuilder(LONG_WRAPPER_BUILDER_STUB.get()).setValue((Long) value).build();
    } else if (value instanceof Float) {
      return FloatWrapper.newBuilder(FLOAT_WRAPPER_BUILDER_STUB.get()).setValue((Float) value).build();
    } else if (value instanceof Double) {
      return DoubleWrapper.newBuilder(DOUBLE_WRAPPER_BUILDER_STUB.get()).setValue((Double) value).build();
    } else if (value instanceof ByteBuffer) {
      return BytesWrapper.newBuilder(BYTES_WRAPPER_BUILDER_STUB.get()).setValue((ByteBuffer) value).build();
    } else if (value instanceof String || value instanceof Utf8) {
      return StringWrapper.newBuilder(STRING_WRAPPER_BUILDER_STUB.get()).setValue(value.toString()).build();
    } else if (value instanceof ArrayComparable) {
      List<Object> avroValues = OrderingValues.getValues((ArrayComparable) value).stream().map(HoodieAvroUtils::wrapValueIntoAvro).collect(Collectors.toList());
      return ArrayWrapper.newBuilder(ARRAY_WRAPPER_BUILDER_STUB.get()).setWrappedValues(avroValues).build();
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the value (%s)", value.getClass()));
    }
  }

  /**
   * Unwraps Avro value wrapper into Java value.
   *
   * @param avroValueWrapper A wrapped value with Avro type wrapper.
   * @return Java value.
   */
  public static Comparable<?> unwrapAvroValueWrapper(Object avroValueWrapper) {
    if (avroValueWrapper == null) {
      return null;
    }

    Pair<Boolean, String> isValueWrapperObfuscated = getIsValueWrapperObfuscated(avroValueWrapper);
    if (isValueWrapperObfuscated.getKey()) {
      return unwrapAvroValueWrapper(avroValueWrapper, isValueWrapperObfuscated.getValue());
    }

    if (avroValueWrapper instanceof DateWrapper) {
      return Date.valueOf(LocalDate.ofEpochDay(((DateWrapper) avroValueWrapper).getValue()));
    } else if (avroValueWrapper instanceof LocalDateWrapper) {
      return LocalDate.ofEpochDay(((LocalDateWrapper) avroValueWrapper).getValue());
    } else if (avroValueWrapper instanceof DecimalWrapper) {
      Schema valueSchema = DecimalWrapper.SCHEMA$.getField("value").schema();
      return AVRO_DECIMAL_CONVERSION.fromBytes(((DecimalWrapper) avroValueWrapper).getValue(), valueSchema, valueSchema.getLogicalType());
    } else if (avroValueWrapper instanceof TimestampMicrosWrapper) {
      return microsToInstant(((TimestampMicrosWrapper) avroValueWrapper).getValue());
    } else if (avroValueWrapper instanceof BooleanWrapper) {
      return ((BooleanWrapper) avroValueWrapper).getValue();
    } else if (avroValueWrapper instanceof IntWrapper) {
      return ((IntWrapper) avroValueWrapper).getValue();
    } else if (avroValueWrapper instanceof LongWrapper) {
      return ((LongWrapper) avroValueWrapper).getValue();
    } else if (avroValueWrapper instanceof FloatWrapper) {
      return ((FloatWrapper) avroValueWrapper).getValue();
    } else if (avroValueWrapper instanceof DoubleWrapper) {
      return ((DoubleWrapper) avroValueWrapper).getValue();
    } else if (avroValueWrapper instanceof BytesWrapper) {
      return ((BytesWrapper) avroValueWrapper).getValue();
    } else if (avroValueWrapper instanceof StringWrapper) {
      return ((StringWrapper) avroValueWrapper).getValue();
    } else if (avroValueWrapper instanceof ArrayWrapper) {
      ArrayWrapper arrayWrapper = (ArrayWrapper) avroValueWrapper;
      return OrderingValues.create(arrayWrapper.getWrappedValues().stream()
          .map(HoodieAvroUtils::unwrapAvroValueWrapper)
          .toArray(Comparable[]::new));
    } else if (avroValueWrapper instanceof GenericRecord) {
      // NOTE: This branch could be hit b/c Avro records could be reconstructed
      //       as {@code GenericRecord)
      // TODO add logical type decoding
      GenericRecord genRec = (GenericRecord) avroValueWrapper;
      return (Comparable<?>) genRec.get("value");
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the value (%s)", avroValueWrapper.getClass()));
    }
  }

  public static Comparable<?> unwrapAvroValueWrapper(Object avroValueWrapper, String wrapperClassName) {
    if (avroValueWrapper == null) {
      return null;
    } else if (DateWrapper.class.getSimpleName().equals(wrapperClassName)) {
      ValidationUtils.checkArgument(avroValueWrapper instanceof GenericRecord);
      return Date.valueOf(LocalDate.ofEpochDay((Integer) ((GenericRecord) avroValueWrapper).get(0)));
    } else if (LocalDateWrapper.class.getSimpleName().equals(wrapperClassName)) {
      ValidationUtils.checkArgument(avroValueWrapper instanceof GenericRecord);
      return LocalDate.ofEpochDay((Integer) ((GenericRecord) avroValueWrapper).get(0));
    } else if (TimestampMicrosWrapper.class.getSimpleName().equals(wrapperClassName)) {
      ValidationUtils.checkArgument(avroValueWrapper instanceof GenericRecord);
      Instant instant = microsToInstant((Long) ((GenericRecord) avroValueWrapper).get(0));
      return Timestamp.from(instant);
    } else if (DecimalWrapper.class.getSimpleName().equals(wrapperClassName)) {
      Schema valueSchema = DecimalWrapper.SCHEMA$.getField("value").schema();
      ValidationUtils.checkArgument(avroValueWrapper instanceof GenericRecord);
      return AVRO_DECIMAL_CONVERSION.fromBytes((ByteBuffer)((GenericRecord) avroValueWrapper).get(0), valueSchema, valueSchema.getLogicalType());
    } else {
      throw new UnsupportedOperationException(String.format("Unsupported type of the value (%s)", avroValueWrapper.getClass()));
    }
  }

  private static Pair<Boolean, String> getIsValueWrapperObfuscated(Object statsValue) {
    if (statsValue != null) {
      String statsValueSchemaClassName = ((GenericRecord) statsValue).getSchema().getName();
      boolean toReturn = statsValueSchemaClassName.equals(DateWrapper.class.getSimpleName())
          || statsValueSchemaClassName.equals(LocalDateWrapper.class.getSimpleName())
          || statsValueSchemaClassName.equals(TimestampMicrosWrapper.class.getSimpleName())
          || statsValueSchemaClassName.equals(DecimalWrapper.class.getSimpleName());
      if (toReturn) {
        return Pair.of(true, ((GenericRecord) statsValue).getSchema().getName());
      }
    }
    return Pair.of(false, null);
  }

  /**
   * Returns field name and the resp data type of the field. The data type will always refer to the leaf node.
   * for eg, for a.b.c, we turn Pair.of(a.b.c, DataType(c))
   * @param schema schema of table.
   * @param fieldName field name of interest.
   * @return
   */
  @VisibleForTesting
  public static Pair<String, Schema.Field> getSchemaForField(Schema schema, String fieldName) {
    return getSchemaForField(schema, fieldName, StringUtils.EMPTY_STRING);
  }

  @VisibleForTesting
  public static Pair<String, Schema.Field> getSchemaForField(Schema schema, String fieldName, String prefix) {
    Schema nonNullableSchema = unwrapNullable(schema);
    if (!fieldName.contains(".")) {
      return Pair.of(prefix + fieldName, nonNullableSchema.getField(fieldName));
    } else {
      int rootFieldIndex = fieldName.indexOf(".");
      Schema.Field rootField = nonNullableSchema.getField(fieldName.substring(0, rootFieldIndex));
      if (rootField == null) {
        throw new HoodieException("Failed to find " + fieldName + " in the table schema ");
      }
      return getSchemaForField(rootField.schema(), fieldName.substring(rootFieldIndex + 1), prefix + fieldName.substring(0, rootFieldIndex + 1));
    }
  }

  public static Object toJavaDefaultValue(Schema.Field field) {
    Object defaultVal = field.defaultVal();
    if (defaultVal == null || defaultVal == org.apache.avro.JsonProperties.NULL_VALUE) {
      return null;
    }

    Schema.Type type = getNonNullType(field.schema());
    switch (type) {
      case STRING:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case ENUM:
      case BYTES:
        return defaultVal;
      case ARRAY:
      case MAP:
      case RECORD:
        // Use Avro's standard GenericData utility for complex types
        return GenericData.get().getDefaultValue(field);
      default:
        throw new IllegalArgumentException("Unsupported Avro type: " + type);
    }
  }

  private static Schema.Type getNonNullType(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      return schema.getTypes().stream()
          .filter(s -> s.getType() != Schema.Type.NULL)
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Only NULL in union"))
          .getType();
    }
    return schema.getType();
  }
}
