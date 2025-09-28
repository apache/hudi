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
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.RewriteAvroPayload;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversions;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.Objects;

import static org.apache.hudi.avro.AvroSchemaUtils.resolveNullableSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldSchemaFromWriteSchema;
import static org.apache.hudi.avro.HoodieAvroUtils.sanitizeName;
import static org.apache.hudi.avro.HoodieAvroWrapperUtils.unwrapAvroValueWrapper;
import static org.apache.hudi.avro.HoodieAvroWrapperUtils.wrapValueIntoAvro;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie avro utilities.
 */
public class TestHoodieAvroUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieAvroUtils.class);

  private static final String EVOLVED_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec1\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"new_col_not_nullable_default_dummy_val\", \"type\": \"string\", \"default\": \"dummy_val\"},"
      + "{\"name\": \"new_col_nullable_wo_default\", \"type\": [\"int\", \"null\"]},"
      + "{\"name\": \"new_col_nullable_default_null\", \"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"new_col_nullable_default_dummy_val\", \"type\": [\"string\" ,\"null\"],\"default\": \"dummy_val\"}]}";

  private static final String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  private static final String EXAMPLE_SCHEMA_WITH_PROPS = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\", \"custom_field_property\":\"value\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}], "
      + "\"custom_schema_property\": \"custom_schema_property_value\"}";

  private static final String EXAMPLE_SCHEMA_WITH_META_FIELDS = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"_hoodie_commit_time\",\"type\": \"string\"},"
      + "{\"name\": \"_hoodie_commit_seqno\",\"type\": \"string\"},"
      + "{\"name\": \"_hoodie_record_key\",\"type\": \"string\"},"
      + "{\"name\": \"_hoodie_partition_path\",\"type\": \"string\"},"
      + "{\"name\": \"_hoodie_file_name\",\"type\": \"string\"},"
      + "{\"name\": \"timestamp\",\"type\": \"double\"},"
      + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  private static final int NUM_FIELDS_IN_EXAMPLE_SCHEMA = 4;

  private static final String SCHEMA_WITH_METADATA_FIELD = "{\"type\": \"record\",\"name\": \"testrec2\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"nullable_field_wo_default\",\"type\": [\"null\" ,\"string\"]}]}";

  private static final String SCHEMA_WITH_NON_NULLABLE_FIELD =
      "{\"type\": \"record\",\"name\": \"testrec3\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"non_nullable_field_wo_default\",\"type\": \"string\"},"
      + "{\"name\": \"non_nullable_field_with_default\",\"type\": \"string\", \"default\": \"dummy\"}]}";

  private static final String SCHEMA_WITH_NON_NULLABLE_FIELD_WITH_DEFAULT =
      "{\"type\": \"record\",\"name\": \"testrec4\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
      + "{\"name\": \"non_nullable_field_with_default\",\"type\": \"string\", \"default\": \"dummy\"}]}";

  private static final String SCHEMA_WITH_DECIMAL_FIELD = "{\"type\":\"record\",\"name\":\"record\",\"fields\":["
      + "{\"name\":\"key_col\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"decimal_col\",\"type\":[\"null\","
      + "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":8,\"scale\":4}],\"default\":null}]}";

  public static String SCHEMA_WITH_NESTED_FIELD_STR = "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":\"string\"},"
      + "{\"name\":\"lastname\",\"type\":\"string\"},"
      + "{\"name\":\"student\",\"type\":{\"name\":\"student\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"firstnameNested\",\"type\":[\"null\" ,\"string\"],\"default\": null},{\"name\":\"lastnameNested\",\"type\":[\"null\" ,\"string\"],\"default\": null}]}}]}";

  private static final String SCHEMA_WITH_NESTED_FIELD_RENAMED =
      "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
      + "{\"name\":\"fn\",\"type\":\"string\"},"
      + "{\"name\":\"ln\",\"type\":\"string\"},"
      + "{\"name\":\"ss\",\"type\":{\"name\":\"ss\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"fnn\",\"type\":[\"null\" ,\"string\"],\"default\": null},{\"name\":\"lnn\",\"type\":[\"null\" ,\"string\"],\"default\": null}]}}]}";

  public static final String SCHEMA_WITH_AVRO_TYPES_STR = "{\"name\":\"TestRecordAvroTypes\",\"type\":\"record\",\"fields\":["
      // Primitive types
      + "{\"name\":\"booleanField\",\"type\":\"boolean\"},"
      + "{\"name\":\"intField\",\"type\":\"int\"},"
      + "{\"name\":\"longField\",\"type\":\"long\"},"
      + "{\"name\":\"floatField\",\"type\":\"float\"},"
      + "{\"name\":\"doubleField\",\"type\":\"double\"},"
      + "{\"name\":\"bytesField\",\"type\":\"bytes\"},"
      + "{\"name\":\"stringField\",\"type\":\"string\"},"
      + "{\"name\":\"secondLevelField\",\"type\":[\"null\", {\"name\":\"secondLevelField\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"lastname\",\"type\":[\"null\",\"string\"],\"default\":null}"
      + "]}],\"default\":null},"
      // Logical types
      + "{\"name\":\"decimalField\",\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":5},"
      + "{\"name\":\"timeMillisField\",\"type\":\"int\",\"logicalType\":\"time-millis\"},"
      + "{\"name\":\"timeMicrosField\",\"type\":\"long\",\"logicalType\":\"time-micros\"},"
      + "{\"name\":\"timestampMillisField\",\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},"
      + "{\"name\":\"timestampMicrosField\",\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},"
      + "{\"name\":\"localTimestampMillisField\",\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"},"
      + "{\"name\":\"localTimestampMicrosField\",\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}"
      + "]}";

  private static final Schema SCHEMA_WITH_AVRO_TYPES = new Schema.Parser().parse(SCHEMA_WITH_AVRO_TYPES_STR);

  // Define schema with a nested field containing a union type
  private static final String NESTED_SCHEMA_WITH_UNION = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"name\": \"NestedRecordWithUnion\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"student\",\n"
      + "      \"type\": [\n"
      + "        \"null\",\n"
      + "        {\n"
      + "          \"type\": \"record\",\n"
      + "          \"name\": \"Student\",\n"
      + "          \"fields\": [\n"
      + "            {\"name\": \"firstname\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
      + "            {\"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null}\n"
      + "          ]\n"
      + "        }\n"
      + "      ],\n"
      + "      \"default\": null\n"
      + "    }\n"
      + "  ]\n"
      + "}";

  public static String SCHEMA_WITH_NESTED_FIELD_LARGE_STR = "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":\"string\"},"
      + "{\"name\":\"lastname\",\"type\":\"string\"},"
      + "{\"name\":\"nested_field\",\"type\":[\"null\"," + SCHEMA_WITH_AVRO_TYPES_STR + "],\"default\":null},"
      + "{\"name\":\"student\",\"type\":{\"name\":\"student\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":[\"null\" ,\"string\"],\"default\": null},{\"name\":\"lastname\",\"type\":[\"null\" ,\"string\"],\"default\": null}]}}]}";

  public static Schema SCHEMA_WITH_NESTED_FIELD_LARGE = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD_LARGE_STR);

  @Test
  public void testPropsPresent() {
    Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    boolean piiPresent = false;
    for (Schema.Field field : schema.getFields()) {
      if (HoodieAvroUtils.isMetadataField(field.name())) {
        continue;
      }

      assertNotNull(field.name(), "field name is null");
      Map<String, Object> props = field.getObjectProps();
      assertNotNull(props, "The property is null");

      if (field.name().equals("pii_col")) {
        piiPresent = true;
        assertTrue(props.containsKey("column_category"), "sensitivity_level is removed in field 'pii_col'");
      } else {
        assertEquals(0, props.size(), "The property shows up but not set");
      }
    }
    assertTrue(piiPresent, "column pii_col doesn't show up");
  }

  @Test
  public void testDefaultValue() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    Schema schemaWithMetadata = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EVOLVED_SCHEMA));
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, schemaWithMetadata);
    assertEquals("dummy_val", rec1.get("new_col_not_nullable_default_dummy_val"));
    assertNull(rec1.get("new_col_nullable_wo_default"));
    assertNull(rec1.get("new_col_nullable_default_null"));
    assertEquals("dummy_val", rec1.get("new_col_nullable_default_dummy_val"));
    assertNull(rec1.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
  }

  @Test
  public void testDefaultValueWithSchemaEvolution() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(EVOLVED_SCHEMA));
    assertEquals("dummy_val", rec1.get("new_col_not_nullable_default_dummy_val"));
    assertNull(rec1.get("new_col_nullable_wo_default"));
  }

  @Test
  public void testMetadataField() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_METADATA_FIELD));
    assertNull(rec1.get("_hoodie_commit_time"));
    assertNull(rec1.get("nullable_field"));
    assertNull(rec1.get("nullable_field_wo_default"));
  }

  @Test
  public void testJoinedGenericRecord() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);

    GenericRecord rec1 = new JoinedGenericRecord(rec, 5, new Schema.Parser().parse(EXAMPLE_SCHEMA_WITH_META_FIELDS));
    assertNull(rec1.get("_hoodie_commit_time"));
    assertNull(rec1.get("_hoodie_record_key"));

    assertEquals(rec.get("_row_key"), rec1.get("_row_key"));
    assertEquals(rec.get("_row_key"), rec1.get(6));
    assertEquals(rec.get("non_pii_col"), rec1.get("non_pii_col"));
    assertEquals(rec.get("non_pii_col"), rec1.get(7));
    assertEquals(rec.get("pii_col"), rec1.get("pii_col"));
    assertEquals(rec.get("pii_col"), rec1.get(8));
    assertEquals(rec.get("timestamp"), rec1.get("timestamp"));
    assertEquals(rec.get("timestamp"), rec1.get(5));

    // lets add meta field values and validate
    rec1.put(0, "commitTime1");
    rec1.put(1, "commitSecNo1");
    rec1.put(2, "recKey1");
    rec1.put(3, "pPath1");
    rec1.put(4, "fileName");

    assertEquals("commitTime1", rec1.get(0));
    assertEquals("commitTime1", rec1.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD));
    assertEquals("commitSecNo1", rec1.get(1));
    assertEquals("commitSecNo1", rec1.get(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD));
    assertEquals("recKey1", rec1.get(2));
    assertEquals("recKey1", rec1.get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
    assertEquals("pPath1", rec1.get(3));
    assertEquals("pPath1", rec1.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD));
    assertEquals("fileName", rec1.get(4));
    assertEquals("fileName", rec1.get(HoodieRecord.FILENAME_METADATA_FIELD));
  }

  @Test
  public void testNonNullableFieldWithoutDefault() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    assertThrows(SchemaCompatibilityException.class, () -> HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_NON_NULLABLE_FIELD)));
  }

  @Test
  public void testNonNullableFieldWithDefault() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(SCHEMA_WITH_NON_NULLABLE_FIELD_WITH_DEFAULT));
    assertEquals("dummy", rec1.get("non_nullable_field_with_default"));
  }

  @Test
  public void testJsonNodeNullWithDefaultValues() {
    List<Schema.Field> fields = new ArrayList<>();
    Schema initialSchema = Schema.createRecord("test_record", "test record", "org.test.namespace", false);
    Schema.Field field1 = new Schema.Field("key", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field field2 = new Schema.Field("key1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field field3 = new Schema.Field("key2", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    fields.add(field1);
    fields.add(field2);
    fields.add(field3);
    initialSchema.setFields(fields);
    GenericRecord rec = new GenericData.Record(initialSchema);
    rec.put("key", "val");
    rec.put("key1", "val1");
    rec.put("key2", "val2");

    List<Schema.Field> evolvedFields = new ArrayList<>();
    Schema evolvedSchema = Schema.createRecord("evolved_record", "evolved record", "org.evolved.namespace", false);
    Schema.Field evolvedField1 = new Schema.Field("key", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField2 = new Schema.Field("key1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField3 = new Schema.Field("key2", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField4 = new Schema.Field("evolved_field", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    Schema.Field evolvedField5 = new Schema.Field("evolved_field1", HoodieAvroUtils.METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    evolvedFields.add(evolvedField1);
    evolvedFields.add(evolvedField2);
    evolvedFields.add(evolvedField3);
    evolvedFields.add(evolvedField4);
    evolvedFields.add(evolvedField5);
    evolvedSchema.setFields(evolvedFields);

    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, evolvedSchema);
    //evolvedField4.defaultVal() returns a JsonProperties.Null instance.
    assertNull(rec1.get("evolved_field"));
    //evolvedField5.defaultVal() returns null.
    assertNull(rec1.get("evolved_field1"));
  }

  @Test
  public void testAddingAndRemovingMetadataFields() {
    Schema schemaWithMetaCols = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    assertEquals(NUM_FIELDS_IN_EXAMPLE_SCHEMA + HoodieRecord.HOODIE_META_COLUMNS.size(), schemaWithMetaCols.getFields().size());
    Schema schemaWithoutMetaCols = HoodieAvroUtils.removeMetadataFields(schemaWithMetaCols);
    assertEquals(NUM_FIELDS_IN_EXAMPLE_SCHEMA, schemaWithoutMetaCols.getFields().size());
  }

  @Test
  public void testRemoveFields() {
    // partitioned table test.
    String schemaStr = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
        + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
        + "{\"name\": \"non_pii_col\", \"type\": \"string\"}]}";
    Schema expectedSchema = new Schema.Parser().parse(schemaStr);
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.removeFields(rec, Collections.singleton("pii_col"));
    assertEquals("key1", rec1.get("_row_key"));
    assertEquals("val1", rec1.get("non_pii_col"));
    assertEquals(3.5, rec1.get("timestamp"));
    if (HoodieAvroUtils.gteqAvro1_10()) {
      GenericRecord finalRec1 = rec1;
      assertThrows(AvroRuntimeException.class, () -> finalRec1.get("pii_col"));
    } else {
      assertNull(rec1.get("pii_col"));
    }
    assertEquals(expectedSchema, rec1.getSchema());

    // non-partitioned table test with empty list of fields.
    schemaStr = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
        + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
        + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
        + "{\"name\": \"pii_col\", \"type\": \"string\"}]}";
    expectedSchema = new Schema.Parser().parse(schemaStr);
    rec1 = HoodieAvroUtils.removeFields(rec, Collections.singleton(""));
    assertEquals(expectedSchema, rec1.getSchema());
  }

  @Test
  public void testGetRootLevelFieldName() {
    assertEquals("a", HoodieAvroUtils.getRootLevelFieldName("a.b.c"));
    assertEquals("a", HoodieAvroUtils.getRootLevelFieldName("a"));
    assertEquals("", HoodieAvroUtils.getRootLevelFieldName(""));
  }

  @Test
  public void testGetNestedFieldVal() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");

    Object rowKey = HoodieAvroUtils.getNestedFieldVal(rec, "_row_key", true, false);
    assertEquals("key1", rowKey);

    Object rowKeyNotExist = HoodieAvroUtils.getNestedFieldVal(rec, "fake_key", true, false);
    assertNull(rowKeyNotExist);

    // Field does not exist
    assertEquals("fake_key(Part -fake_key) field not found in record. Acceptable fields were :[timestamp, _row_key, non_pii_col, pii_col]",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "fake_key", false, false)).getMessage());

    // Field exists while value not
    assertNull(HoodieAvroUtils.getNestedFieldVal(rec, "timestamp", false, false));
  }

  @Test
  public void testGetNestedFieldValWithNestedField() {
    Schema nestedSchema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD_STR);
    GenericRecord rec = new GenericData.Record(nestedSchema);

    // test get .
    assertEquals(". field not found in record. Acceptable fields were :[firstname, lastname, student]",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, ".", false, false)).getMessage());

    // test get fake_key
    assertEquals("fake_key(Part -fake_key) field not found in record. Acceptable fields were :[firstname, lastname, student]",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "fake_key", false, false)).getMessage());

    // test get student(null)
    assertNull(HoodieAvroUtils.getNestedFieldVal(rec, "student", false, false));

    // test get student
    GenericRecord studentRecord = new GenericData.Record(rec.getSchema().getField("student").schema());
    studentRecord.put("firstnameNested", "person");
    rec.put("student", studentRecord);
    assertEquals(studentRecord, HoodieAvroUtils.getNestedFieldVal(rec, "student", false, false));

    // test get student.fake_key
    assertEquals("student.fake_key(Part -fake_key) field not found in record. Acceptable fields were :[firstnameNested, lastnameNested]",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "student.fake_key", false, false)).getMessage());

    // test get student.firstname
    assertEquals("person", HoodieAvroUtils.getNestedFieldVal(rec, "student.firstnameNested", false, false));

    // test get student.lastname(null)
    assertNull(HoodieAvroUtils.getNestedFieldVal(rec, "student.lastnameNested", false, false));

    // test get student.firstname.fake_key
    assertEquals("Cannot find a record at part value :firstnameNested",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "student.firstnameNested.fake_key", false, false)).getMessage());

    // test get student.lastname(null).fake_key
    assertEquals("Cannot find a record at part value :lastnameNested",
        assertThrows(HoodieException.class, () ->
            HoodieAvroUtils.getNestedFieldVal(rec, "student.lastnameNested.fake_key", false, false)).getMessage());
  }

  @Test
  public void testGetNestedFieldValWithDecimalField() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(SCHEMA_WITH_DECIMAL_FIELD));
    rec.put("key_col", "key");
    BigDecimal bigDecimal = new BigDecimal("1234.5678");
    ByteBuffer byteBuffer = ByteBuffer.wrap(bigDecimal.unscaledValue().toByteArray());
    rec.put("decimal_col", byteBuffer);

    Object decimalCol = HoodieAvroUtils.getNestedFieldVal(rec, "decimal_col", true, false);
    assertEquals(bigDecimal, decimalCol);

    Object obj = rec.get(1);
    assertTrue(obj instanceof ByteBuffer);
    ByteBuffer buffer = (ByteBuffer) obj;
    assertEquals(0, buffer.position());
  }

  @Test
  public void testGetNestedFieldSchema() throws IOException {
    Schema schema = SchemaTestUtil.getEvolvedSchema();
    GenericRecord rec = new GenericData.Record(schema);
    rec.put("field1", "key1");
    rec.put("field2", "val1");
    rec.put("name", "val2");
    rec.put("favorite_number", 2);
    // test simple field schema
    assertEquals(Schema.create(Schema.Type.STRING), getNestedFieldSchemaFromWriteSchema(rec.getSchema(), "field1"));

    GenericRecord rec2 = new GenericData.Record(schema);
    rec2.put("field1", "key1");
    rec2.put("field2", "val1");
    rec2.put("name", "val2");
    rec2.put("favorite_number", 12);
    // test comparison of non-string type
    assertEquals(-1, GenericData.get().compare(rec.get("favorite_number"), rec2.get("favorite_number"), getNestedFieldSchemaFromWriteSchema(rec.getSchema(), "favorite_number")));

    // test nested field schema
    Schema nestedSchema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD_STR);
    GenericRecord rec3 = new GenericData.Record(nestedSchema);
    rec3.put("firstname", "person1");
    rec3.put("lastname", "person2");
    GenericRecord studentRecord = new GenericData.Record(rec3.getSchema().getField("student").schema());
    studentRecord.put("firstnameNested", "person1");
    studentRecord.put("lastnameNested", "person2");
    rec3.put("student", studentRecord);

    assertEquals(Schema.create(Schema.Type.STRING), getNestedFieldSchemaFromWriteSchema(rec3.getSchema(), "student.firstnameNested"));
    assertEquals(Schema.create(Schema.Type.STRING), getNestedFieldSchemaFromWriteSchema(nestedSchema, "student.firstnameNested"));
  }

  @Test
  public void testGetNestedFieldSchemaWithUnion() {
    Schema schema = new Schema.Parser().parse(NESTED_SCHEMA_WITH_UNION);
    // Create a record for the schema
    GenericRecord rec = new GenericData.Record(schema);
    Schema studentSchema = schema.getField("student").schema().getTypes().get(1); // Resolve union schema for "student"
    GenericRecord studentRecord = new GenericData.Record(studentSchema);
    studentRecord.put("firstname", "John");
    studentRecord.put("lastname", "Doe");
    rec.put("student", studentRecord);

    // Test nested field schema for "student.firstname"
    Schema expectedFirstnameSchema = Schema.create(Schema.Type.STRING);
    assertEquals(expectedFirstnameSchema, getNestedFieldSchemaFromWriteSchema(schema, "student.firstname"));

    // Test nested field schema for "student.lastname"
    Schema expectedLastnameSchema = Schema.create(Schema.Type.STRING);
    assertEquals(expectedLastnameSchema, getNestedFieldSchemaFromWriteSchema(schema, "student.lastname"));

    // Test nullable handling for "student" (entire field)
    assertEquals(studentSchema, getNestedFieldSchemaFromWriteSchema(schema, "student"));

    // Test exception for invalid nested field
    Exception exception = assertThrows(HoodieException.class, () -> getNestedFieldSchemaFromWriteSchema(schema, "student.middleName"));
    assertTrue(exception.getMessage().contains("Failed to get schema. Not a valid field name"));
  }

  @Test
  public void testReWriteAvroRecordWithNewSchema() {
    Schema nestedSchema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD_STR);
    GenericRecord rec3 = new GenericData.Record(nestedSchema);
    rec3.put("firstname", "person1");
    rec3.put("lastname", "person2");
    GenericRecord studentRecord = new GenericData.Record(rec3.getSchema().getField("student").schema());
    studentRecord.put("firstnameNested", "person3");
    studentRecord.put("lastnameNested", "person4");
    rec3.put("student", studentRecord);

    Schema nestedSchemaRename = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD_RENAMED);
    Map<String, String> colRenames = new HashMap<>();
    colRenames.put("fn", "firstname");
    colRenames.put("ln", "lastname");
    colRenames.put("ss", "student");
    colRenames.put("ss.fnn", "firstnameNested");
    colRenames.put("ss.lnn", "lastnameNested");
    GenericRecord studentRecordRename = HoodieAvroUtils.rewriteRecordWithNewSchema(rec3, nestedSchemaRename, colRenames);
    Assertions.assertTrue(GenericData.get().validate(nestedSchemaRename, studentRecordRename));
    Assertions.assertEquals("person1", studentRecordRename.get("fn"));
    Assertions.assertEquals("person2",  studentRecordRename.get("ln"));
    Assertions.assertEquals("person3", ((GenericRecord) studentRecordRename.get("ss")).get("fnn"));
    Assertions.assertEquals("person4",  ((GenericRecord) studentRecordRename.get("ss")).get("lnn"));
  }

  @Test
  public void testReWriteAvroRecordWithSwappedNames() {
    Schema origNestedSchema = new Schema.Parser().parse(SCHEMA_WITH_NESTED_FIELD_STR);
    GenericRecord rec = new GenericData.Record(origNestedSchema);
    rec.put("firstname", "John");
    rec.put("lastname",  "Doe");

    GenericRecord student = new GenericData.Record(
        origNestedSchema.getField("student").schema());
    student.put("firstnameNested", "Albert");
    student.put("lastnameNested",  "Einstein");
    rec.put("student", student);

    final String SCHEMA_WITH_SWAPPED_NAMES =
        "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
            +   "{\"name\":\"fn\",\"type\":\"string\"},"
            +   "{\"name\":\"firstname\",\"type\":\"string\"},"
            +   "{\"name\":\"student\",\"type\":{\"name\":\"student\",\"type\":\"record\",\"fields\":["
            +     "{\"name\":\"fnn\",\"type\":[\"null\",\"string\"],\"default\":null},"
            +     "{\"name\":\"firstnameNested\",\"type\":[\"null\",\"string\"],\"default\":null}]}}]}";

    Schema swappedSchema = new Schema.Parser().parse(SCHEMA_WITH_SWAPPED_NAMES);

    Map<String, String> renames = new HashMap<>();
    renames.put("fn", "firstname");
    renames.put("firstname", "lastname");
    renames.put("student.fnn", "firstnameNested");
    renames.put("student.firstnameNested", "lastnameNested");

    GenericRecord rewritten = HoodieAvroUtils.rewriteRecordWithNewSchema(rec, swappedSchema, renames);
    Assertions.assertTrue(GenericData.get().validate(swappedSchema, rewritten));
    Assertions.assertEquals("Albert", ((GenericRecord) rewritten.get("student")).get("fnn"));
    Assertions.assertEquals("Einstein",  ((GenericRecord) rewritten.get("student")).get("firstnameNested"));
    Assertions.assertEquals("John", rewritten.get("fn"));
    Assertions.assertEquals("Doe",  rewritten.get("firstname"));
  }

  @Test
  public void testConvertDaysToDate() {
    Date now = new Date(System.currentTimeMillis());
    int days = HoodieAvroUtils.fromJavaDate(now);
    assertEquals(now.toLocalDate(), HoodieAvroUtils.toJavaDate(days).toLocalDate());
  }

  @Test
  public void testSanitizeName() {
    assertEquals("__23456", sanitizeName("123456"));
    assertEquals("abcdef", sanitizeName("abcdef"));
    assertEquals("_1", sanitizeName("_1"));
    assertEquals("a*bc", sanitizeName("a.bc", "*"));
    assertEquals("abcdef___", sanitizeName("abcdef_."));
    assertEquals("__ab__cd__", sanitizeName("1ab*cd?"));
  }

  @Test
  public void testGenerateProjectionSchema() {
    Schema originalSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));

    Schema schema1 = HoodieAvroUtils.generateProjectionSchema(originalSchema, Arrays.asList("_row_key", "timestamp"));
    assertEquals(2, schema1.getFields().size());
    List<String> fieldNames1 = schema1.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    assertTrue(fieldNames1.contains("_row_key"));
    assertTrue(fieldNames1.contains("timestamp"));

    assertTrue(assertThrows(HoodieException.class, () ->
        HoodieAvroUtils.generateProjectionSchema(originalSchema, Arrays.asList("_row_key", "timestamp", "fake_field")))
        .getMessage().contains("Field fake_field not found in log schema. Query cannot proceed!"));
  }

  @Test
  public void testWrapAndUnwrapAvroValues() throws IOException {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_AVRO_TYPES_STR);
    GenericRecord record = new GenericData.Record(schema);
    Map<String, Class> expectedWrapperClass = new HashMap<>();

    record.put("booleanField", true);
    expectedWrapperClass.put("booleanField", BooleanWrapper.class);
    record.put("intField", 698);
    expectedWrapperClass.put("intField", IntWrapper.class);
    record.put("longField", 192485030493L);
    expectedWrapperClass.put("longField", LongWrapper.class);
    record.put("floatField", 18.125f);
    expectedWrapperClass.put("floatField", FloatWrapper.class);
    record.put("doubleField", 94385932.342104);
    expectedWrapperClass.put("doubleField", DoubleWrapper.class);
    record.put("bytesField", ByteBuffer.wrap(new byte[] {1, 20, 0, 60, 2, 108}));
    expectedWrapperClass.put("bytesField", BytesWrapper.class);
    record.put("stringField", "abcdefghijk");
    expectedWrapperClass.put("stringField", StringWrapper.class);
    record.put("decimalField", ByteBuffer.wrap(getUTF8Bytes("9213032.4966")));
    expectedWrapperClass.put("decimalField", BytesWrapper.class);
    record.put("timeMillisField", 57996136);
    expectedWrapperClass.put("timeMillisField", IntWrapper.class);
    record.put("timeMicrosField", 57996136930L);
    expectedWrapperClass.put("timeMicrosField", LongWrapper.class);
    record.put("timestampMillisField", 1690828731156L);
    expectedWrapperClass.put("timestampMillisField", LongWrapper.class);
    record.put("timestampMicrosField", 1690828731156982L);
    expectedWrapperClass.put("timestampMicrosField", LongWrapper.class);
    record.put("localTimestampMillisField", 1690828731156L);
    expectedWrapperClass.put("localTimestampMillisField", LongWrapper.class);
    record.put("localTimestampMicrosField", 1690828731156982L);
    expectedWrapperClass.put("localTimestampMicrosField", LongWrapper.class);

    GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
    writer.write(record, encoder);
    encoder.flush();
    byte[] data = baos.toByteArray();

    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, 0, data.length, null);
    GenericRecord deserializedRecord = reader.read(null, decoder);
    Map<String, Object> fieldValueMapping = deserializedRecord.getSchema().getFields().stream()
        // filtering out nested record field
        .filter(field -> !field.schema().getType().equals(Schema.Type.UNION))
        .collect(Collectors.toMap(
            Schema.Field::name,
            field -> deserializedRecord.get(field.name())
        ));

    for (String fieldName : fieldValueMapping.keySet()) {
      Object value = fieldValueMapping.get(fieldName);
      Object wrapperValue = wrapValueIntoAvro((Comparable) value);
      assertTrue(expectedWrapperClass.get(fieldName).isInstance(wrapperValue));
      if (value instanceof Utf8) {
        assertEquals(value.toString(), ((GenericRecord) wrapperValue).get(0));
        assertEquals(value.toString(), unwrapAvroValueWrapper(wrapperValue));
      } else {
        assertEquals(value, ((GenericRecord) wrapperValue).get(0));
        assertEquals(value, unwrapAvroValueWrapper(wrapperValue));
      }
    }
  }

  @Test
  public void testConvertingGenericDataCompare() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_AVRO_TYPES_STR);
    // create two records with same values
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("booleanField", true);
    record1.put("intField", 698);
    record1.put("longField", 192485030493L);
    record1.put("floatField", 18.125f);
    record1.put("doubleField", 94385932.342104);
    record1.put("bytesField", new byte[] {1, 20, 0, 60, 2, 108});
    record1.put("stringField", "abcdefghijk");
    record1.put("decimalField", getUTF8Bytes("9213032.4966"));
    record1.put("timeMillisField", 57996136);
    record1.put("timeMicrosField", 57996136930L);
    record1.put("timestampMillisField", 1690828731156L);
    record1.put("timestampMicrosField", 1690828731156982L);
    record1.put("localTimestampMillisField", 1690828731156L);
    record1.put("localTimestampMicrosField", 1690828731156982L);

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("booleanField", true);
    record2.put("intField", 698);
    record2.put("longField", 192485030493L);
    record2.put("floatField", 18.125f);
    record2.put("doubleField", 94385932.342104);
    record2.put("bytesField", new byte[] {1, 20, 0, 60, 2, 108});
    record2.put("stringField", "abcdefghijk");
    record2.put("decimalField", getUTF8Bytes("9213032.4966"));
    record2.put("timeMillisField", 57996136);
    record2.put("timeMicrosField", 57996136930L);
    record2.put("timestampMillisField", 1690828731156L);
    record2.put("timestampMicrosField", 1690828731156982L);
    record2.put("localTimestampMillisField", 1690828731156L);
    record2.put("localTimestampMicrosField", 1690828731156982L);

    // get schema of each field in SCHEMA_WITH_AVRO_TYPES
    List<Schema> fieldSchemas = schema.getFields().stream().map(Schema.Field::schema).collect(Collectors.toList());
    // compare each field in SCHEMA_WITH_AVRO_TYPES
    for (int i = 0; i < fieldSchemas.size(); i++) {
      assertEquals(0, ConvertingGenericData.INSTANCE.compare(record1.get(i), record2.get(i), fieldSchemas.get(i)));
    }
  }

  public static Stream<Arguments> javaValueParams() {
    Object[][] data =
        new Object[][] {
            {new Timestamp(1690766971000L), TimestampMicrosWrapper.class},
            {new Date(1672560000000L), DateWrapper.class},
            {LocalDate.of(2023, 1, 1), LocalDateWrapper.class},
            {new BigDecimal("12345678901234.2948"), DecimalWrapper.class}
        };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("javaValueParams")
  public void testWrapAndUnwrapJavaValues(Comparable value, Class expectedWrapper) {
    Object wrapperValue = wrapValueIntoAvro(value);
    assertTrue(expectedWrapper.isInstance(wrapperValue));
    if (value instanceof Timestamp) {
      assertEquals(((Timestamp) value).getTime() * 1000L,
          ((GenericRecord) wrapperValue).get(0));
      assertEquals(((Timestamp) value).getTime(),
          ((Timestamp) unwrapAvroValueWrapper(wrapperValue)).getTime());
    } else if (value instanceof Date) {
      assertEquals((int) ChronoUnit.DAYS.between(
              LocalDate.ofEpochDay(0), ((Date) value).toLocalDate()),
          ((GenericRecord) wrapperValue).get(0));
      assertEquals(((Date)value).toString(), ((Date)unwrapAvroValueWrapper(wrapperValue)).toString());
    } else if (value instanceof LocalDate) {
      assertEquals((int) ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), (LocalDate) value),
          ((GenericRecord) wrapperValue).get(0));
      assertEquals(value, unwrapAvroValueWrapper(wrapperValue));
    } else {
      assertEquals("0.000000000000000",
          ((BigDecimal) value)
              .subtract((BigDecimal) unwrapAvroValueWrapper(wrapperValue)).toPlainString());
    }
  }

  @Test
  public void testAddMetadataFields() {
    Schema baseSchema = new Schema.Parser().parse(EXAMPLE_SCHEMA_WITH_PROPS);
    Schema schemaWithMetadata = HoodieAvroUtils.addMetadataFields(baseSchema);
    List<Schema.Field> updatedFields = schemaWithMetadata.getFields();
    // assert fields added in expected order
    assertEquals(HoodieRecord.COMMIT_TIME_METADATA_FIELD, updatedFields.get(0).name());
    assertEquals(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, updatedFields.get(1).name());
    assertEquals(HoodieRecord.RECORD_KEY_METADATA_FIELD, updatedFields.get(2).name());
    assertEquals(HoodieRecord.PARTITION_PATH_METADATA_FIELD, updatedFields.get(3).name());
    assertEquals(HoodieRecord.FILENAME_METADATA_FIELD, updatedFields.get(4).name());
    // assert original fields are copied over
    List<Schema.Field> originalFieldsInUpdatedSchema = updatedFields.subList(5, updatedFields.size());
    assertEquals(baseSchema.getFields(), originalFieldsInUpdatedSchema);
    // validate properties are properly copied over
    assertEquals("custom_schema_property_value", schemaWithMetadata.getProp("custom_schema_property"));
    assertEquals("value", originalFieldsInUpdatedSchema.get(0).getProp("custom_field_property"));
  }

  @Test
  void testSafeAvroToJsonStringMissingRequiredField() {
    Schema schema = new Schema.Parser().parse(EXAMPLE_SCHEMA);
    GenericRecord record = new GenericData.Record(schema);
    record.put("non_pii_col", "val1");
    record.put("pii_col", "val2");
    record.put("timestamp", 3.5);
    String jsonString = HoodieAvroUtils.safeAvroToJsonString(record);
    assertEquals("{\"timestamp\": 3.5, \"_row_key\": null, \"non_pii_col\": \"val1\", \"pii_col\": \"val2\"}", jsonString);
  }

  @Test
  void testSafeAvroToJsonStringBadDataType() {
    Schema schema = new Schema.Parser().parse(EXAMPLE_SCHEMA);
    GenericRecord record = new GenericData.Record(schema);
    record.put("non_pii_col", "val1");
    record.put("_row_key", "key");
    record.put("pii_col", "val2");
    record.put("timestamp", "foo");
    String jsonString = HoodieAvroUtils.safeAvroToJsonString(record);
    assertEquals("{\"timestamp\": \"foo\", \"_row_key\": \"key\", \"non_pii_col\": \"val1\", \"pii_col\": \"val2\"}", jsonString);
  }

  @Test
  void testConvertBytesToFixed() {
    Random rand = new Random();
    //size calculated using AvroInternalSchemaConverter.computeMinBytesForPrecision
    testConverBytesToFixedHelper(rand.nextDouble(), 13, 7, 6);
    testConverBytesToFixedHelper(rand.nextDouble(), 4, 2, 2);
    testConverBytesToFixedHelper(rand.nextDouble(), 32, 12, 14);
  }

  private static void testConverBytesToFixedHelper(double value, int precision, int scale, int size) {
    BigDecimal decfield = BigDecimal.valueOf(value * Math.pow(10, precision - scale))
        .setScale(scale, RoundingMode.HALF_UP).round(new MathContext(precision, RoundingMode.HALF_UP));
    byte[] encodedDecimal = decfield.unscaledValue().toByteArray();
    Schema fixedSchema = new Schema.Parser().parse("{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [{\"name\": \"decfield\", \"type\": {\"type\": \"fixed\", \"name\": \"idk\","
        + " \"logicalType\": \"decimal\", \"precision\": " + precision + ", \"scale\": " + scale + ", \"size\": " + size + "}}]}").getFields().get(0).schema();
    GenericData.Fixed fixed = (GenericData.Fixed) HoodieAvroUtils.convertBytesToFixed(encodedDecimal, fixedSchema);
    BigDecimal after = new Conversions.DecimalConversion().fromFixed(fixed, fixedSchema, fixedSchema.getLogicalType());
    assertEquals(decfield, after);
  }

  @Test
  void testHasDecimalField() {
    assertTrue(HoodieAvroUtils.hasDecimalField(new Schema.Parser().parse(SCHEMA_WITH_DECIMAL_FIELD)));
    assertFalse(HoodieAvroUtils.hasDecimalField(new Schema.Parser().parse(EVOLVED_SCHEMA)));
    assertFalse(HoodieAvroUtils.hasDecimalField(new Schema.Parser().parse(SCHEMA_WITH_NON_NULLABLE_FIELD)));
    assertTrue(HoodieAvroUtils.hasDecimalField(HoodieTestDataGenerator.AVRO_SCHEMA));
    assertTrue(HoodieAvroUtils.hasDecimalField(HoodieTestDataGenerator.AVRO_TRIP_ENCODED_DECIMAL_SCHEMA));
    Schema recordWithMapAndArray = Schema.createRecord("recordWithMapAndArray", null, null, false,
        Arrays.asList(new Schema.Field("mapfield", Schema.createMap(Schema.create(Schema.Type.INT)), null, null),
            new Schema.Field("arrayfield", Schema.createArray(Schema.create(Schema.Type.INT)), null, null)
        ));
    assertFalse(HoodieAvroUtils.hasDecimalField(recordWithMapAndArray));
    Schema recordWithDecMapAndArray = Schema.createRecord("recordWithDecMapAndArray", null, null, false,
        Arrays.asList(new Schema.Field("mapfield",
                Schema.createMap(LogicalTypes.decimal(10,6).addToSchema(Schema.create(Schema.Type.BYTES))), null, null),
            new Schema.Field("arrayfield", Schema.createArray(Schema.create(Schema.Type.INT)), null, null)
        ));
    assertTrue(HoodieAvroUtils.hasDecimalField(recordWithDecMapAndArray));
    Schema recordWithMapAndDecArray = Schema.createRecord("recordWithMapAndDecArray", null, null, false,
        Arrays.asList(new Schema.Field("mapfield",
            Schema.createMap(Schema.create(Schema.Type.INT)), null, null), new Schema.Field("arrayfield",
            Schema.createArray(LogicalTypes.decimal(10,6).addToSchema(Schema.create(Schema.Type.BYTES))), null, null)
        ));
    assertTrue(HoodieAvroUtils.hasDecimalField(recordWithMapAndDecArray));
  }

  @Test
  void testCreateFullName() {
    String result = HoodieAvroUtils.createFullName(new ArrayDeque<>(Arrays.asList("a", "b", "c")));
    String resultSingle = HoodieAvroUtils.createFullName(new ArrayDeque<>(Collections.singletonList("a")));
    String resultEmpty = HoodieAvroUtils.createFullName(new ArrayDeque<>());
    assertEquals("c.b.a", result);
    assertEquals("a", resultSingle);
    assertEquals("", resultEmpty);
  }

  @Test
  public void testCreateNamePrefix() {
    assertNull(HoodieAvroUtils.createNamePrefix(true, new ArrayDeque<>(Collections.singletonList("field1"))));
    assertEquals("field1", HoodieAvroUtils.createNamePrefix(false, new ArrayDeque<>(Collections.singletonList("field1"))));
    assertNull(HoodieAvroUtils.createNamePrefix(false, new ArrayDeque<>()));
    assertEquals("parent.child", HoodieAvroUtils.createNamePrefix(false, new ArrayDeque<>(Arrays.asList("child", "parent"))));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetSortColumnValuesWithPartitionPathAndRecordKey(boolean suffixRecordKey) {
    Schema schema = new Schema.Parser().parse(EXAMPLE_SCHEMA);
    GenericRecord record = new GenericData.Record(schema);
    record.put("non_pii_col", "val1");
    record.put("pii_col", "val2");
    record.put("timestamp", 3.5);
    HoodieRecordPayload avroPayload = new RewriteAvroPayload(record);
    HoodieAvroRecord avroRecord = new HoodieAvroRecord(new HoodieKey("record1", "partition1"), avroPayload);

    String[] userSortColumns = new String[] {"non_pii_col", "timestamp"};
    Object[] sortColumnValues = HoodieAvroUtils.getSortColumnValuesWithPartitionPathAndRecordKey(avroRecord, userSortColumns, Schema.parse(EXAMPLE_SCHEMA), suffixRecordKey, true);
    if (suffixRecordKey) {
      assertArrayEquals(new Object[] {"partition1", "val1", 3.5, "record1"}, sortColumnValues);
    } else {
      assertArrayEquals(new Object[] {"partition1", "val1", 3.5}, sortColumnValues);
    }
  }

  @Test
  void testHasListOrMapField() {
    Schema nestedList = Schema.createRecord("nestedList", null, null, false, Arrays.asList(
        new Schema.Field("intField", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("nested", Schema.createRecord("nestedSchema", null, null, false, Collections.singletonList(
            new Schema.Field("listField", Schema.createArray(Schema.create(Schema.Type.INT)), null, null)
        )), null, null)
    ));
    Schema nestedMap = Schema.createRecord("nestedMap", null, null, false, Arrays.asList(
        new Schema.Field("intField", Schema.create(Schema.Type.INT), null, null),
        new Schema.Field("nested", Schema.createUnion(Schema.create(Schema.Type.NULL),
            Schema.createRecord("nestedSchema", null, null, false,
                Collections.singletonList(new Schema.Field("mapField", Schema.createMap(Schema.create(Schema.Type.INT)), null, null)
                ))), null, null)
    ));
    assertTrue(HoodieAvroUtils.hasListOrMapField(nestedList));
    assertTrue(HoodieAvroUtils.hasListOrMapField(nestedMap));
    assertFalse(HoodieAvroUtils.hasListOrMapField(new Schema.Parser().parse(EXAMPLE_SCHEMA)));
  }

  @Test
  void testHasSmallPrecisionDecimalField() {
    assertTrue(HoodieAvroUtils.hasSmallPrecisionDecimalField(new Schema.Parser().parse(SCHEMA_WITH_DECIMAL_FIELD)));
    assertFalse(HoodieAvroUtils.hasSmallPrecisionDecimalField(new Schema.Parser().parse(SCHEMA_WITH_AVRO_TYPES_STR)));
    assertFalse(HoodieAvroUtils.hasSmallPrecisionDecimalField(new Schema.Parser().parse(EXAMPLE_SCHEMA)));
  }

  public static Stream<Arguments> getSchemaForFieldParams() {
    Object[][] data =
        new Object[][] {
            {"booleanField", Schema.Type.BOOLEAN},
            {"intField", Schema.Type.INT},
            {"longField", Schema.Type.LONG},
            {"floatField", Schema.Type.FLOAT},
            {"bytesField", Schema.Type.BYTES},
            {"stringField", Schema.Type.STRING},
            {"decimalField", Schema.Type.BYTES},
            {"timestampMillisField", Schema.Type.LONG}
        };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("getSchemaForFieldParams")
  public void testGetSchemaForFieldSimple(String colName, Schema.Type schemaType) {
    Pair<String, Schema.Field> actualColNameAndSchemaFile = HoodieAvroUtils.getSchemaForField(SCHEMA_WITH_AVRO_TYPES, colName);
    assertEquals(colName, actualColNameAndSchemaFile.getKey());
    assertEquals(schemaType, actualColNameAndSchemaFile.getValue().schema().getType());
  }

  public static Stream<Arguments> getSchemaForFieldParamsNested() {
    Object[][] data =
        new Object[][] {
            {"student.firstname", Schema.Type.STRING},
            {"student.lastname", Schema.Type.STRING},
            {"nested_field.booleanField", Schema.Type.BOOLEAN},
            {"nested_field.intField", Schema.Type.INT},
            {"nested_field.longField", Schema.Type.LONG},
            {"nested_field.floatField", Schema.Type.FLOAT},
            {"nested_field.bytesField", Schema.Type.BYTES},
            {"nested_field.stringField", Schema.Type.STRING},
            {"nested_field.decimalField", Schema.Type.BYTES},
            {"nested_field.timestampMillisField", Schema.Type.LONG}
        };
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("getSchemaForFieldParamsNested")
  public void testGetSchemaForFieldNested(String colName, Schema.Type schemaType) {
    Pair<String, Schema.Field> actualColNameAndSchemaFile = HoodieAvroUtils.getSchemaForField(SCHEMA_WITH_NESTED_FIELD_LARGE, colName);
    assertEquals(colName, actualColNameAndSchemaFile.getKey());
    assertEquals(schemaType, resolveNullableSchema(actualColNameAndSchemaFile.getValue().schema()).getType());
  }

  public static Stream<Arguments> getExpectedSchemaForFields() {
    // Projection of two nested fields. secondLevelField is entirely projected since both its fields are included
    List<String> fields1 = Arrays.asList("nested_field.secondLevelField.firstname", "nested_field.secondLevelField.lastname");
    // Expected schema - top level field and one nested field
    String expectedSchema1 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"TestRecordAvroTypes\",\n"
            + "      \"fields\": [\n"
            + "        { \"name\": \"secondLevelField\", \"type\": [\"null\", {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"secondLevelField\",\n"
            + "          \"fields\": [\n"
            + "            { \"name\": \"firstname\", \"type\": [\"null\", \"string\"], \"default\": null },\n"
            + "            { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "          ]\n"
            + "        }], \"default\": null }\n"
            + "      ]\n"
            + "    }], \"default\": null }\n"
            + "  ]\n"
            + "}";

    // Projection of first level nested field and top level field which contains the nested field
    // Also include the nested field twice
    // Expected schema - top level field
    List<String> fields2 = Arrays.asList("nested_field.secondLevelField.lastname", "nested_field",
        "nested_field.secondLevelField.lastname");
    String expectedSchema2 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", " + SCHEMA_WITH_AVRO_TYPES_STR + "], \"default\": null }\n"
            + "  ]\n"
            + "}";

    // Projection of non overlapping nested field and top level field with nested fields
    // Expected schema - top level field and one nested field
    List<String> fields3 = Arrays.asList("student.lastname", "nested_field");
    String expectedSchema3 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", " + SCHEMA_WITH_AVRO_TYPES_STR + "], \"default\": null },\n"
            + "    { \"name\": \"student\", \"type\": {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"student\",\n"
            + "      \"fields\": [\n"
            + "        { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "      ]\n"
            + "    }}\n"
            + "  ]\n"
            + "}";

    // Projection of two nested fields
    // Expected schema - two nested fields
    List<String> fields4 = Arrays.asList("student.lastname", "nested_field.secondLevelField.lastname");
    String expectedSchema4 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"TestRecordAvroTypes\",\n"
            + "      \"fields\": [\n"
            + "        { \"name\": \"secondLevelField\", \"type\": [\"null\", {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"secondLevelField\",\n"
            + "          \"fields\": [\n"
            + "            { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "          ]\n"
            + "        }], \"default\": null }\n"
            + "      ]\n"
            + "    }], \"default\": null },\n"
            + "    { \"name\": \"student\", \"type\": {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"student\",\n"
            + "      \"namespace\": \"com.acme.avro\","
            + "      \"fields\": [\n"
            + "        { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "      ]\n"
            + "    }}\n"
            + "  ]\n"
            + "}";

    // Projection of top level field and nested field column
    List<String> fields5 = Arrays.asList("firstname", "nested_field.secondLevelField.lastname", "nested_field.longField");
    // Expected schema - top level field and one nested field
    String expectedSchema5 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"firstname\", \"type\": \"string\" },\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"TestRecordAvroTypes\",\n"
            + "      \"fields\": [\n"
            + "        { \"name\": \"longField\", \"type\": \"long\" },\n"
            + "        { \"name\": \"secondLevelField\", \"type\": [\"null\", {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"secondLevelField\",\n"
            + "          \"fields\": [\n"
            + "            { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "          ]\n"
            + "        }], \"default\": null }\n"
            + "      ]\n"
            + "    }], \"default\": null }\n"
            + "  ]\n"
            + "}";

    Object[][] data = new Object[][] {
        {fields1, expectedSchema1},
        {fields2, expectedSchema2},
        {fields3, expectedSchema3},
        {fields4, expectedSchema4},
        {fields5, expectedSchema5}};
    return Stream.of(data).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("getExpectedSchemaForFields")
  public void testProjectSchemaWithNullableAndNestedFields(List<String> projectedFields, String expectedSchemaStr) {
    Schema expectedSchema = Schema.parse(expectedSchemaStr);
    Schema projectedSchema = HoodieAvroUtils.projectSchema(SCHEMA_WITH_NESTED_FIELD_LARGE, projectedFields);
    assertEquals(expectedSchema, projectedSchema);
    assertTrue(AvroSchemaUtils.isSchemaCompatible(projectedSchema, expectedSchema, false));
  }

  private static Stream<Arguments> recordNeedsRewriteForExtendedAvroTypePromotion() {
    Schema decimal1 = LogicalTypes.decimal(12, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema decimal2 = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema floatSchema = Schema.create(Schema.Type.FLOAT);
    Schema stringSchema = Schema.create(Schema.Type.STRING);

    Schema recordSchema1 = Schema.createRecord("record1", null, "com.example", false,
        Arrays.asList(new Schema.Field("decimalField", decimal1, null, null),
            new Schema.Field("doubleField", doubleSchema, null, null)));

    Schema recordSchema2 = Schema.createRecord("record2", null, "com.example2", false,
        Arrays.asList(new Schema.Field("decimalField", decimal1, null, null),
            new Schema.Field("doubleField", doubleSchema, null, null)));

    return Stream.of(
        Arguments.of(intSchema, longSchema, false),
        Arguments.of(intSchema, floatSchema, false),
        Arguments.of(longSchema, intSchema, true),
        Arguments.of(longSchema, floatSchema, false),
        Arguments.of(decimal1, decimal2, true),
        Arguments.of(doubleSchema, decimal1, true),
        Arguments.of(decimal1, doubleSchema, true),
        Arguments.of(intSchema, stringSchema, true),
        Arguments.of(longSchema, doubleSchema, false),
        Arguments.of(intSchema, doubleSchema, false),
        Arguments.of(longSchema, stringSchema, true),
        Arguments.of(floatSchema, stringSchema, true),
        Arguments.of(doubleSchema, stringSchema, true),
        Arguments.of(decimal1, stringSchema, true),
        Arguments.of(stringSchema, decimal2, true),
        Arguments.of(stringSchema, intSchema, true),
        Arguments.of(floatSchema, doubleSchema, true),
        Arguments.of(doubleSchema, floatSchema, true),
        Arguments.of(recordSchema1, recordSchema2, false),
        Arguments.of(dateSchema, stringSchema, true)
    );
  }

  @ParameterizedTest
  @MethodSource
  void recordNeedsRewriteForExtendedAvroTypePromotion(Schema writerSchema, Schema readerSchema, boolean expected) {
    boolean result = HoodieAvroUtils.recordNeedsRewriteForExtendedAvroTypePromotion(writerSchema, readerSchema);
    assertEquals(expected, result);
  }

  /**
   * Utility class for generating random GenericRecord instances and comparing records
   */
  private static class AvroTestUtils {
    private static final Random RANDOM = new Random(42); // Fixed seed for reproducible tests

    /**
     * Generate a random GenericRecord for the given schema
     */
    public static GenericRecord generateRandomRecord(Schema schema) {
      GenericRecord record = new GenericData.Record(schema);
      for (Schema.Field field : schema.getFields()) {
        Object value = generateRandomValue(field.schema(), field.defaultVal());
        record.put(field.pos(), value);
      }
      return record;
    }

    /**
     * Generate a list of random GenericRecord instances
     */
    public static List<GenericRecord> generateRandomRecords(Schema schema, int count) {
      List<GenericRecord> records = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        records.add(generateRandomRecord(schema));
      }
      return records;
    }

    /**
     * Generate a random value for the given schema type
     */
    private static Object generateRandomValue(Schema schema, Object defaultValue) {
      // Handle union types first
      if (schema.getType() == Schema.Type.UNION) {
        List<Schema> types = schema.getTypes();
        // For nullable unions, sometimes return null
        if (types.size() == 2 && types.get(0).getType() == Schema.Type.NULL && RANDOM.nextBoolean()) {
          return null;
        }
        // For complex unions (more than 2 types), never return null to avoid HoodieAvroSchemaException
        if (types.size() > 2) {
          // Choose the first non-null type
          Schema nonNullType = types.stream()
              .filter(t -> t.getType() != Schema.Type.NULL)
              .findFirst()
              .orElse(types.get(0));
          return generateRandomValue(nonNullType, null);
        }
        // For 2-type unions that are not simple nullable unions, also avoid null
        if (types.size() == 2 && !(types.get(0).getType() == Schema.Type.NULL || types.get(1).getType() == Schema.Type.NULL)) {
          // Choose the first non-null type
          Schema nonNullType = types.stream()
              .filter(t -> t.getType() != Schema.Type.NULL)
              .findFirst()
              .orElse(types.get(0));
          return generateRandomValue(nonNullType, null);
        }
        // Choose a non-null type from the union
        Schema nonNullType = types.stream()
            .filter(t -> t.getType() != Schema.Type.NULL)
            .findFirst()
            .orElse(types.get(0));
        return generateRandomValue(nonNullType, null);
      }
      // Handle default values
      if (defaultValue != null
          && !(defaultValue instanceof org.apache.avro.JsonProperties.Null)
          && RANDOM.nextBoolean()) {
        return defaultValue;
      }
      // For nullable fields, sometimes return null
      if (schema.getType() == Schema.Type.UNION && schema.getTypes().size() == 2
          && schema.getTypes().get(0).getType() == Schema.Type.NULL && RANDOM.nextBoolean()) {
        return null;
      }
      Schema actualSchema = schema;
      try {
        actualSchema = resolveNullableSchema(schema);
      } catch (Exception e) {
        // If we can't resolve the schema, just use the original
        actualSchema = schema;
      }

      switch (actualSchema.getType()) {
        case NULL:
          return null;
        case BOOLEAN:
          return RANDOM.nextBoolean();
        case INT:
          return RANDOM.nextInt(1000);
        case LONG:
          return RANDOM.nextLong() % 1000000L;
        case FLOAT:
          return RANDOM.nextFloat() * 100f;
        case DOUBLE:
          return RANDOM.nextDouble() * 1000.0;
        case STRING:
          return "test_string_" + RANDOM.nextInt(1000);
        case BYTES:
          byte[] bytes = new byte[RANDOM.nextInt(10) + 1];
          RANDOM.nextBytes(bytes);
          return ByteBuffer.wrap(bytes);
        case RECORD:
          return generateRandomRecord(actualSchema);
        case ENUM:
          List<String> symbols = actualSchema.getEnumSymbols();
          return new GenericData.EnumSymbol(actualSchema, symbols.get(RANDOM.nextInt(symbols.size())));
        case ARRAY:
          List<Object> array = new ArrayList<>();
          int arraySize = RANDOM.nextInt(3) + 1;
          for (int i = 0; i < arraySize; i++) {
            array.add(generateRandomValue(actualSchema.getElementType(), null));
          }
          return array;
        case MAP:
          Map<String, Object> map = new HashMap<>();
          int mapSize = RANDOM.nextInt(3) + 1;
          for (int i = 0; i < mapSize; i++) {
            map.put("key_" + i, generateRandomValue(actualSchema.getValueType(), null));
          }
          return map;
        case FIXED:
          byte[] fixedBytes = new byte[actualSchema.getFixedSize()];
          RANDOM.nextBytes(fixedBytes);
          return new GenericData.Fixed(actualSchema, fixedBytes);
        default:
          return null;
      }
    }

    /**
     * Compare a SpecificRecord with a GenericRecord using the same schema
     */
    public static void assertRecordsEqual(SpecificRecord specificRecord, GenericRecord genericRecord, Schema schema) {
      assertEquals(specificRecord.getClass().getSimpleName(), genericRecord.getSchema().getName());

      for (Schema.Field field : schema.getFields()) {
        Object specificValue = specificRecord.get(field.pos());
        Object genericValue = genericRecord.get(field.pos());

        // Normalize null values - treat JsonProperties$Null as null
        if (isNullValue(specificValue)) {
          specificValue = null;
        }
        if (isNullValue(genericValue)) {
          genericValue = null;
        }
        if (specificValue == null && genericValue == null) {
          continue;
        }
        if (specificValue == null || genericValue == null) {
          assertEquals(specificValue, genericValue,
              "Field " + field.name() + " has different null values");
          continue;
        }
        // Handle different types of comparisons based on field schema
        Schema fieldSchema;
        try {
          fieldSchema = resolveNullableSchema(field.schema());
        } catch (Exception e) {
          // For complex union types that can't be resolved, do content-based comparison
          if (specificValue instanceof SpecificRecord && genericValue instanceof GenericRecord) {
            // Compare wrapper types by their content
            assertWrapperRecordsEqual((SpecificRecord) specificValue, (GenericRecord) genericValue, field.name());
          } else {
            assertEquals(specificValue, genericValue,
                "Field " + field.name() + " values differ");
          }
          continue;
        }
        switch (fieldSchema.getType()) {
          case RECORD:
            if (specificValue instanceof SpecificRecord && genericValue instanceof GenericRecord) {
              assertRecordsEqual((SpecificRecord) specificValue, (GenericRecord) genericValue, fieldSchema);
            } else {
              assertEquals(specificValue, genericValue,
                  "Field " + field.name() + " record values differ");
            }
            break;
          case ARRAY:
            assertArrayValuesEqual((List<?>) specificValue, (List<?>) genericValue, fieldSchema, field.name());
            break;
          case MAP:
            assertMapValuesEqual((Map<?, ?>) specificValue, (Map<?, ?>) genericValue, fieldSchema, field.name());
            break;
          case ENUM:
            assertEquals(specificValue.toString(), genericValue.toString(),
                "Field " + field.name() + " enum values differ");
            break;
          case BYTES:
            assertByteArrayEqual((ByteBuffer) specificValue, (ByteBuffer) genericValue, field.name());
            break;
          default:
            assertEquals(specificValue, genericValue,
                "Field " + field.name() + " values differ");
        }
      }
    }

    private static void assertArrayValuesEqual(List<?> specificArray, List<?> genericArray, Schema schema, String fieldName) {
      assertEquals(specificArray.size(), genericArray.size(),
          "Field " + fieldName + " array sizes differ");

      Schema elementSchema = schema.getElementType();
      for (int i = 0; i < specificArray.size(); i++) {
        Object specificElement = specificArray.get(i);
        Object genericElement = genericArray.get(i);

        // Normalize null values
        if (isNullValue(specificElement)) {
          specificElement = null;
        }
        if (isNullValue(genericElement)) {
          genericElement = null;
        }
        if (specificElement == null && genericElement == null) {
          continue;
        }
        if (specificElement instanceof SpecificRecord && genericElement instanceof GenericRecord) {
          assertRecordsEqual((SpecificRecord) specificElement, (GenericRecord) genericElement, elementSchema);
        } else if (specificElement instanceof List && genericElement instanceof List) {
          // Handle nested lists
          assertListValuesEqual((List<?>) specificElement, (List<?>) genericElement, elementSchema, fieldName + "[" + i + "]");
        } else {
          // For other types, compare by content if they're different instances but same content
          if (!Objects.equals(specificElement, genericElement)) {
            // Try to compare by string representation as a fallback
            if (specificElement != null && genericElement != null
                && specificElement.toString().equals(genericElement.toString())) {
              // They have the same content, so they're equivalent
              continue;
            }
            assertEquals(specificElement, genericElement,
                "Field " + fieldName + " array element " + i + " differs");
          }
        }
      }
    }

    private static void assertMapValuesEqual(Map<?, ?> specificMap, Map<?, ?> genericMap, Schema schema, String fieldName) {
      assertEquals(specificMap.size(), genericMap.size(),
          "Field " + fieldName + " map sizes differ");

      Schema valueSchema = schema.getValueType();
      for (Map.Entry<?, ?> entry : specificMap.entrySet()) {
        Object key = entry.getKey();
        Object specificValue = entry.getValue();
        Object genericValue = genericMap.get(key);

        // Normalize null values
        if (isNullValue(specificValue)) {
          specificValue = null;
        }
        if (isNullValue(genericValue)) {
          genericValue = null;
        }
        if (specificValue == null && genericValue == null) {
          continue;
        }
        if (specificValue instanceof SpecificRecord && genericValue instanceof GenericRecord) {
          // Handle union types by getting the actual record schema
          Schema recordSchema = valueSchema.getType() == Schema.Type.UNION ? resolveNullableSchema(valueSchema) : valueSchema;
          assertRecordsEqual((SpecificRecord) specificValue, (GenericRecord) genericValue, recordSchema);
        } else if (specificValue instanceof List && genericValue instanceof List) {
          // Handle List comparison by content
          assertListValuesEqual((List<?>) specificValue, (List<?>) genericValue, valueSchema, fieldName + "[" + key + "]");
        } else {
          // For other types, compare by content if they're different instances but same content
          if (!Objects.equals(specificValue, genericValue)) {
            // Try to compare by string representation as a fallback
            if (specificValue != null && genericValue != null
                && specificValue.toString().equals(genericValue.toString())) {
              // They have the same content, so they're equivalent
              continue;
            }
            assertEquals(specificValue, genericValue, 
                "Field " + fieldName + " map value for key " + key + " differs");
          }
        }
      }
    }

    private static void assertByteArrayEqual(ByteBuffer specificBytes, ByteBuffer genericBytes, String fieldName) {
      if (specificBytes == null && genericBytes == null) {
        return;
      }
      if (specificBytes == null || genericBytes == null) {
        assertEquals(specificBytes, genericBytes, "Field " + fieldName + " byte arrays differ");
        return;
      }
      byte[] specificArray = specificBytes.array();
      byte[] genericArray = genericBytes.array();
      assertArrayEquals(specificArray, genericArray, "Field " + fieldName + " byte arrays differ");
    }

    /**
     * Check if a value represents null (including JsonProperties$Null)
     */
    private static boolean isNullValue(Object value) {
      return value == null || value instanceof org.apache.avro.JsonProperties.Null;
    }

    /**
     * Compare wrapper records (like BooleanWrapper vs GenericRecord) by their content
     */
    private static void assertWrapperRecordsEqual(SpecificRecord specificRecord, GenericRecord genericRecord, String fieldName) {
      // Get the schema from the specific record
      Schema schema = specificRecord.getSchema();

      // Compare each field in the wrapper
      for (Schema.Field field : schema.getFields()) {
        Object specificValue = specificRecord.get(field.pos());
        Object genericValue = genericRecord.get(field.pos());

        // Normalize null values
        if (isNullValue(specificValue)) {
          specificValue = null;
        }
        if (isNullValue(genericValue)) {
          genericValue = null;
        }
        assertEquals(specificValue, genericValue,
            "Field " + fieldName + "." + field.name() + " values differ");
      }
    }

    /**
     * Compare two lists by content (for cases where they might be different instances)
     */
    private static void assertListValuesEqual(List<?> list1, List<?> list2, Schema elementSchema, String fieldName) {
      assertEquals(list1.size(), list2.size(), 
          "Field " + fieldName + " list sizes differ");
      
      for (int i = 0; i < list1.size(); i++) {
        Object element1 = list1.get(i);
        Object element2 = list2.get(i);
        // Normalize null values
        if (isNullValue(element1)) {
          element1 = null;
        }
        if (isNullValue(element2)) {
          element2 = null;
        }
        if (element1 == null && element2 == null) {
          continue;
        }
        if (element1 instanceof SpecificRecord && element2 instanceof GenericRecord) {
          // For list elements, we need to get the element schema from the array schema
          Schema recordSchema = elementSchema.getType() == Schema.Type.ARRAY ? elementSchema.getElementType() : elementSchema;
          // Handle union types by getting the actual record schema
          if (recordSchema.getType() == Schema.Type.UNION) {
            recordSchema = resolveNullableSchema(recordSchema);
          }
          assertRecordsEqual((SpecificRecord) element1, (GenericRecord) element2, recordSchema);
        } else if (element1 instanceof List && element2 instanceof List) {
          assertListValuesEqual((List<?>) element1, (List<?>) element2, elementSchema, fieldName + "[" + i + "]");
        } else {
          // For other types, compare by content if they're different instances but same content
          if (!Objects.equals(element1, element2)) {
            // Try to compare by string representation as a fallback
            if (element1 != null && element2 != null
                && element1.toString().equals(element2.toString())) {
              // They have the same content, so they're equivalent
              continue;
            }
            assertEquals(element1, element2,
                "Field " + fieldName + " list element " + i + " differs");
          }
        }
      }
    }
  }

  /**
   * Test convertToSpecificRecord for all specified Avro model classes
   */
  @ParameterizedTest
  @MethodSource("provideAvroModelClasses")
  void testConvertToSpecificRecordForAllTypes(Class<? extends SpecificRecord> recordClass) {
    // Get schema from the class
    Schema schema = SpecificData.get().getSchema(recordClass);
    // Generate random generic record
    GenericRecord genericRecord = AvroTestUtils.generateRandomRecord(schema);
    // Convert to specific record - cast to the expected type
    @SuppressWarnings("unchecked")
    Class<? extends org.apache.avro.specific.SpecificRecordBase> specificRecordBaseClass =
        (Class<? extends org.apache.avro.specific.SpecificRecordBase>) recordClass;
    SpecificRecord specificRecord = HoodieAvroUtils.convertToSpecificRecord(specificRecordBaseClass, genericRecord);
    // Assert the output class
    assertEquals(recordClass, specificRecord.getClass());
    // Compare the records
    AvroTestUtils.assertRecordsEqual(specificRecord, genericRecord, schema);
  }

  /**
   * Test convertToSpecificRecord with multiple random records for each type
   */
  @ParameterizedTest
  @MethodSource("provideAvroModelClasses")
  void testConvertToSpecificRecordMultipleRecords(Class<? extends SpecificRecord> recordClass) {
    // Get schema from the class
    Schema schema = SpecificData.get().getSchema(recordClass);
    // Generate multiple random records
    List<GenericRecord> genericRecords = AvroTestUtils.generateRandomRecords(schema, 3);
    for (GenericRecord genericRecord : genericRecords) {
      // Convert to specific record - cast to the expected type
      @SuppressWarnings("unchecked")
      Class<? extends org.apache.avro.specific.SpecificRecordBase> specificRecordBaseClass =
          (Class<? extends org.apache.avro.specific.SpecificRecordBase>) recordClass;
      SpecificRecord specificRecord = HoodieAvroUtils.convertToSpecificRecord(specificRecordBaseClass, genericRecord);
      // Assert the output class
      assertEquals(recordClass, specificRecord.getClass());
      // Compare the records
      AvroTestUtils.assertRecordsEqual(specificRecord, genericRecord, schema);
    }
  }

  /**
   * Provide all the Avro model classes to test
   */
  static Stream<Arguments> provideAvroModelClasses() {
    return Stream.of(
        Arguments.of(org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieSavepointPartitionMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieWriteStat.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieCleanPartitionMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieCleanFileInfo.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieActionInstant.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieCompactionStrategy.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieCompactionOperation.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieFSPermission.class),
        Arguments.of(org.apache.hudi.avro.model.HoodiePath.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieFileStatus.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieBootstrapFilePartitionInfo.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieCompactionPlan.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieCleanerPlan.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieCleanMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieReplaceCommitMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieSavepointMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieMergeArchiveFilePlan.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieRollbackMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieBootstrapPartitionMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieBootstrapIndexInfo.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieIndexPlan.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieRestoreMetadata.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieRestorePlan.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieRollbackPlan.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieDeleteRecordList.class),
        Arguments.of(org.apache.hudi.avro.model.HoodieCommitMetadata.class)
    );
  }
}
