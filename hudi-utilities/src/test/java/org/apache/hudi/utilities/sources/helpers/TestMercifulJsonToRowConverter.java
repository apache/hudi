/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.hudi.avro.MercifulJsonConverterTestBase;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.stats.ValueType;
import org.apache.hudi.utilities.exception.HoodieJsonToRowConversionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_ENCODED_DECIMAL_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestMercifulJsonToRowConverter extends MercifulJsonConverterTestBase {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final MercifulJsonToRowConverter CONVERTER = new MercifulJsonToRowConverter(true, "__");

  private static final String SIMPLE_AVRO_WITH_DEFAULT = "/schema/simple-test-with-default-value.avsc";

  protected static SparkSession spark;

  @BeforeAll
  public static void start() {
    spark = SparkSession
        .builder()
        .master("local[*]")
        .appName(TestMercifulJsonToRowConverter.class.getName())
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();
  }

  @AfterAll
  public static void clear() {
    spark.close();
  }

  @Test
  void basicConversion() throws IOException {
    Schema simpleSchema = SchemaTestUtil.getSchema(SIMPLE_AVRO_WITH_DEFAULT);
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    Map<String, Object> data = new HashMap<>();
    data.put("name", name);
    data.put("favorite_number", number);
    data.put("favorite_color", color);
    String json = MAPPER.writeValueAsString(data);

    List<Object> values = new ArrayList<>(Collections.nCopies(simpleSchema.getFields().size(), null));
    values.set(0, name);
    values.set(1, number);
    values.set(3, color);
    Row recRow = RowFactory.create(values.toArray());
    Row realRow = CONVERTER.convertToRow(json, simpleSchema);
    validateSchemaCompatibility(Collections.singletonList(realRow), simpleSchema);
    assertEquals(recRow, realRow);
  }

  @ParameterizedTest
  @MethodSource("dataNestedJsonAsString")
  void nestedJsonAsString(String nameInput) throws IOException {
    Schema simpleSchema = SchemaTestUtil.getSimpleSchema();
    int number = 1337;
    String color = "Blue. No yellow!";

    Map<String, Object> data = new HashMap<>();
    data.put("name", nameInput);
    data.put("favorite_number", number);
    data.put("favorite_color", color);
    String json = MAPPER.writeValueAsString(data);

    List<Object> values = new ArrayList<>(Collections.nCopies(simpleSchema.getFields().size(), null));
    values.set(0, nameInput);
    values.set(1, number);
    values.set(2, color);
    Row recRow = RowFactory.create(values.toArray());

    Assertions.assertEquals(recRow, CONVERTER.convertToRow(json, simpleSchema));
  }

  private static final String DECIMAL_AVRO_FILE_PATH = "/decimal-logical-type.avsc";
  /**
   * Covered case:
   * Avro Logical Type: Decimal
   * Avro type: bytes
   * Input: String number "123.45"
   * Output: Object using Byte data type as the schema specified.
   */
  @Test
  void decimalLogicalTypeByteTypeTest() throws IOException {
    String num = "123.45";
    BigDecimal bigDecimal = new BigDecimal(num);

    Map<String, Object> data = new HashMap<>();
    data.put("decimalField", num);
    String json = MAPPER.writeValueAsString(data);

    Schema schema = SchemaTestUtil.getSchema(DECIMAL_AVRO_FILE_PATH);

    Row expectRow = RowFactory.create(bigDecimal);
    Row realRow = CONVERTER.convertToRow(json, schema);
    validateSchemaCompatibility(Collections.singletonList(realRow), schema);
    assertEquals(expectRow, realRow);
  }

  /**
   * Covered case:
   * Avro Logical Type: Decimal
   * Exhaustive unsupported input coverage.
   */
  @ParameterizedTest
  @MethodSource("decimalBadCases")
  void decimalLogicalTypeInvalidCaseTest(String avroFile, String strInput, Double numInput, boolean testFixedByteArray) throws IOException {
    Schema schema = SchemaTestUtil.getSchema(avroFile);

    Map<String, Object> data = new HashMap<>();
    if (strInput != null) {
      data.put("decimalField", strInput);
    } else if (numInput != null) {
      data.put("decimalField", numInput);
    } else if (testFixedByteArray) {
      // Convert the fixed value to int array, which is used as json value literals.
      int[] intArray = {0, 0, 48, 57};
      data.put("decimalField", intArray);
    }
    String json = MAPPER.writeValueAsString(data);

    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToRowConversionException.class, () -> {
      CONVERTER.convertToRow(json, schema);
    });
  }

  /**
   * Covered case:
   * Avro Logical Type: Decimal
   * Avro type: bytes, fixed
   * Input: Check test parameter
   * Output: Object using Byte data type as the schema specified.
   */
  @ParameterizedTest
  @MethodSource("decimalGoodCases")
  void decimalLogicalTypeTest(String avroFilePath, String groundTruth, String strInput,
                              Number numInput, boolean testFixedByteArray) throws IOException {
    BigDecimal bigDecimal = new BigDecimal(groundTruth);
    Map<String, Object> data = new HashMap<>();

    Schema schema = SchemaTestUtil.getSchema(avroFilePath);

    // Decide the decimal field input according to the test dimension.
    if (strInput != null) {
      data.put("decimalField", strInput); // String number input
    } else if (numInput != null) {
      data.put("decimalField", numInput); // Number input
    } else if (testFixedByteArray) {
      // Fixed byte array input.
      // Example: 123.45 - byte array [0, 0, 48, 57].
      Schema fieldSchema = schema.getField("decimalField").schema();
      GenericFixed fixedValue = new Conversions.DecimalConversion().toFixed(
          bigDecimal, fieldSchema, fieldSchema.getLogicalType());
      // Convert the fixed value to int array, which is used as json value literals.
      byte[] byteArray = fixedValue.bytes();
      int[] intArray = new int[byteArray.length];
      for (int i = 0; i < byteArray.length; i++) {
        // Byte is signed in Java, int is 32-bit. Convert by & 0xFF to handle negative values correctly.
        intArray[i] = byteArray[i] & 0xFF;
      }
      data.put("decimalField", intArray);
    }

    String json = MAPPER.writeValueAsString(data);

    Row expectRow = RowFactory.create(bigDecimal);
    Row realRow = CONVERTER.convertToRow(json, schema);
    validateSchemaCompatibility(Collections.singletonList(realRow), schema);
    assertEquals(expectRow, realRow);
  }

  private static final String DURATION_AVRO_FILE_PATH = "/duration-logical-type.avsc";

  @ParameterizedTest
  @MethodSource("zeroScaleDecimalCases")
  void zeroScaleDecimalConversion(String inputValue, String expected, boolean shouldConvert) {
    Schema schema = new Schema.Parser().parse(
        "{\"namespace\": \"example.avro\",\"type\": \"record\",\"name\": \"decimalLogicalType\","
            + "\"fields\": [{\"name\": \"decimalField\", \"type\": {\"type\": \"bytes\", "
            + "\"logicalType\": \"decimal\", \"precision\": 38, \"scale\": 0}}]}");
    String json = String.format("{\"decimalField\":%s}", inputValue);

    if (shouldConvert) {
      BigDecimal bigDecimal = new BigDecimal(expected);
      Row expectedRow = RowFactory.create(bigDecimal);
      Row actualRow = CONVERTER.convertToRow(json, schema);
      validateSchemaCompatibility(Collections.singletonList(actualRow), schema);
      assertEquals(expectedRow, actualRow);
    } else {
      assertThrows(HoodieJsonToRowConversionException.class, () -> CONVERTER.convertToRow(json, schema));
    }
  }

  /**
   * Covered case:
   * Avro Logical Type: Duration
   * Avro type: 12 byte fixed
   * Input: 3-element list [month, days, milliseconds]
   * Output: Object using the avro data type as the schema specified.
   */
  @ParameterizedTest
  @MethodSource("durationGoodCases")
  void durationLogicalTypeTest(int months, int days, int milliseconds) throws IOException {
    List<Integer> num = new ArrayList<>();
    num.add(months);
    num.add(days);
    num.add(milliseconds);

    Map<String, Object> data = new HashMap<>();
    data.put("duration", num);
    String json = MAPPER.writeValueAsString(data);

    ByteBuffer buffer = ByteBuffer.allocate(12).order(ByteOrder.LITTLE_ENDIAN);
    buffer.putInt(months);  // months
    buffer.putInt(days); // days
    buffer.putInt(milliseconds); // milliseconds
    buffer.flip();
    Schema schema = SchemaTestUtil.getSchema(DURATION_AVRO_FILE_PATH);

    // Duration type is not supported in Row object.
    assertThrows(HoodieJsonToRowConversionException.class, () -> {
      CONVERTER.convertToRow(json, schema);
    });
  }

  @ParameterizedTest
  @MethodSource("durationBadCases")
  void durationLogicalTypeBadTest(String schemaFile, Object input) throws IOException {
    // As duration uses 12 byte fixed type to store 3 unsigned int numbers, Long.MAX would cause overflow.
    // Verify it is gracefully handled.
    Map<String, Object> data = new HashMap<>();
    data.put("duration", input);
    String json = MAPPER.writeValueAsString(data);

    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(schemaFile);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToRowConversionException.class, () -> {
      CONVERTER.convertToRow(json, schema);
    });
  }

  private static final String DATE_AVRO_FILE_PATH = "/date-type.avsc";
  private static final String DATE_AVRO_INVALID_FILE_PATH = "/date-type-invalid.avsc";

  /**
   * Covered case:
   * Avro Logical Type: Date
   * Avro type: int
   * Input: Check parameter definition
   * Output: Object using the avro data type as the schema specified.
   */
  @ParameterizedTest
  @MethodSource("dateProviderForRow")
  void dateLogicalTypeTest(String groundTruthRow, Object dateInput) throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchema(DATE_AVRO_FILE_PATH);

    Map<String, Object> data = new HashMap<>();
    data.put("dateField", dateInput);
    String json = MAPPER.writeValueAsString(data);

    if (groundTruthRow == null) {
      return;
    }
    Row rec = RowFactory.create(java.sql.Date.valueOf(groundTruthRow).toLocalDate());
    Row realRow = CONVERTER.convertToRow(json, schema);
    validateSchemaCompatibility(Collections.singletonList(realRow), schema);
    assertEquals(rec.getLocalDate(0).toString(), realRow.getLocalDate(0).toString());
  }

  /**
   * Covered case:
   * Avro Logical Type: Date
   * Invalid schema configuration.
   */
  @Test
  void dateLogicalTypeTest() throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchema(DATE_AVRO_INVALID_FILE_PATH);

    Map<String, Object> data = new HashMap<>();
    data.put("dateField", 1);
    String json = MAPPER.writeValueAsString(data);
    assertThrows(HoodieJsonToRowConversionException.class, () -> {
      CONVERTER.convertToRow(json, schema);
    });
  }

  private static final String LOCAL_TIME_AVRO_FILE_PATH = "/local-timestamp-logical-type.avsc";

  @FunctionalInterface
  public interface ThrowingRunnable {
    void run() throws Exception;
  }

  public static void timestampNTZCompatibility(ThrowingRunnable r) throws Exception {
    // TODO: Remove this when we get rid of spark3.3. TimestampNTZ needs this config
    //  to be set to true to work.
    boolean isSpark33 = HoodieSparkUtils.isSpark3_3();
    String propertyValue = null;
    if (isSpark33) {
      propertyValue = System.getProperty("spark.testing");
      System.setProperty("spark.testing", "true");
    }
    try {
      r.run();
    } finally {
      if (isSpark33) {
        if (propertyValue == null) {
          System.clearProperty("spark.testing");
        } else {
          System.setProperty("spark.testing", propertyValue);
        }
      }
    }
  }

  /**
   * Covered case:
   * Avro Logical Type: localTimestampMillisField & localTimestampMillisField
   * Avro type: long for both
   * Input: Check parameter definition
   * Output: Object using the avro data type as the schema specified.
   */
  @ParameterizedTest
  @MethodSource("localTimestampGoodCaseProvider")
  void localTimestampLogicalTypeGoodCaseTest(
      Long expectedMicroSecOfDay, Object timeMilli, Object timeMicro) throws Exception {
    timestampNTZCompatibility(() -> {
      // Example inputs
      long microSecOfDay = expectedMicroSecOfDay;
      long milliSecOfDay = expectedMicroSecOfDay / 1000; // Represents 12h 30 min since the start of the day

      // Define the schema for the date logical type
      Schema schema = SchemaTestUtil.getSchema(LOCAL_TIME_AVRO_FILE_PATH);

      Map<String, Object> data = new HashMap<>();
      data.put("localTimestampMillisField", timeMilli);
      data.put("localTimestampMicrosField", timeMicro);
      String json = MAPPER.writeValueAsString(data);

      Row rec = RowFactory.create(ValueType.castToLocalTimestampMillis(milliSecOfDay, null), ValueType.castToLocalTimestampMicros(microSecOfDay, null));
      Row actualRow = CONVERTER.convertToRow(json, schema);
      validateSchemaCompatibility(Collections.singletonList(actualRow), schema);
      assertEquals(rec, actualRow);
    });
  }

  @ParameterizedTest
  @MethodSource("localTimestampBadCaseProvider")
  void localTimestampLogicalTypeBadTest(
      String schemaFile, Object input) throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchema(schemaFile);
    Map<String, Object> data = new HashMap<>();
    data.put("timestamp", input);
    String json = MAPPER.writeValueAsString(data);

    assertThrows(HoodieJsonToRowConversionException.class, () -> {
      CONVERTER.convertToRow(json, schema);
    });
  }

  private static final String TIMESTAMP_AVRO_FILE_PATH = "/timestamp-logical-type2.avsc";

  /**
   * Covered case:
   * Avro Logical Type: localTimestampMillisField & localTimestampMillisField
   * Avro type: long for both
   * Input: Check parameter definition
   * Output: Object using the avro data type as the schema specified.
   */
  @ParameterizedTest
  @MethodSource("timestampGoodCaseProvider")
  void timestampLogicalTypeGoodCaseTest(
      Long expectedMicroSecOfDay, Object timeMilli, Object timeMicro) throws IOException {
    // Example inputs
    long microSecOfDay = expectedMicroSecOfDay;
    long milliSecOfDay = expectedMicroSecOfDay / 1000;

    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchema(TIMESTAMP_AVRO_FILE_PATH);

    Map<String, Object> data = new HashMap<>();
    data.put("timestampMillisField", timeMilli);
    data.put("timestampMicrosField", timeMicro);
    String json = MAPPER.writeValueAsString(data);

    Row rec = RowFactory.create(new Timestamp(milliSecOfDay), new Timestamp(microSecOfDay / 1000));
    Row actualRow = CONVERTER.convertToRow(json, schema);
    validateSchemaCompatibility(Collections.singletonList(actualRow), schema);
    assertEquals(rec, actualRow);
  }

  @ParameterizedTest
  @MethodSource("timestampBadCaseProvider")
  void timestampLogicalTypeBadTest(Object input) throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchema(TIMESTAMP_AVRO_FILE_PATH);
    Map<String, Object> data = new HashMap<>();
    data.put("timestampMillisField", input);
    data.put("timestampMicrosField", input);
    String json = MAPPER.writeValueAsString(data);
    // Schedule with timestamp same as that of committed instant

    assertThrows(HoodieJsonToRowConversionException.class, () -> {
      CONVERTER.convertToRow(json, schema);
    });
  }

  private static final String TIME_AVRO_FILE_PATH = "/time-logical-type.avsc";

  /**
   * Covered case:
   * Avro Logical Type: time-micros & time-millis
   * Avro type: long for time-micros, int for time-millis
   * Input: Check parameter definition
   * Output: Object using the avro data type as the schema specified.
   */
  @ParameterizedTest
  @MethodSource("timeGoodCaseProvider")
  void timeLogicalTypeTest(Long expectedMicroSecOfDay, Object timeMilli, Object timeMicro) throws IOException {
    // Example inputs
    long microSecOfDay = expectedMicroSecOfDay;
    int milliSecOfDay = (int) (expectedMicroSecOfDay / 1000); // Represents 12h 30 min since the start of the day

    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchema(TIME_AVRO_FILE_PATH);

    Map<String, Object> data = new HashMap<>();
    data.put("timeMicroField", timeMicro);
    data.put("timeMillisField", timeMilli);
    String json = MAPPER.writeValueAsString(data);

    Row rec = RowFactory.create(microSecOfDay, milliSecOfDay);
    Row realRow = CONVERTER.convertToRow(json, schema);
    validateSchemaCompatibility(Collections.singletonList(realRow), schema);
    assertEquals(rec.get(0).toString(), realRow.get(0).toString());
    assertEquals(rec.get(1).toString(), realRow.get(1).toString());
  }

  @ParameterizedTest
  @MethodSource("timeBadCaseProvider")
  void timeLogicalTypeBadCaseTest(Object invalidInput) throws IOException {
    String validInput = "00:00:00";
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchemaFromResourceFilePath(TIME_AVRO_FILE_PATH);

    // Only give one of the field invalid value at a time so that both processor type can have coverage.
    Map<String, Object> data = new HashMap<>();
    data.put("timeMicroField", validInput);
    data.put("timeMillisField", invalidInput);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToRowConversionException.class, () -> {
      CONVERTER.convertToRow(MAPPER.writeValueAsString(data), schema);
    });

    data.clear();
    data.put("timeMicroField", invalidInput);
    data.put("timeMillisField", validInput);
    // Schedule with timestamp same as that of committed instant
    assertThrows(HoodieJsonToRowConversionException.class, () -> {
      CONVERTER.convertToRow(MAPPER.writeValueAsString(data), schema);
    });
  }

  private static final String UUID_AVRO_FILE_PATH = "/uuid-logical-type.avsc";

  /**
   * Covered case:
   * Avro Logical Type: uuid
   * Avro type: string
   * Input: uuid string
   * Output: Object using the avro data type as the schema specified.
   */
  @ParameterizedTest
  @MethodSource("uuidDimension")
  void uuidLogicalTypeTest(String uuid) throws IOException {
    // Define the schema for the date logical type
    Schema schema = SchemaTestUtil.getSchema(UUID_AVRO_FILE_PATH);

    Map<String, Object> data = new HashMap<>();
    data.put("uuidField", uuid);
    String json = MAPPER.writeValueAsString(data);

    Row rec = RowFactory.create(uuid);
    Row real = CONVERTER.convertToRow(json, schema);
    validateSchemaCompatibility(Collections.singletonList(real), schema);
    assertEquals(rec, real);
  }

  @ParameterizedTest
  @MethodSource("nestedRecord")
  void nestedRecordTest(String contactInput, boolean isString) {
    String nestedSchemaStr =
        "{\"type\":\"record\",\"name\":\"User\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},"
            +
            "{\"name\":\"contact\",\"type\":{\"type\":\"record\",\"name\":\"Contact\",\"fields\":[{\"name\":\"email\",\"type\":\"string\"}]}}]}";
    String json = isString ? String.format("{\"name\":\"Jane Smith\",\"contact\":{\"email\":\"%s\"}}", contactInput)
        : String.format("{\"name\":\"Jane Smith\",\"contact\":{\"email\":%s}}", contactInput);
    Schema nestedSchema = new Schema.Parser().parse(nestedSchemaStr);

    Row expected = RowFactory.create("Jane Smith", RowFactory.create(contactInput));
    Row real = CONVERTER.convertToRow(json, nestedSchema);
    validateSchemaCompatibility(Collections.singletonList(real), nestedSchema);
    assertEquals(expected, real);
  }

  @Test
  public void conversionWithFieldNameSanitization() throws IOException {
    String sanitizedSchemaString =
        "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"__name\", \"type\": \"string\"}, "
            +
            "{\"name\": \"favorite__number\", \"type\": \"int\"}, {\"name\": \"favorite__color__\", \"type\": \"string\"}]}";
    Schema sanitizedSchema = Schema.parse(sanitizedSchemaString);
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    Map<String, Object> data = new HashMap<>();
    data.put("$name", name);
    data.put("favorite-number", number);
    data.put("favorite.color!", color);
    String json = MAPPER.writeValueAsString(data);

    List<Object> values = new ArrayList<>(Collections.nCopies(sanitizedSchema.getFields().size(), null));
    values.set(0, name);
    values.set(1, number);
    values.set(2, color);
    Row expected = RowFactory.create(values.toArray());
    Row actual = CONVERTER.convertToRow(json, sanitizedSchema);
    validateSchemaCompatibility(Collections.singletonList(actual), sanitizedSchema);
    assertEquals(expected, actual);
  }

  @Test
  void conversionWithFieldNameAliases() throws IOException {
    String schemaStringWithAliases = "{\"namespace\": \"example.avro\", \"type\": \"record\", \"name\": \"User\", \"fields\": [{\"name\": \"name\", \"type\": \"string\", \"aliases\": [\"$name\"]}, "
        + "{\"name\": \"favorite_number\",  \"type\": \"int\", \"aliases\": [\"unused\", \"favorite-number\"]}, {\"name\": \"favorite_color\", \"type\": \"string\", \"aliases\": "
        + "[\"favorite.color!\"]}, {\"name\": \"unmatched\", \"type\": \"string\", \"default\": \"default_value\"}]}";
    Schema sanitizedSchema = new Schema.Parser().parse(schemaStringWithAliases);
    String name = "John Smith";
    int number = 1337;
    String color = "Blue. No yellow!";
    String unmatched = "unmatched";
    Map<String, Object> data = new HashMap<>();
    data.put("$name", name);
    data.put("favorite-number", number);
    data.put("favorite.color!", color);
    data.put("unmatched", unmatched);
    String json = MAPPER.writeValueAsString(data);

    List<Object> values = new ArrayList<>(Collections.nCopies(sanitizedSchema.getFields().size(), null));
    values.set(0, name);
    values.set(1, number);
    values.set(2, color);
    values.set(3, unmatched);
    Row recRow = RowFactory.create(values.toArray());
    Row realRow = CONVERTER.convertToRow(json, sanitizedSchema);
    validateSchemaCompatibility(Collections.singletonList(realRow), sanitizedSchema);
    assertEquals(recRow, realRow);
  }

  @ParameterizedTest
  @MethodSource("encodedDecimalScalePrecisionProvider")
  void testEncodedDecimal(int scale, int precision) throws JsonProcessingException {
    Random rand = new Random();
    BigDecimal decfield = BigDecimal.valueOf(rand.nextDouble())
        .setScale(scale, RoundingMode.HALF_UP).round(new MathContext(precision, RoundingMode.HALF_UP));
    Map<String, Object> data = new HashMap<>();
    data.put("_row_key", "mykey");
    long timestamp = 214523432;
    data.put("timestamp", timestamp);
    data.put("rider", "myrider");
    data.put("decfield", Base64.getEncoder().encodeToString(decfield.unscaledValue().toByteArray()));
    data.put("driver", "mydriver");
    data.put("lowprecision", 12.34);
    data.put("highprecision", 12.987654312312);
    data.put("fare", rand.nextDouble() * 100);
    data.put("_hoodie_is_deleted", false);
    String json = MAPPER.writeValueAsString(data);
    Schema tripSchema = new Schema.Parser().parse(
        TRIP_ENCODED_DECIMAL_SCHEMA.replace("6", Integer.toString(scale)).replace("10", Integer.toString(precision)));
    Row rec = CONVERTER.convertToRow(json, tripSchema);
    validateSchemaCompatibility(Collections.singletonList(rec), tripSchema);
    BigDecimal actualField = rec.getDecimal(tripSchema.getField("decfield").pos());
    assertEquals(decfield, actualField);
  }

  @ParameterizedTest
  @MethodSource("encodedDecimalFixedScalePrecisionProvider")
  void testEncodedDecimalAvroSparkPostProcessorCase(int size, int scale, int precision) throws JsonProcessingException {
    Random rand = new Random();
    String postProcessSchemaString = String.format("{\"type\":\"record\",\"name\":\"tripUberRec\","
        + "\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"\"},{\"name\":\"_row_key\","
        + "\"type\":\"string\",\"doc\":\"\"},{\"name\":\"rider\",\"type\":\"string\",\"doc\":\"\"},"
        + "{\"name\":\"decfield\",\"type\":{\"type\":\"fixed\",\"name\":\"fixed\","
        + "\"namespace\":\"tripUberRec.decfield\",\"size\":%d,\"logicalType\":\"decimal\","
        + "\"precision\":%d,\"scale\":%d},\"doc\":\"\"},{\"name\":\"driver\",\"type\":\"string\","
        + "\"doc\":\"\"},{\"name\":\"fare\",\"type\":\"double\",\"doc\":\"\"},{\"name\":\"_hoodie_is_deleted\","
        + "\"type\":\"boolean\",\"doc\":\"\"}]}", size, precision, scale);
    Schema postProcessSchema = new Schema.Parser().parse(postProcessSchemaString);
    BigDecimal decfield = BigDecimal.valueOf(rand.nextDouble())
        .setScale(scale, RoundingMode.HALF_UP).round(new MathContext(precision, RoundingMode.HALF_UP));
    Map<String, Object> data = new HashMap<>();
    data.put("_row_key", "mykey");
    long timestamp = 214523432;
    data.put("timestamp", timestamp);
    data.put("rider", "myrider");
    data.put("decfield", Base64.getEncoder().encodeToString(decfield.unscaledValue().toByteArray()));
    data.put("driver", "mydriver");
    data.put("fare", rand.nextDouble() * 100);
    data.put("_hoodie_is_deleted", false);
    String json = MAPPER.writeValueAsString(data);
    Row rec = CONVERTER.convertToRow(json, postProcessSchema);
    BigDecimal actualField = rec.getDecimal(postProcessSchema.getField("decfield").pos());
    validateSchemaCompatibility(Collections.singletonList(rec), postProcessSchema);
    assertEquals(decfield, actualField);
  }

  private void validateSchemaCompatibility(List<Row> rows, Schema schema) {
    StructType rowSchema = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    Dataset<Row> dataset = spark.createDataFrame(rows, rowSchema);
    assertDoesNotThrow(dataset::collect, "Schema validation and dataset creation should not throw any exceptions.");
  }
}
