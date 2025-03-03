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

import org.junit.jupiter.params.provider.Arguments;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Stream;

public class MercifulJsonConverterTestBase {

  private static final String DECIMAL_AVRO_FILE_INVALID_PATH = "/decimal-logical-type-invalid.avsc";
  protected static final String DECIMAL_AVRO_FILE_PATH = "/decimal-logical-type.avsc";
  private static final String DECIMAL_FIXED_AVRO_FILE_PATH = "/decimal-logical-type-fixed-type.avsc";
  protected static final String DECIMAL_ZERO_SCALE_AVRO_FILE_PATH = "/decimal-logical-type-zero-scale.avsc";
  private static final String LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH = "/local-timestamp-micros-logical-type.avsc";
  private static final String LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH = "/local-timestamp-millis-logical-type.avsc";
  private static final String DURATION_AVRO_FILE_PATH_INVALID = "/duration-logical-type-invalid.avsc";

  protected static final String DURATION_AVRO_FILE_PATH = "/duration-logical-type.avsc";
  protected static final String DATE_AVRO_FILE_PATH = "/date-type.avsc";
  private static final String DATE_AVRO_INVALID_FILE_PATH = "/date-type-invalid.avsc";
  protected static final String TIMESTAMP_AVRO_FILE_PATH = "/timestamp-logical-type2.avsc";

  static Stream<Object> decimalBadCases() {
    return Stream.of(
        // Invalid schema definition.
        Arguments.of(DECIMAL_AVRO_FILE_INVALID_PATH, "123.45", null, false),
        // Schema set precision as 5, input overwhelmed the precision.
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "123333.45", null, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, null, 123333.45, false),
        // Schema precision set to 5, scale set to 2, so there is only 3 digit to accommodate integer part.
        // As we do not do rounding, any input with more than 3 digit integer would fail.
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "1233", null, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, null, 1233D, false),
        // Schema set scale as 2, input overwhelmed the scale.
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "0.222", null, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, null, 0.222, false),
        // Invalid string which cannot be parsed as number.
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "", null, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "NotAValidString", null, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "-", null, false),
        // Schema requires byte type while input is fixed type raw data.
        Arguments.of(DECIMAL_AVRO_FILE_PATH, null, null, true)
    );
  }

  static Stream<Object> decimalGoodCases() {
    return Stream.of(
        // The schema all set precision as 5, scale as 2.
        // Test dimension: Schema file, Ground truth, string input, number input, fixed byte array input.
        // Test some random numbers.
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "123.45", "123.45", null, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "123.45", null, 123.45, false),
        // Test MIN/MAX allowed by the schema.
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "-999.99", "-999.99", null, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "999.99", null, 999.99, false),
        // Test 0.
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "0", null, 0D, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "0", "0", null, false),
        Arguments.of(DECIMAL_AVRO_FILE_PATH, "0", "000.00", null, false),
        // Same set of coverage over schame using byte/fixed type.
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "123.45", "123.45", null, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "123.45", null, 123.45, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "-999.99", "-999.99", null, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "999.99", null, 999.99, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "999", null, 999, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "999", null, 999L, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "999", null, (short) 999, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "100", null, (byte) 100, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "0", null, 0D, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "0", null, 0, false),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "0", "0", null, true),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "0", "000.00", null, true),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "123.45", null, null, true),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "123.45", null, 123.45, true),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "-999.99", null, null, true),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "999.99", null, 999.99, true),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "0", null, null, true),
        Arguments.of(DECIMAL_FIXED_AVRO_FILE_PATH, "0", null, null, true),
        Arguments.of(DECIMAL_ZERO_SCALE_AVRO_FILE_PATH, "12345", "12345.0", null, false),
        Arguments.of(DECIMAL_ZERO_SCALE_AVRO_FILE_PATH, "12345", null, 12345.0, false),
        Arguments.of(DECIMAL_ZERO_SCALE_AVRO_FILE_PATH, "12345", null, 12345, false),
        Arguments.of(DECIMAL_ZERO_SCALE_AVRO_FILE_PATH, "1230", null, 1.23e+3, false),
        Arguments.of(DECIMAL_ZERO_SCALE_AVRO_FILE_PATH, "1230", "1.23e+3", null, false)
    );
  }

  static Stream<Object> zeroScaleDecimalCases() {
    return Stream.of(
        // Input value in JSON, expected decimal, whether conversion should be successful
        // Values that can be converted
        Arguments.of("0.0", "0", true),
        Arguments.of("20.0", "20", true),
        Arguments.of("320", "320", true),
        Arguments.of("320.00", "320", true),
        Arguments.of("-1320.00", "-1320", true),
        Arguments.of("1520423524459", "1520423524459", true),
        Arguments.of("1520423524459.0", "1520423524459", true),
        Arguments.of("1000000000000000.0", "1000000000000000", true),
        // Values that are big enough and out of range of int or long types
        // Note that we can have at most 17 significant decimal digits in double values
        Arguments.of("1.2684037455962608e+16", "12684037455962608", true),
        Arguments.of("4.0100001e+16", "40100001000000000", true),
        Arguments.of("3.52838e+17", "352838000000000000", true),
        Arguments.of("9223372036853999600.0000", "9223372036853999600", true),
        Arguments.of("999998887654321000000000000000.0000", "999998887654321000000000000000", true),
        Arguments.of("-999998887654321000000000000000.0000", "-999998887654321000000000000000",
            true),
        // Values covering high precision decimals that lose precision when converting to a double
        Arguments.of("3.781239258857277e+16", "37812392588572770", true),
        Arguments.of("1.6585135379127473e+18", "1658513537912747300", true),
        // Values that should not be converted
        Arguments.of("0.0001", null, false),
        Arguments.of("300.9999", null, false),
        Arguments.of("1928943043.0001", null, false)
    );
  }

  static Stream<Object> durationGoodCases() {
    return Stream.of(
        // Normal inputs.
        Arguments.of(1, 2, 3),
        // Negative int would be interpreted as some unsigned int by Avro. They all 4-byte.
        Arguments.of(-1, -2, -3),
        // Signed -1 interpreted to unsigned would be unsigned MAX
        Arguments.of(-1, -1, -1),
        // Other special edge cases.
        Arguments.of(0, 0, 0),
        Arguments.of(Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE),
        Arguments.of(Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE)
    );
  }

  static Stream<Object> durationBadCases() {
    return Stream.of(
        // As duration uses 12 byte fixed type to store 3 unsigned int numbers, Long.MAX would cause overflow.
        // Verify it is gracefully handled.
        Arguments.of(DURATION_AVRO_FILE_PATH, Arrays.asList(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE)),
        // Invalid num of element count
        Arguments.of(DURATION_AVRO_FILE_PATH, Arrays.asList(1, 2, 3, 4)),
        Arguments.of(DURATION_AVRO_FILE_PATH, Arrays.asList(1, 2)),
        Arguments.of(DURATION_AVRO_FILE_PATH, (Object) new int[]{}),
        Arguments.of(DURATION_AVRO_FILE_PATH, "InvalidString"),
        Arguments.of(DURATION_AVRO_FILE_PATH_INVALID, Arrays.asList(1, 2, 3))
    );
  }

  static Stream<Object> dateGoodCaseProvider() {
    return Stream.of(
        Arguments.of(18506, 18506), // epochDays
        Arguments.of(18506, "2020-09-01"),  // dateString
        Arguments.of(7323356, "+22020-09-01"),  // dateString
        Arguments.of(18506, "18506"),  // epochDaysString
        Arguments.of(Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE)),
        Arguments.of(Integer.MIN_VALUE, Integer.toString(Integer.MIN_VALUE))
    );
  }

  static Stream<Object> dateBadCaseProvider() {
    return Stream.of(
        Arguments.of(DATE_AVRO_INVALID_FILE_PATH, 18506), // epochDays
        Arguments.of(DATE_AVRO_FILE_PATH, "#$@#%$@$%#@"),
        Arguments.of(DATE_AVRO_FILE_PATH, "22020-09-01000"),
        Arguments.of(DATE_AVRO_FILE_PATH, "2020-02-45"),
        Arguments.of(DATE_AVRO_FILE_PATH, Arrays.asList(1, 2, 3))
    );
  }

  static Stream<Object> localTimestampGoodCaseProvider() {
    return Stream.of(
        // Test cases with 'T' as the separator
        Arguments.of(
            (long)(1715644416 * 1e6 + 4000000 / 1e3), // Num of micro sec since unix epoch
            "2024-05-13T23:53:36.004", // Timestamp equivalence
            "2024-05-13T23:53:36.004"),
        Arguments.of(
            (long)(1715644416 * 1e6), // Num of micro sec since unix epoch
            "2024-05-13T23:53:36", // Timestamp equivalence
            "2024-05-13T23:53:36"),
        // Test cases with ' ' as the separator
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3), // Num of micro sec since unix epoch
            "2024-05-13 23:53:36.004", // Timestamp equivalence
            "2024-05-13 23:53:36.004"),
        Arguments.of(
            (long) (1715644416 * 1e6), // Num of micro sec since unix epoch
            "2024-05-13 23:53:36", // Timestamp equivalence
            "2024-05-13 23:53:36"),
        Arguments.of(
            2024L, "2", "2024"),
        Arguments.of(
            (long)(1715644416 * 1e6 + 4000000 / 1e3),
            (long)(1715644416 * 1e3 + 4000000 / 1e6),
            (long)(1715644416 * 1e6 + 4000000 / 1e3)),
        Arguments.of(
            (long)(1715644416 * 1e6 + 4000000 / 1e3),
            (long)(1715644416 * 1e3 + 4000000 / 1e6),
            Long.toString((long)(1715644416 * 1e6 + 4000000 / 1e3))),
        // Test higher precision that only micro sec unit can capture.
        Arguments.of(
            (long)(1715644416 * 1e6 + 4000000 / 1e6),
            "2024-05-13T23:53:36.000", // Timestamp equivalence
            "2024-05-13T23:53:36.000004"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e6),
            "2024-05-13 23:53:36.000", // Timestamp equivalence
            "2024-05-13 23:53:36.000004"),
        // Test full range of time
        Arguments.of(
            0L,
            "1970-01-01T00:00:00.000", // Timestamp equivalence
            "1970-01-01T00:00:00.000000"),
        Arguments.of(
            0L,
            "1970-01-01 00:00:00.000", // Timestamp equivalence
            "1970-01-01 00:00:00.000000"),
        Arguments.of(
            Long.MAX_VALUE,
            "+294247-01-10T04:00:54.775", // Timestamp in far future must be prefixed with '+'
            "+294247-01-10T04:00:54.775807"),
        Arguments.of(
            Long.MAX_VALUE,
            "+294247-01-10 04:00:54.775", // Timestamp in far future must be prefixed with '+'
            "+294247-01-10 04:00:54.775807"),
        Arguments.of(
            0L, 0L, 0L),
        Arguments.of(
            -1L * 1000, -1L, -1L * 1000),
        Arguments.of(
            Long.MIN_VALUE, Long.MIN_VALUE / 1000, Long.MIN_VALUE),
        Arguments.of(
            Long.MAX_VALUE, Long.MAX_VALUE / 1000, Long.MAX_VALUE),
        Arguments.of(
            -62167219200000000L, "0000-01-01T00:00:00.00000", "0000-01-01T00:00:00.00000"),
        Arguments.of(
            -62167219200000000L, -62167219200000000L / 1000, -62167219200000000L),
        Arguments.of(
            -62167219200000000L, "0000-01-01 00:00:00.00000", "0000-01-01 00:00:00.00000"),
        Arguments.of(
            -62167219200000000L, -62167219200000000L / 1000, -62167219200000000L)
    );
  }

  static Stream<Object> localTimestampBadCaseProvider() {
    return Stream.of(
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "2024-05-1323:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "2024-05-1T23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "2024-05-1 23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "2024-0-13T23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "2024-0-13 23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "20242-05-13T23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "20242-05-13 23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "202-05-13T23:53:36.0000000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "202-05-13 23:53:36.0000000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "202-05-13T23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "202-05-13 23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "2024-05-13T23:53:36.000Z"),
        Arguments.of(LOCAL_TIMESTAMP_MILLI_AVRO_FILE_PATH, "2024-05-13 23:53:36.000Z"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024-05-1323:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024-05-1T23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024-05-1 23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024-0-13T23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024-0-13 23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "20242-05-13T23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "20242-05-13 23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "202-05-13T23:53:36.0000000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "202-05-13 23:53:36.0000000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "202-05-13T23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "202-05-13 23:53:36.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2022-05-13T99:99:99.000"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024-05-13T23:53:36.000Z"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024-05-13 23:53:36.000Z"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "Not a timestamp at all!"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024 05 13T23:00"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024 05 13 23:00"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2024-05"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2011-12-03T10:15:30+01:00"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2011-12-03 10:15:30+01:00"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2011-12-03T10:15:30[Europe/ Paris]"),
        Arguments.of(LOCAL_TIMESTAMP_MICRO_AVRO_FILE_PATH, "2011-12-03 10:15:30[Europe/ Paris]")
    );
  }

  static Stream<Object> timestampGoodCaseProvider() {
    return Stream.of(
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3), // Num of micro sec since unix epoch
            "2024-05-13T23:53:36.004Z", // Timestamp equivalence
            "2024-05-13T23:53:36.004Z"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3), // Num of micro sec since unix epoch
            "2024-05-13 23:53:36.004Z", // Timestamp equivalence
            "2024-05-13 23:53:36.004Z"),
        Arguments.of(
            (long) (1715644416 * 1e6), // Num of micro sec since unix epoch
            "2024-05-13T23:53:36Z", // Timestamp equivalence
            "2024-05-13T23:53:36Z"),
        Arguments.of(
            (long) (1715644416 * 1e6), // Num of micro sec since unix epoch
            "2024-05-13 23:53:36Z", // Timestamp equivalence
            "2024-05-13 23:53:36Z"),
        // Test timestamps with no zone offset
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3),
            "2024-05-13T23:53:36.004",
            "2024-05-13T23:53:36.004"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3),
            "2024-05-13 23:53:36.004",
            "2024-05-13 23:53:36.004"),
        // Test timestamps with different zone offsets
        Arguments.of(
            (long) (1715644416 * 1e6 - 2 * 3600 * 1e6),
            "2024-05-13T23:53:36+02:00",
            "2024-05-13T23:53:36+02:00"),
        Arguments.of(
            (long) (1715644416 * 1e6 - 2 * 3600 * 1e6),
            "2024-05-13 23:53:36+02:00",
            "2024-05-13 23:53:36+02:00"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3),
            "2024-05-13T23:53:36.004+00:00",
            "2024-05-13T23:53:36.004+00:00"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3),
            "2024-05-13 23:53:36.004+00:00",
            "2024-05-13 23:53:36.004+00:00"),
        Arguments.of(
            (long) (1715644416 * 1e6 - 3 * 3600 * 1e6 + 4000000 / 1e3),
            "2024-05-13T23:53:36.004+03:00",
            "2024-05-13T23:53:36.004+03:00"),
        Arguments.of(
            (long) (1715644416 * 1e6 - 3 * 3600 * 1e6 + 4000000 / 1e3),
            "2024-05-13 23:53:36.004+03:00",
            "2024-05-13 23:53:36.004+03:00"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 6 * 3600 * 1e6 + 4000000 / 1e3),
            "2024-05-13T23:53:36.004-06:00",
            "2024-05-13T23:53:36.004-06:00"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 6 * 3600 * 1e6 + 4000000 / 1e3),
            "2024-05-13 23:53:36.004-06:00",
            "2024-05-13 23:53:36.004-06:00"),
        Arguments.of(
            (long) (1715644416 * 1e6 + (8 * 3600 + 1800) * 1e6 + 4000000 / 1e3),
            "2024-05-13T23:53:36.004-08:30",
            "2024-05-13T23:53:36.004-08:30"),
        Arguments.of(
            (long) (1715644416 * 1e6 + (8 * 3600 + 1800) * 1e6 + 4000000 / 1e3),
            "2024-05-13 23:53:36.004-08:30",
            "2024-05-13 23:53:36.004-08:30"),
        Arguments.of(
            2024L, "2", "2024"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3),
            (long) (1715644416 * 1e3 + 4000000 / 1e6),
            (long) (1715644416 * 1e6 + 4000000 / 1e3)),
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e3),
            (long) (1715644416 * 1e3 + 4000000 / 1e6),
            Long.toString((long) (1715644416 * 1e6 + 4000000 / 1e3))),
        // Test higher precision that only micro sec unit can capture.
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e6),
            "2024-05-13T23:53:36.000Z", // Timestamp equivalence
            "2024-05-13T23:53:36.000004Z"),
        Arguments.of(
            (long) (1715644416 * 1e6 + 4000000 / 1e6),
            "2024-05-13 23:53:36.000Z",
            "2024-05-13 23:53:36.000004Z"),
        Arguments.of(
            (long) (1715644416 * 1e6 + (2 * 3600 + 1800) * 1e6 + 4000000 / 1e6),
            "2024-05-13T23:53:36.000-02:30",
            "2024-05-13T23:53:36.000004-02:30"),
        Arguments.of(
            (long) (1715644416 * 1e6 + (2 * 3600 + 1800) * 1e6 + 4000000 / 1e6),
            "2024-05-13 23:53:36.000-02:30",
            "2024-05-13 23:53:36.000004-02:30"),
        // Test full range of time
        Arguments.of(
            0L,
            "1970-01-01T00:00:00.000Z", // Timestamp equivalence
            "1970-01-01T00:00:00.000000Z"),
        Arguments.of(
            (long) (-3600 * 1e6),
            "1970-01-01T00:00:00.000+01:00",
            "1970-01-01T00:00:00.000000+01:00"),
        // The test case leads to long overflow due to how java calculate duration between 2 timestamps
        // Arguments.of(
        //  Long.MAX_VALUE,
        //  "+294247-01-10T04:00:54.775Z", // Timestamp in far future must be prefixed with '+'
        //  "+294247-01-10T04:00:54.775807Z"),
        Arguments.of(
            0L, 0L, 0L),
        Arguments.of(
            -1L * 1000, -1L, -1L * 1000),
        Arguments.of(
            Long.MAX_VALUE, Long.MAX_VALUE / 1000, Long.MAX_VALUE),
        // The test case leads to long overflow due to how java calculate duration between 2 timestamps
        // Arguments.of(
        //  -62167219200000000L, "0000-01-01T00:00:00.00000Z", "0000-01-01T00:00:00.00000Z"),
        Arguments.of(
            -62167219200000000L, -62167219200000000L / 1000, -62167219200000000L)
    );
  }

  static Stream<Object> timestampBadCaseProvider() {
    return Stream.of(
        Arguments.of(TIMESTAMP_AVRO_FILE_PATH, "2024-05-1323:53:36.000"),
        Arguments.of(TIMESTAMP_AVRO_FILE_PATH, "2024-05-1323:99:99.000Z"),
        Arguments.of(TIMESTAMP_AVRO_FILE_PATH, "2024-05-1323:53:36.000 UTC"),
        Arguments.of(TIMESTAMP_AVRO_FILE_PATH, "Tue, 3 Jun 2008 11:05:30 GMT")
    );
  }

  static Stream<Object> timeGoodCaseProvider() {
    return Stream.of(
        // 12 hours and 30 minutes in milliseconds / microseconds
        Arguments.of((long)4.5e10, (int)4.5e7, (long)4.5e10),
        // 12 hours and 30 minutes in milliseconds / microseconds as string
        Arguments.of((long)4.5e10, Integer.toString((int)4.5e7), Long.toString((long)4.5e10)),
        // 12 hours and 30 minutes
        Arguments.of((long)4.5e10, "12:30:00", "12:30:00"),
        Arguments.of(
            (long)(4.5e10 + 1e3), // 12 hours, 30 minutes and 0.001 seconds in microseconds
            "12:30:00.001", // 12 hours, 30 minutes and 0.001 seconds
            "12:30:00.001" // 12 hours, 30 minutes and 0.001 seconds
        ),
        // Test value ranges
        Arguments.of(
            0L,
            "00:00:00.000",
            "00:00:00.00000"
        ),
        Arguments.of(
            86399999990L,
            "23:59:59.999",
            "23:59:59.99999"
        ),
        Arguments.of((long)Integer.MAX_VALUE, Integer.MAX_VALUE / 1000, (long)Integer.MAX_VALUE),
        Arguments.of((long)Integer.MIN_VALUE, Integer.MIN_VALUE / 1000, (long)Integer.MIN_VALUE)
    );
  }

  static Stream<Object> timeBadCaseProvider() {
    return Stream.of(
        Arguments.of("00:0"),
        Arguments.of("00:00:99")
    );
  }

  static Stream<Object> uuidDimension() {
    return Stream.of(
        // Normal UUID
        UUID.randomUUID().toString(),
        // Arbitrary string will also pass as neither Avro library nor json convertor validate the string content.
        "",
        "NotAnUUID"
    );
  }

  static Stream<Object> dateProviderForRow() {
    return Stream.of(
        // 18506 epoch days since Unix epoch is 2020-09-01, while
        // 18506 * MILLI_SECONDS_PER_DAY is 2020-08-31.
        // That's why you see for same 18506 days from avro side we can have different
        // row equivalence.
        Arguments.of("2020-09-01", 18506), // epochDays
        Arguments.of("2020-09-01", "2020-09-01"),  // dateString
        Arguments.of(null, "+22020-09-01"),  // dateString, not supported by row
        Arguments.of("2020-09-01", "18506"),  // epochDaysString, not supported by row
        Arguments.of(null, Integer.toString(Integer.MAX_VALUE)), // not supported by row
        Arguments.of(null, Integer.toString(Integer.MIN_VALUE)) // not supported by row
    );
  }

  static Stream<Object> dataNestedJsonAsString() {
    return Stream.of(
        "{\"first\":\"John\",\"last\":\"Smith\"}",
        "[{\"first\":\"John\",\"last\":\"Smith\"}]",
        "{\"first\":\"John\",\"last\":\"Smith\",\"suffix\":3}"
    );
  }

  static Stream<Object> nestedRecord() {
    return Stream.of(
        Arguments.of("abc@xyz.com", true),
        Arguments.of("{\"primary\":\"def@xyz.com\"}", false)
    );
  }

  static Stream<Object> encodedDecimalScalePrecisionProvider() {
    return Stream.of(
        Arguments.of(6, 10),
        Arguments.of(30, 32),
        Arguments.of(1, 3)
    );
  }

  static Stream<Object> encodedDecimalFixedScalePrecisionProvider() {
    return Stream.of(
        Arguments.of(5, 6, 10),
        Arguments.of(14, 30, 32),
        Arguments.of(2, 1, 3)
    );
  }
}
