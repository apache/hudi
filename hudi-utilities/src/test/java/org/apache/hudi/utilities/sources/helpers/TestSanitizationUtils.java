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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.testutils.SanitizationTestUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest;
import static org.apache.hudi.utilities.testutils.SanitizationTestUtils.generateProperFormattedSchema;
import static org.apache.hudi.utilities.testutils.SanitizationTestUtils.generateRenamedSchemaWithConfiguredReplacement;
import static org.apache.hudi.utilities.testutils.SanitizationTestUtils.generateRenamedSchemaWithDefaultReplacement;
import static org.apache.hudi.utilities.testutils.SanitizationTestUtils.invalidCharMask;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestSanitizationUtils {

  protected static SparkSession spark;
  protected static JavaSparkContext jsc;

  @BeforeAll
  public static void start() {
    spark = SparkSession
        .builder()
        .config(getSparkConfForTest(TestSanitizationUtils.class.getName()))
        .getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterAll
  public static void shutdown() throws IOException {
    jsc.close();
    spark.close();
  }

  @ParameterizedTest
  @MethodSource("provideDataFiles")
  public void testSanitizeDataset(String unsanitizedDataFile, String sanitizedDataFile, StructType unsanitizedSchema, StructType sanitizedSchema) {
    Dataset<Row> expectedSanitizedDataset = spark.read().schema(sanitizedSchema).format("json").load(sanitizedDataFile);
    Dataset<Row> unsanitizedDataset = spark.read().schema(unsanitizedSchema).format("json").load(unsanitizedDataFile);
    Dataset<Row> sanitizedDataset = SanitizationUtils.sanitizeColumnNamesForAvro(unsanitizedDataset, invalidCharMask);
    assertEquals(unsanitizedDataset.count(), sanitizedDataset.count());
    assertEquals(expectedSanitizedDataset.schema(), sanitizedDataset.schema());
    assertEquals(expectedSanitizedDataset.collectAsList(), sanitizedDataset.collectAsList());
  }

  private void testSanitizeSchema(String unsanitizedSchema, HoodieSchema expectedSanitizedSchema) {
    testSanitizeSchema(unsanitizedSchema, expectedSanitizedSchema, true);
  }

  private void testSanitizeSchema(String unsanitizedSchema, HoodieSchema expectedSanitizedSchema, boolean shouldSanitize) {
    testSanitizeSchema(unsanitizedSchema, expectedSanitizedSchema, shouldSanitize, invalidCharMask);
  }

  private void testSanitizeSchema(String unsanitizedSchema, HoodieSchema expectedSanitizedSchema, boolean shouldSanitize, String charMask) {
    HoodieSchema sanitizedSchema = SanitizationUtils.parseAvroSchema(unsanitizedSchema, shouldSanitize, charMask);
    assertEquals(sanitizedSchema, expectedSanitizedSchema);
  }

  @Test
  public void testGoodAvroSchema() {
    String goodJson = getJson("src/test/resources/streamer-config/file_schema_provider_valid.avsc");
    testSanitizeSchema(goodJson, generateProperFormattedSchema());
  }

  @Test
  public void testBadAvroSchema() {
    String badJson = getJson("src/test/resources/streamer-config/file_schema_provider_invalid.avsc");
    testSanitizeSchema(badJson, generateRenamedSchemaWithDefaultReplacement());
  }

  @Test
  public void testBadAvroSchemaAltCharMask() {
    String badJson = getJson("src/test/resources/streamer-config/file_schema_provider_invalid.avsc");
    testSanitizeSchema(badJson, generateRenamedSchemaWithConfiguredReplacement(),true, "_");
  }

  @Test
  public void testBadAvroSchemaDisabledTest() {
    String badJson = getJson("src/test/resources/streamer-config/file_schema_provider_invalid.avsc");
    assertThrows(HoodieAvroSchemaException.class, () -> testSanitizeSchema(badJson, generateRenamedSchemaWithDefaultReplacement(), false));
  }

  String getJson(String path) {
    FileSystem fs = HadoopFSUtils.getFs(path, jsc.hadoopConfiguration(), true);
    String schemaStr;
    try (InputStream in = fs.open(new Path(path))) {
      schemaStr = FileIOUtils.readAsUTFString(in);
    } catch (IOException e) {
      throw new HoodieIOException("can't read schema file", e);
    }
    return schemaStr;
  }

  private static Stream<Arguments> provideDataFiles() {
    return SanitizationTestUtils.provideDataFiles();
  }

}
