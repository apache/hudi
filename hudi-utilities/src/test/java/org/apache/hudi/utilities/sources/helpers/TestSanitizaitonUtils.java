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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.utilities.testutils.SanitizationTestBase;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSanitizaitonUtils extends SanitizationTestBase {

  @ParameterizedTest
  @MethodSource("provideDataFiles")
  public void testSanitizeDataset(String unsanitizedDataFile, String sanitizedDataFile, StructType unsanitizedSchema, StructType sanitizedSchema) {
    Dataset<Row> expectedSantitizedDataset = spark.read().schema(sanitizedSchema).format("json").load(sanitizedDataFile);
    Dataset<Row> unsanitizedDataset = spark.read().schema(unsanitizedSchema).format("json").load(unsanitizedDataFile);
    Dataset<Row> sanitizedDataset = SanitizationUtils.sanitizeColumnNamesForAvro(unsanitizedDataset,invalidCharMask);
    assertEquals(unsanitizedDataset.count(), sanitizedDataset.count());
    assertEquals(expectedSantitizedDataset.schema(), sanitizedDataset.schema());
    assertEquals(expectedSantitizedDataset.collectAsList(), sanitizedDataset.collectAsList());
  }

  private void testSanitizeSchema(String unsanitizedSchema, Schema expectedSanitizedSchema) {
    Schema sanitizedSchema = SanitizationUtils.parseAvroSchema(unsanitizedSchema, true, invalidCharMask);
    assertEquals(sanitizedSchema, expectedSanitizedSchema);
  }

  @Test
  public void testGoodAvroSchema() {
    String goodJson = getJson("src/test/resources/delta-streamer-config/file_schema_provider_valid.avsc");
    testSanitizeSchema(goodJson,generateProperFormattedSchema());
  }

  @Test
  public void testBadAvroSchema() {
    String badJson = getJson("src/test/resources/delta-streamer-config/file_schema_provider_invalid.avsc");
    testSanitizeSchema(badJson,generateRenamedSchemaWithDefaultReplacement());
  }

  private String getJson(String path) {
    FileSystem fs = FSUtils.getFs(path, jsc.hadoopConfiguration(), true);
    String schemaStr;
    try (FSDataInputStream in = fs.open(new Path(path))) {
      schemaStr = FileIOUtils.readAsUTFString(in);
    } catch (IOException e) {
      throw new HoodieIOException("can't read schema file", e);
    }
    return schemaStr;
  }

}
