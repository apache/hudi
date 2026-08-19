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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.avro.VariantShreddingSchemaInferrer.VariantSample;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The branches of {@link SparkVariantSampleExtractor} that need no Spark adapter: ordinal
 * resolution, the absent-column and non-row (delete payload) legs, and the shared-size estimate.
 * Extraction of a present column goes through {@code SparkAdapter.extractVariantBinary} and is
 * covered by the functional inference tests.
 */
public class TestSparkVariantSampleExtractor {

  private static final HoodieSchema SCHEMA = HoodieSchema.createRecord("rec", "ns", null, Arrays.asList(
      HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.LONG)),
      HoodieSchemaField.of("v", HoodieSchema.createNullable(HoodieSchema.createVariant()))));
  private static final StructType STRUCT_TYPE = new StructType()
      .add("id", DataTypes.LongType)
      .add("v", DataTypes.BinaryType);
  private static final Properties PROPS = new Properties();
  private static final HoodieKey KEY = new HoodieKey("r1", "p");

  @Test
  public void testAbsentColumnYieldsNullSample() throws Exception {
    // "w" is not in the row's StructType: sampled as null (ordinal -1), never through the adapter.
    SparkVariantSampleExtractor extractor = new SparkVariantSampleExtractor(singletonList("w"), STRUCT_TYPE);
    GenericInternalRow row = new GenericInternalRow(new Object[] {1L, null});
    VariantSample[] samples = extractor.extract(new HoodieSparkRecord(KEY, row, STRUCT_TYPE, false), SCHEMA, PROPS);
    assertEquals(1, samples.length);
    assertNull(samples[0]);
  }

  @Test
  public void testNonRowDataYieldsNullSamples() throws Exception {
    // A record whose data is not an InternalRow (a delete payload) contributes one empty slot per column.
    SparkVariantSampleExtractor extractor = new SparkVariantSampleExtractor(Arrays.asList("v", "w"), STRUCT_TYPE);
    GenericRecord avroData = new GenericData.Record(SCHEMA.toAvroSchema());
    avroData.put("id", 1L);
    VariantSample[] samples = extractor.extract(new HoodieAvroIndexedRecord(KEY, avroData), SCHEMA, PROPS);
    assertEquals(2, samples.length);
    assertNull(samples[0]);
    assertNull(samples[1]);
  }

  @Test
  public void testSharedSizeIsTheStructType() {
    // Every Spark record of the file references the (cached) StructType; the decorator subtracts
    // it from each record's size estimate so the byte cap is not consumed by the schema.
    SparkVariantSampleExtractor extractor = new SparkVariantSampleExtractor(singletonList("v"), STRUCT_TYPE);
    assertTrue(extractor.sharedSizeEstimate(SCHEMA) > 0);
    assertEquals(extractor.sharedSizeEstimate(SCHEMA), extractor.sharedSizeEstimate(SCHEMA));
  }
}
