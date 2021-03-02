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

package org.apache.hudi.keygen;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.KeyGeneratorTestUtilities;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestRangePartitionKeyGenerator extends KeyGeneratorTestUtilities {

  private GenericRecord baseRecord;
  private TypedProperties properties = new TypedProperties();

  private Schema schema;
  private StructType structType;
  private Row baseRow;

  @BeforeEach
  public void initialize() throws IOException {
    schema = SchemaTestUtil.getComplexEvolvedSchema();
    structType = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    baseRecord = SchemaTestUtil
        .generateAvroRecordFromJson(schema, 1, "001", "f1");
    baseRow = genericRecordToRow(baseRecord, schema, structType);
  }

  private TypedProperties getCommonProps(String partitionField) {
    TypedProperties properties = new TypedProperties();
    properties.put(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY, "field1");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY, partitionField);
    properties.put(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_OPT_KEY, "true");
    properties.put(RangePartitionAvroKeyGenerator.Config.RANGE_PER_PARTITION_PROP, "10000");
    properties.put(RangePartitionAvroKeyGenerator.Config.RANGE_PARTITION_NAME_PROP, "bucket");
    return properties;
  }

  @Test
  public void testRangePartitionKeyGenerator() {
    // test long
    baseRecord.put("favoriteNumber", 100000L);
    properties = getCommonProps("favoriteNumber");
    RangePartitionKeyGenerator keyGen = new RangePartitionKeyGenerator(properties);
    HoodieKey hk1 = keyGen.getKey(baseRecord);
    assertEquals("bucket=10", hk1.getPartitionPath());
    // test Row
    baseRow = genericRecordToRow(baseRecord, schema, structType);
    assertEquals("bucket=10", keyGen.getPartitionPath(baseRow));

    // test int
    baseRecord.put("favoriteIntNumber", 110000);
    properties = getCommonProps("favoriteIntNumber");
    keyGen = new RangePartitionKeyGenerator(properties);
    HoodieKey hk2 = keyGen.getKey(baseRecord);
    assertEquals("bucket=11", hk2.getPartitionPath());
    // test Row
    baseRow = genericRecordToRow(baseRecord, schema, structType);
    assertEquals("bucket=11", keyGen.getPartitionPath(baseRow));

    // test double
    baseRecord.put("favoriteDoubleNumber", 120000.1d);
    properties = getCommonProps("favoriteDoubleNumber");
    keyGen = new RangePartitionKeyGenerator(properties);
    HoodieKey hk3 = keyGen.getKey(baseRecord);
    assertEquals("bucket=12", hk3.getPartitionPath());
    // test Row
    baseRow = genericRecordToRow(baseRecord, schema, structType);
    assertEquals("bucket=12", keyGen.getPartitionPath(baseRow));

    // test float
    baseRecord.put("favoriteFloatNumber", 130000.123f);
    properties = getCommonProps("favoriteFloatNumber");
    keyGen = new RangePartitionKeyGenerator(properties);
    HoodieKey hk4 = keyGen.getKey(baseRecord);
    assertEquals("bucket=13", hk4.getPartitionPath());
    // test Row
    baseRow = genericRecordToRow(baseRecord, schema, structType);
    assertEquals("bucket=13", keyGen.getPartitionPath(baseRow));

    // test string
    baseRecord.put("field2", "140000");
    properties = getCommonProps("field2");
    keyGen = new RangePartitionKeyGenerator(properties);
    HoodieKey hk5 = keyGen.getKey(baseRecord);
    assertEquals("bucket=14", hk5.getPartitionPath());
    // test Row
    baseRow = genericRecordToRow(baseRecord, schema, structType);
    assertEquals("bucket=14", keyGen.getPartitionPath(baseRow));
  }

}
