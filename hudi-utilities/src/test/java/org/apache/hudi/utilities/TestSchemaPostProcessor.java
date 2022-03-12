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

package org.apache.hudi.utilities;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.exception.HoodieSchemaPostProcessException;
import org.apache.hudi.utilities.schema.DeleteSupportSchemaPostProcessor;
import org.apache.hudi.utilities.schema.DropColumnSchemaPostProcessor;
import org.apache.hudi.utilities.schema.SchemaPostProcessor;
import org.apache.hudi.utilities.schema.SchemaPostProcessor.Config;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.schema.SparkAvroPostProcessor;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.hudi.utilities.transform.FlatteningTransformer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestSchemaPostProcessor extends UtilitiesTestBase {

  private final TypedProperties properties = new TypedProperties();

  private static final String ORIGINAL_SCHEMA = "{\"type\":\"record\",\"name\":\"tripUberRec\",\"fields\":"
      + "[{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"rider\","
      + "\"type\":\"string\"},{\"name\":\"driver\",\"type\":\"string\"},{\"name\":\"fare\",\"type\":\"double\"}]}";

  private static final String RESULT_SCHEMA = "{\"type\":\"record\",\"name\":\"hoodie_source\","
      + "\"namespace\":\"hoodie.source\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\"},"
      + "{\"name\":\"_row_key\",\"type\":\"string\"},{\"name\":\"rider\",\"type\":\"string\"},{\"name\":\"driver\","
      + "\"type\":\"string\"},{\"name\":\"fare\",\"type\":\"double\"}]}";

  @Test
  public void testPostProcessor() throws IOException {
    properties.put(Config.SCHEMA_POST_PROCESSOR_PROP, DummySchemaPostProcessor.class.getName());
    SchemaProvider provider =
        UtilHelpers.wrapSchemaProviderWithPostProcessor(
        UtilHelpers.createSchemaProvider(DummySchemaProvider.class.getName(), properties, jsc),
            properties, jsc,null);

    Schema schema = provider.getSourceSchema();
    assertEquals(schema.getType(), Type.RECORD);
    assertEquals(schema.getName(), "test");
    assertNotNull(schema.getField("testString"));
  }

  @Test
  public void testSparkAvro() throws IOException {
    properties.put(Config.SCHEMA_POST_PROCESSOR_PROP, SparkAvroPostProcessor.class.getName());
    List<String> transformerClassNames = new ArrayList<>();
    transformerClassNames.add(FlatteningTransformer.class.getName());

    SchemaProvider provider =
            UtilHelpers.wrapSchemaProviderWithPostProcessor(
                    UtilHelpers.createSchemaProvider(SparkAvroSchemaProvider.class.getName(), properties, jsc),
                    properties, jsc, transformerClassNames);

    Schema schema = provider.getSourceSchema();
    assertEquals(schema.getType(), Type.RECORD);
    assertEquals(schema.getName(), "hoodie_source");
    assertEquals(schema.getNamespace(), "hoodie.source");
    assertNotNull(schema.getField("day"));
  }

  @Test
  public void testDeleteSupport() {
    DeleteSupportSchemaPostProcessor processor = new DeleteSupportSchemaPostProcessor(properties, null);
    Schema schema = new Schema.Parser().parse(ORIGINAL_SCHEMA);
    Schema targetSchema = processor.processSchema(schema);
    assertNotNull(targetSchema.getField("_hoodie_is_deleted"));
  }

  @Test
  public void testChainedSchemaPostProcessor() {
    // DeleteSupportSchemaPostProcessor first, DummySchemaPostProcessor second
    properties.put(Config.SCHEMA_POST_PROCESSOR_PROP,
        "org.apache.hudi.utilities.schema.DeleteSupportSchemaPostProcessor,org.apache.hudi.utilities.DummySchemaPostProcessor");

    SchemaPostProcessor processor = UtilHelpers.createSchemaPostProcessor(properties.getString(Config.SCHEMA_POST_PROCESSOR_PROP), properties, jsc);
    Schema schema = new Schema.Parser().parse(ORIGINAL_SCHEMA);
    Schema targetSchema = processor.processSchema(schema);

    assertNull(targetSchema.getField("_row_key"));
    assertNull(targetSchema.getField("_hoodie_is_deleted"));
    assertNotNull(targetSchema.getField("testString"));

    // DummySchemaPostProcessor first, DeleteSupportSchemaPostProcessor second
    properties.put(Config.SCHEMA_POST_PROCESSOR_PROP,
        "org.apache.hudi.utilities.DummySchemaPostProcessor,org.apache.hudi.utilities.schema.DeleteSupportSchemaPostProcessor");

    processor = UtilHelpers.createSchemaPostProcessor(properties.getString(Config.SCHEMA_POST_PROCESSOR_PROP), properties, jsc);
    schema = new Schema.Parser().parse(ORIGINAL_SCHEMA);
    targetSchema = processor.processSchema(schema);

    assertNull(targetSchema.getField("_row_key"));
    assertNotNull(targetSchema.getField("_hoodie_is_deleted"));
    assertNotNull(targetSchema.getField("testString"));
  }

  @Test
  public void testDeleteColumn() {
    // remove column ums_id_ from source schema
    properties.put(DropColumnSchemaPostProcessor.Config.DELETE_COLUMN_POST_PROCESSOR_COLUMN_PROP, "rider");
    DropColumnSchemaPostProcessor processor = new DropColumnSchemaPostProcessor(properties, null);
    Schema schema = new Schema.Parser().parse(ORIGINAL_SCHEMA);
    Schema targetSchema = processor.processSchema(schema);

    assertNull(targetSchema.getField("rider"));
    assertNotNull(targetSchema.getField("_row_key"));
  }

  @Test
  public void testDeleteColumnThrows() {
    // remove all columns from source schema
    properties.put(DropColumnSchemaPostProcessor.Config.DELETE_COLUMN_POST_PROCESSOR_COLUMN_PROP, "timestamp,_row_key,rider,driver,fare");
    DropColumnSchemaPostProcessor processor = new DropColumnSchemaPostProcessor(properties, null);
    Schema schema = new Schema.Parser().parse(ORIGINAL_SCHEMA);

    Assertions.assertThrows(HoodieSchemaPostProcessException.class, () -> processor.processSchema(schema));
  }

  @Test
  public void testSparkAvroSchema() throws IOException {
    SparkAvroPostProcessor processor = new SparkAvroPostProcessor(properties, null);
    Schema schema = new Schema.Parser().parse(ORIGINAL_SCHEMA);
    assertEquals(processor.processSchema(schema).toString(), RESULT_SCHEMA);
  }
}
