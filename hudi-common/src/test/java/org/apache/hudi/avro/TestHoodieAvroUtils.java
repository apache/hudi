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

import java.util.Map;
import org.apache.avro.JsonProperties.Null;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie avro utilities.
 */
public class TestHoodieAvroUtils {

  private static String EVOLVED_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec1\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"new_col1\", \"type\": \"string\", \"default\": \"dummy_val\"},"
      + "{\"name\": \"new_col2\", \"type\": [\"int\", \"null\"]}]}";

  private static String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  private static String SCHEMA_WITH_METADATA_FIELD =
      "{\"type\": \"record\",\"name\": \"testrec2\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
      + "{\"name\": \"_hoodie_commit_time\", \"type\": [\"null\", \"string\"]},"
      + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null}]}";

  private static String SCHEMA_WITH_UNIONS = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\":"
      + " [{\"name\": \"recordWithoutDefault\",\"type\": [\"null\",{\"type\": \"record\",\"name\": \"Record\",\"fields\": "
      + "[{\"name\": \"fieldWithWrongTypeSort\",\"type\": [\"string\",\"null\"]}]}]},"
      + "{\"name\": \"fieldWithoutDefault\",\"type\": \"string\"},{\"name\": \"fieldWithNoNullType\",\"type\": "
      + "[\"string\",\"boolean\"]}]}";

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
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EVOLVED_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(EVOLVED_SCHEMA));
    assertEquals(rec1.get("new_col1"), "dummy_val");
    assertNull(rec1.get("new_col2"));
  }

  @Test
  public void testDefaultValueWithSchemaEvolution() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(EVOLVED_SCHEMA));
    assertEquals(rec1.get("new_col1"), "dummy_val");
    assertNull(rec1.get("new_col2"));
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
  }

  @Test
  public void testRewriteIncorrectDefaults() {
    Schema schema = new Schema.Parser().parse(SCHEMA_WITH_UNIONS);

    HoodieAvroUtils.rewriteIncorrectDefaults(schema);
    Field updatedRecord = schema.getFields().get(0);
    Assert.assertTrue(updatedRecord.defaultVal() instanceof Null);
    Assert.assertEquals(updatedRecord.schema().getTypes().size(), 2);
    Assert.assertEquals(updatedRecord.schema().getTypes().get(0).getType(), Type.NULL);
    Assert.assertEquals(updatedRecord.schema().getTypes().get(1).getType(), Type.RECORD);

    Field updatedUnion = schema.getFields().get(0).schema().getTypes().get(1).getFields().get(0);
    Assert.assertTrue(updatedUnion.defaultVal() instanceof Null);
    Assert.assertEquals(updatedUnion.schema().getTypes().size(), 2);
    Assert.assertEquals(updatedUnion.schema().getTypes().get(0).getType(), Type.NULL);
    Assert.assertEquals(updatedUnion.schema().getTypes().get(1).getType(), Type.STRING);

    Field notNullString = schema.getFields().get(1);
    Assert.assertNull(notNullString.defaultVal());
    Assert.assertEquals(notNullString.schema().getType(), Type.STRING);

    Field notNullUnion = schema.getFields().get(2);
    Assert.assertNull(notNullUnion.defaultVal());
    Assert.assertEquals(notNullUnion.schema().getType(), Type.UNION);
    Assert.assertEquals(notNullUnion.schema().getTypes().size(), 2);
    Assert.assertEquals(notNullUnion.schema().getTypes().get(0).getType(), Type.STRING);
    Assert.assertEquals(notNullUnion.schema().getTypes().get(1).getType(), Type.BOOLEAN);
  }
}
