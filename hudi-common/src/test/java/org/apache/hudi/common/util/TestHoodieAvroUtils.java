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

package org.apache.hudi.common.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests hoodie avro utilities.
 */
public class TestHoodieAvroUtils {

  private static String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
    + "{\"name\": \"new_col1\", \"type\": \"string\", \"default\": \"dummy_val\"},"
    + "{\"name\": \"new_col2\", \"type\": [\"int\", \"null\"]}]}";

  @Test
  public void testPropsPresent() {
    Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    boolean piiPresent = false;
    for (Schema.Field field : schema.getFields()) {
      if (HoodieAvroUtils.isMetadataField(field.name())) {
        continue;
      }

      Assert.assertNotNull("field name is null", field.name());
      Map<String, Object> props = field.getObjectProps();
      Assert.assertNotNull("The property is null", props);

      if (field.name().equals("pii_col")) {
        piiPresent = true;
        Assert.assertTrue("sensitivity_level is removed in field 'pii_col'", props.containsKey("column_category"));
      } else {
        Assert.assertEquals("The property shows up but not set", 0, props.size());
      }
    }
    Assert.assertTrue("column pii_col doesn't show up", piiPresent);
  }

  @Test
  public void testDefaultValue() {
    GenericRecord rec = new GenericData.Record(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    rec.put("_row_key", "key1");
    rec.put("non_pii_col", "val1");
    rec.put("pii_col", "val2");
    rec.put("timestamp", 3.5);
    GenericRecord rec1 = HoodieAvroUtils.rewriteRecord(rec, new Schema.Parser().parse(EXAMPLE_SCHEMA));
    Assert.assertEquals(rec1.get("new_col1"), "dummy_val");
    Assert.assertNull(rec1.get("new_col2"));
  }
}
