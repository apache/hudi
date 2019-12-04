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
import org.codehaus.jackson.JsonNode;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests hoodie avro utilities.
 */
public class TestHoodieAvroUtils {

  private static String EXAMPLE_SCHEMA = "{\"type\": \"record\"," + "\"name\": \"testrec\"," + "\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"}," + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  @Test
  public void testPropsPresent() {
    Schema schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(EXAMPLE_SCHEMA));
    boolean piiPresent = false;
    for (Schema.Field field : schema.getFields()) {
      if (HoodieAvroUtils.isMetadataField(field.name())) {
        continue;
      }

      Assert.assertTrue("field name is null", field.name() != null);
      Map<String, JsonNode> props = field.getJsonProps();
      Assert.assertTrue("The property is null", props != null);

      if (field.name().equals("pii_col")) {
        piiPresent = true;
        Assert.assertTrue("sensitivity_level is removed in field 'pii_col'", props.containsKey("column_category"));
      } else {
        Assert.assertTrue("The property shows up but not set", props.size() == 0);
      }
    }
    Assert.assertTrue("column pii_col doesn't show up", piiPresent);
  }
}
