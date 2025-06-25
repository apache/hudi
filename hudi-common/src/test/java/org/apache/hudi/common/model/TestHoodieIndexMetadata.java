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

package org.apache.hudi.common.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test cases for {@link HoodieIndexMetadata}.
 */
public class TestHoodieIndexMetadata {
  @Test
  void testSerDeWithIgnoredFields() throws Exception {
    HoodieIndexDefinition def = new HoodieIndexDefinition("index_a", "type_c", "func_d",
        Arrays.asList("a", "b", "c"), Collections.emptyMap());
    assertThat(def.getSourceFieldsKey(), is("a.b.c"));
    HoodieIndexMetadata indexMetadata = new HoodieIndexMetadata(Collections.singletonMap("index_a", def));
    String serialized = indexMetadata.toJson();
    assertFalse(serialized.contains("sourceFieldsKey"), "The field 'sourceFieldsKey' should be ignored in serialization");
    HoodieIndexMetadata deserialized = HoodieIndexMetadata.fromJson(serialized);
    Map<String, HoodieIndexDefinition> indexDefinitionMap = deserialized.getIndexDefinitions();
    assertThat(indexDefinitionMap.size(), is(1));
    assertThat(indexDefinitionMap.values().iterator().next().getSourceFieldsKey(), is("a.b.c"));
  }
}
