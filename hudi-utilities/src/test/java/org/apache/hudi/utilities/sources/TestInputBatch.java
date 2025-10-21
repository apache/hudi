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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestInputBatch {

  @Test
  public void getSchemaProviderShouldThrowException() {
    final InputBatch<String> inputBatch = new InputBatch<>(Option.of("foo"), (String) null, null);
    Throwable t = assertThrows(HoodieException.class, inputBatch::getSchemaProvider);
    assertEquals("Schema provider is required for this operation and for the source of interest. "
        + "Please set '--schemaprovider-class' in the top level HoodieStreamer config for the source of interest. "
        + "Based on the schema provider class chosen, additional configs might be required. "
        + "For eg, if you choose 'org.apache.hudi.utilities.schema.SchemaRegistryProvider', "
        + "you may need to set configs like 'hoodie.streamer.schemaprovider.registry.url'.", t.getMessage());
  }

  @Test
  public void getSchemaProviderShouldReturnNullSchemaProvider() {
    final InputBatch<String> inputBatch = new InputBatch<>(Option.empty(), (String) null, null);
    SchemaProvider schemaProvider = inputBatch.getSchemaProvider();
    assertTrue(schemaProvider instanceof InputBatch.NullSchemaProvider);
  }

  @Test
  public void getSchemaProviderShouldReturnGivenSchemaProvider() {
    SchemaProvider schemaProvider = new RowBasedSchemaProvider(null);
    final InputBatch<String> inputBatch = new InputBatch<>(Option.of("foo"), (String) null, schemaProvider);
    assertSame(schemaProvider, inputBatch.getSchemaProvider());
  }
}
