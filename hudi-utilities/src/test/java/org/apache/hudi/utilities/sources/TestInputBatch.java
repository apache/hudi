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
    final InputBatch<String> inputBatch = new InputBatch<>(Option.of("foo"), null, null);
    Throwable t = assertThrows(HoodieException.class, inputBatch::getSchemaProvider);
    assertEquals("Please provide a valid schema provider class!", t.getMessage());
  }

  @Test
  public void getSchemaProviderShouldReturnNullSchemaProvider() {
    final InputBatch<String> inputBatch = new InputBatch<>(Option.empty(), null, null);
    SchemaProvider schemaProvider = inputBatch.getSchemaProvider();
    assertTrue(schemaProvider instanceof InputBatch.NullSchemaProvider);
  }

  @Test
  public void getSchemaProviderShouldReturnGivenSchemaProvider() {
    SchemaProvider schemaProvider = new RowBasedSchemaProvider(null);
    final InputBatch<String> inputBatch = new InputBatch<>(Option.of("foo"), null, schemaProvider);
    assertSame(schemaProvider, inputBatch.getSchemaProvider());
  }
}
