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

package org.apache.hudi.util;

import org.apache.hudi.common.model.WriteOperationType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link OperationConverter}, the jcommander string-to-enum converter.
 */
public class TestOperationConverter {

  private final OperationConverter converter = new OperationConverter();

  @Test
  void convertsEnumConstantNames() {
    assertSame(WriteOperationType.INSERT, converter.convert("INSERT"));
    assertSame(WriteOperationType.UPSERT, converter.convert("UPSERT"));
    assertSame(WriteOperationType.BULK_INSERT, converter.convert("BULK_INSERT"));
  }

  @Test
  void rejectsUnknownOperation() {
    assertThrows(IllegalArgumentException.class, () -> converter.convert("not_an_operation"));
  }
}
