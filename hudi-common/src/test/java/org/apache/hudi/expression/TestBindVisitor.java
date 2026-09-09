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

package org.apache.hudi.expression;

import org.apache.hudi.internal.schema.Types;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestBindVisitor {

  private static Types.RecordType schema() {
    ArrayList<Types.Field> fields = new ArrayList<>(1);
    fields.add(Types.Field.get(0, true, "a", Types.StringType.get()));
    return Types.RecordType.get(fields, "schema");
  }

  @Test
  void testUnsupportedPredicateErrorNamesTheExpression() {
    BindVisitor bindVisitor = new BindVisitor(schema(), true);
    Predicates.StringStartsWithAny startsWithAny =
        Predicates.startsWithAny(new NameReference("a"), Collections.singletonList(Literal.from("Ja")));

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> startsWithAny.accept(bindVisitor));

    assertTrue(e.getMessage().contains("NameReference(name=a).startsWithAny(Ja)"), e::getMessage);
    assertFalse(e.getMessage().contains(BindVisitor.class.getName()), e::getMessage);
    assertTrue(e.getMessage().contains(" cannot be visited as predicate"), e::getMessage);
  }
}
