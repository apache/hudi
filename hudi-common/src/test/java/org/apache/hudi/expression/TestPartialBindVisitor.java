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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class TestPartialBindVisitor {

  private static Types.RecordType schema;
  @BeforeAll
  public static void init() {
    ArrayList<Types.Field> fields = new ArrayList<>(5);
    fields.add(Types.Field.get(0, true, "a", Types.StringType.get()));
    fields.add(Types.Field.get(1, true, "b", Types.DateType.get()));
    fields.add(Types.Field.get(2, true, "c", Types.IntType.get()));
    fields.add(Types.Field.get(3, true, "d", Types.LongType.get()));
    fields.add(Types.Field.get(4, false, "f", Types.BooleanType.get()));
    schema = Types.RecordType.get(fields, "schema");
  }

  @Test
  public void testPartialBindIfAllExisting() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    Predicates.BinaryComparison eq = Predicates.eq(new NameReference("a"),
        Literal.from("Jane"));
    Predicates.BinaryComparison gt = Predicates.gt(new NameReference("c"),
        Literal.from(10));
    Predicates.In in = Predicates.in(new NameReference("d"),
        Arrays.asList(Literal.from(10L), Literal.from(13L)));

    Predicates.And expr = Predicates.and(eq, Predicates.or(gt, in));
    Expression binded = expr.accept(partialBindVisitor);

    Assertions.assertTrue((Boolean) binded.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertTrue((Boolean) binded.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 5, 10L, false))));
    Assertions.assertFalse((Boolean) binded.eval(new ArrayData(Arrays.asList("Lone", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded.eval(new ArrayData(Arrays.asList("Lone", "2023-04-02", 10, 5L, false))));
  }

  @Test
  public void testPartialBindIfFieldMissing() {
    PartialBindVisitor partialBindVisitor = new PartialBindVisitor(schema, false);

    Predicates.BinaryComparison eq = Predicates.eq(new NameReference("a"),
        Literal.from("Jane"));
    Predicates.BinaryComparison lt = Predicates.lt(new NameReference("m"),
        Literal.from(10));
    Predicates.BinaryComparison gteq = Predicates.gteq(new NameReference("d"),
        Literal.from(10L));

    Predicates.And expr = Predicates.and(eq, Predicates.or(lt, gteq));
    // Since Attribute m does not exist in the schema, so the OR expression is always true,
    // the expression is optimized to only consider the EQ expression
    Expression binded = expr.accept(partialBindVisitor);

    Assertions.assertTrue((Boolean) binded.eval(new ArrayData(Arrays.asList("Jane", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded.eval(new ArrayData(Arrays.asList("Lone", "2023-04-02", 15, 5L, false))));
    Assertions.assertFalse((Boolean) binded.eval(new ArrayData(Arrays.asList("Lone", "2023-04-02", 10, 5L, false))));
  }
}
