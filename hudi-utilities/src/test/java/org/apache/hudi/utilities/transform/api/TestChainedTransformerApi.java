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

package org.apache.hudi.utilities.transform.api;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.transform.ChainedTransformer;
import org.apache.hudi.utilities.transform.TestChainedTransformer.SchemaTransformer;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestChainedTransformerApi {
  private static final String TRANSFORMER = SchemaTransformer.class.getName();

  @Test
  void splitsTrimmedPlainAndScopedNames() {
    Pair<Option<String>, String> plain = ChainedTransformer.splitTransformer(" " + TRANSFORMER + " ");
    assertFalse(plain.getLeft().isPresent());
    assertEquals(TRANSFORMER, plain.getRight());
    Pair<Option<String>, String> scoped = ChainedTransformer.splitTransformer(" first : " + TRANSFORMER + " ");
    assertEquals("first", scoped.getLeft().get());
    assertEquals(TRANSFORMER, scoped.getRight());
  }

  @Test
  void loadsConfiguredTransformerThroughPublicFactory() {
    ChainedTransformer.TransformerInfo info = ChainedTransformer.TransformerInfo.from(" first : " + TRANSFORMER + " ");
    assertEquals(SchemaTransformer.class, info.getTransformer().getClass());
  }

  @Test
  void scopesPropertiesWithoutMutatingTheInput() {
    TypedProperties props = new TypedProperties();
    props.setProperty("column", "global");
    props.setProperty("column.first", "first_column");
    props.setProperty("column.second", "second_column");
    props.setProperty("unrelated.third", "retained");
    ChainedTransformer.TransformerInfo info = ChainedTransformer.TransformerInfo.from("first:" + TRANSFORMER);
    TypedProperties scoped = info.getProperties(props, Set.of("first", "second"));
    assertEquals("first_column", scoped.getString("column"));
    assertFalse(scoped.containsKey("column.first"));
    assertFalse(scoped.containsKey("column.second"));
    assertEquals("retained", scoped.getString("unrelated.third"));
    assertEquals("global", props.getString("column"));
    assertEquals("first_column", props.getString("column.first"));
    assertEquals("second_column", props.getString("column.second"));
  }

  @Test
  void unscopedFactoryPreservesProperties() {
    TypedProperties props = new TypedProperties();
    props.setProperty("column", "global");
    props.setProperty("column.first", "first_column");
    TypedProperties actual = ChainedTransformer.TransformerInfo.from(TRANSFORMER).getProperties(props, Set.of("first"));
    assertEquals("global", actual.getString("column"));
    assertEquals("first_column", actual.getString("column.first"));
  }

  @Test
  void factoryRejectsAnEmptyClass() {
    assertThrows(HoodieTransformPlanException.class, () -> ChainedTransformer.TransformerInfo.from("first: "));
  }
}
