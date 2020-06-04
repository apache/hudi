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

package org.apache.hudi.utilities;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.transform.ChainedTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestUtilHelpers {

  public static class TransformerFoo implements Transformer {

    @Override
    public Dataset apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      return null;
    }
  }

  public static class TransformerBar implements Transformer {

    @Override
    public Dataset apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
      return null;
    }
  }

  @Nested
  public class TestCreateTransformer {

    @Test
    public void testCreateTransformerNotPresent() throws IOException {
      assertFalse(UtilHelpers.createTransformer(null).isPresent());
    }

    @Test
    public void testCreateTransformerLoadOneClass() throws IOException {
      Transformer transformer = UtilHelpers.createTransformer(Collections.singletonList(TransformerFoo.class.getName())).get();
      assertTrue(transformer instanceof ChainedTransformer);
      List<String> transformerNames = ((ChainedTransformer) transformer).getTransformersNames();
      assertEquals(1, transformerNames.size());
      assertEquals(TransformerFoo.class.getName(), transformerNames.get(0));
    }

    @Test
    public void testCreateTransformerLoadMultipleClasses() throws IOException {
      List<String> classNames = Arrays.asList(TransformerFoo.class.getName(), TransformerBar.class.getName());
      Transformer transformer = UtilHelpers.createTransformer(classNames).get();
      assertTrue(transformer instanceof ChainedTransformer);
      List<String> transformerNames = ((ChainedTransformer) transformer).getTransformersNames();
      assertEquals(2, transformerNames.size());
      assertEquals(TransformerFoo.class.getName(), transformerNames.get(0));
      assertEquals(TransformerBar.class.getName(), transformerNames.get(1));
    }

    @Test
    public void testCreateTransformerThrowsException() throws IOException {
      Exception e = assertThrows(IOException.class, () -> {
        UtilHelpers.createTransformer(Arrays.asList("foo", "bar"));
      });
      assertEquals("Could not load transformer class(es) [foo, bar]", e.getMessage());
    }
  }
}
