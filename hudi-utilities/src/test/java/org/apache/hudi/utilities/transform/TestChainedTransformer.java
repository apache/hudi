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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestChainedTransformer {
  @Test
  public void testGetTransformersNames() {
    Transformer t1 = (jsc, sparkSession, dataset, properties) -> dataset.withColumnRenamed("foo", "bar");
    Transformer t2 = (jsc, sparkSession, dataset, properties) -> dataset.withColumn("bar", dataset.col("bar").cast(IntegerType));
    ChainedTransformer transformer = new ChainedTransformer(Arrays.asList(t1, t2));
    List<String> classNames = transformer.getTransformersNames();
    assertEquals(t1.getClass().getName(), classNames.get(0));
    assertEquals(t2.getClass().getName(), classNames.get(1));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " first : "})
  void trimsNamesAndIdentifiers(String identifier) {
    ChainedTransformer chain = new ChainedTransformer(Arrays.asList(" " + identifier + SchemaTransformer.class.getName() + " "), Option::empty);
    assertEquals(Arrays.asList(SchemaTransformer.class.getName()), chain.getTransformersNames());
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "id:", " :org.apache.hudi.utilities.transform.TestChainedTransformer$SchemaTransformer", "id: "})
  void rejectsEmptyIdentifierOrClass(String configured) {
    assertThrows(HoodieTransformPlanException.class, () -> new ChainedTransformer(Arrays.asList(configured), Option::empty));
  }

  @Test
  void scopesSchemaPropertiesToEachIdentifier() {
    ChainedTransformer chain = new ChainedTransformer(Arrays.asList(" first :" + SchemaTransformer.class.getName(),
        " second :" + SchemaTransformer.class.getName()),
        () -> Option.of(HoodieSchema.parse("{\"type\":\"record\",\"name\":\"input\",\"fields\":[]}")));
    TypedProperties props = new TypedProperties();
    props.setProperty("column", "global");
    props.setProperty("column.first", "first_column");
    props.setProperty("column.second", "second_column");
    StructType schema = chain.transformedSchema(null, null, new StructType(), props);
    assertEquals(Arrays.asList("first_column", "second_column"), Arrays.asList(schema.fieldNames()));
    assertEquals("global", props.getString("column"));
    assertEquals("first_column", props.getString("column.first"));
  }

  @Test
  void unscopedSchemaPropertiesStillWork() {
    ChainedTransformer chain = new ChainedTransformer(Arrays.asList(SchemaTransformer.class.getName()),
        () -> Option.of(HoodieSchema.parse("{\"type\":\"record\",\"name\":\"input\",\"fields\":[]}")));
    TypedProperties props = new TypedProperties();
    props.setProperty("column", "global");
    assertEquals(Arrays.asList("global"), Arrays.asList(chain.transformedSchema(null, null, new StructType(), props).fieldNames()));
  }

  public static class SchemaTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession spark, Dataset<Row> dataset, TypedProperties props) {
      return dataset;
    }

    @Override
    public StructType transformedSchema(JavaSparkContext jsc, SparkSession spark, StructType schema, TypedProperties props) {
      return schema.add(props.getString("column"), DataTypes.StringType);
    }
  }

  @Test
  public void closeRetiresEveryChildAndPreservesCleanupFailures() {
    IllegalStateException first = new IllegalStateException("first cleanup");
    IllegalStateException second = new IllegalStateException("second cleanup");
    ClosingTransformer one = new ClosingTransformer(first);
    ClosingTransformer two = new ClosingTransformer(second);
    ChainedTransformer chain = new ChainedTransformer(Arrays.asList(one, two));
    IllegalStateException failure = assertThrows(IllegalStateException.class, () -> ((AutoCloseable) chain).close());
    assertSame(first, failure);
    assertTrue(one.closed);
    assertTrue(two.closed);
    assertEquals(1, failure.getSuppressed().length);
    assertSame(second, failure.getSuppressed()[0]);
  }

  private static final class ClosingTransformer implements Transformer, AutoCloseable {
    private final IllegalStateException failure;
    private boolean closed;

    private ClosingTransformer(IllegalStateException failure) {
      this.failure = failure;
    }

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rows, TypedProperties properties) {
      return rows;
    }

    @Override
    public void close() {
      closed = true;
      throw failure;
    }
  }

}
