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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.internal.HoodieSchemaException;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Planning owns a separate transformer instance, so every exit must retire its driver resources. */
public class TestUnstructuredPlanningLifecycle {
  private static final AtomicInteger CREATED = new AtomicInteger();
  private static final AtomicInteger CLOSED = new AtomicInteger();

  @BeforeEach
  void resetCounts() {
    CREATED.set(0);
    CLOSED.set(0);
  }

  @Test
  void successfulPlanningClosesItsTemporaryTransformer() throws Exception {
    plan(PlanningTransformer.class, new TypedProperties());
    assertEquals(1, CREATED.get());
    assertEquals(1, CLOSED.get());
  }

  @Test
  void refusedPlanningClosesItsTemporaryTransformer() {
    TypedProperties props = new TypedProperties();
    props.setProperty("schema.failure", "true");
    assertThrows(HoodieSchemaException.class, () -> plan(PlanningTransformer.class, props));
    assertEquals(1, CREATED.get());
    assertEquals(1, CLOSED.get());
  }

  @Test
  void planningFailureKeepsCleanupFailureSuppressed() {
    TypedProperties props = new TypedProperties();
    props.setProperty("schema.failure", "true");
    props.setProperty("close.failure", "true");
    HoodieSchemaException failure = assertThrows(HoodieSchemaException.class, () -> plan(PlanningTransformer.class, props));
    assertEquals(1, CLOSED.get());
    assertEquals(1, failure.getSuppressed().length);
    assertEquals("planning cleanup failed", failure.getSuppressed()[0].getMessage());
  }

  @Test
  void transformerWithoutCloseOverrideStillPlans() throws Exception {
    StructType schema = plan(StockTransformer.class, new TypedProperties());
    assertEquals(DataTypes.StringType, schema.fields()[0].dataType());
  }

  private static StructType plan(Class<? extends Transformer> transformer, TypedProperties props) throws Exception {
    Method method = UnstructuredSchemaContract.class.getDeclaredMethod("plan", UnstructuredMode.class, String.class,
        Set.class, StructType.class, JavaSparkContext.class, SparkSession.class, TypedProperties.class);
    method.setAccessible(true);
    StructType schema = new StructType().add("value", DataTypes.StringType, false);
    try {
      return (StructType) method.invoke(null, UnstructuredMode.BLOB, transformer.getName(), Collections.emptySet(), schema,
          null, null, props);
    } catch (InvocationTargetException failure) {
      if (failure.getCause() instanceof RuntimeException) {
        throw (RuntimeException) failure.getCause();
      }
      throw failure;
    }
  }

  public static class StockTransformer implements Transformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext context, SparkSession session, Dataset<Row> rows, TypedProperties props) {
      return rows;
    }

    @Override
    public StructType transformedSchema(JavaSparkContext context, SparkSession session, StructType schema, TypedProperties props) {
      return schema;
    }
  }

  public static class PlanningTransformer extends StockTransformer {
    private boolean failsClose;

    public PlanningTransformer() {
      CREATED.incrementAndGet();
    }

    @Override
    public StructType transformedSchema(JavaSparkContext context, SparkSession session, StructType schema, TypedProperties props) {
      failsClose = props.getBoolean("close.failure", false);
      if (props.getBoolean("schema.failure", false)) {
        throw new HoodieSchemaException("planning refused");
      }
      return schema;
    }

    @Override
    public void close() {
      CLOSED.incrementAndGet();
      if (failsClose) {
        throw new IllegalStateException("planning cleanup failed");
      }
    }
  }
}
