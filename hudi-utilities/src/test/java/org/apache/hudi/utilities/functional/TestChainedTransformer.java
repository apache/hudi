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

package org.apache.hudi.utilities.functional;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.transform.ChainedTransformer;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.NESTED_SCHEMA;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("functional")
public class TestChainedTransformer extends SparkClientFunctionalTestHarness {

  @Test
  public void testChainedTransformation() {
    StructType schema = DataTypes.createStructType(
        new StructField[] {
            createStructField("foo", StringType, false)
        });
    Row r1 = RowFactory.create("100");
    Row r2 = RowFactory.create("200");
    Dataset<Row> original = spark().sqlContext().createDataFrame(Arrays.asList(r1, r2), schema);

    Transformer t1 = (jsc, sparkSession, dataset, properties) -> dataset.withColumnRenamed("foo", "bar");
    Transformer t2 = (jsc, sparkSession, dataset, properties) -> dataset.withColumn("bar", dataset.col("bar").cast(IntegerType));
    ChainedTransformer transformer = new ChainedTransformer(Arrays.asList(t1, t2));
    Dataset<Row> transformed = transformer.apply(jsc(), spark(), original, null);

    assertEquals(2, transformed.count());
    assertArrayEquals(new String[] {"bar"}, transformed.columns());
    List<Row> rows = transformed.collectAsList();
    assertEquals(100, rows.get(0).getInt(0));
    assertEquals(200, rows.get(1).getInt(0));
  }

  @ParameterizedTest
  @ValueSource(strings = {
      // empty identifier
      ":org.apache.hudi.utilities.transform.FlatteningTransformer,T2:org.apache.hudi.utilities.transform.FlatteningTransformer",
      // same identifier
      "T1:org.apache.hudi.utilities.transform.FlatteningTransformer,T1:org.apache.hudi.utilities.transform.FlatteningTransformer",
      // Two colons in transformer config
      "T1::org.apache.hudi.utilities.transform.FlatteningTransformer",
      // either all transformers have identifier or none have
      "org.apache.hudi.utilities.transform.FlatteningTransformer,T1:org.apache.hudi.utilities.transform.FlatteningTransformer"
  })
  public void testChainedTransformerValidationFails(String transformerName) {
    try {
      ChainedTransformer transformer = new ChainedTransformer(Arrays.asList(transformerName.split(",")), Option::empty);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof HoodieTransformPlanException, e.getMessage());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "T1:org.apache.hudi.utilities.transform.FlatteningTransformer,T2:org.apache.hudi.utilities.transform.FlatteningTransformer",
      "T2:org.apache.hudi.utilities.transform.FlatteningTransformer,T1:org.apache.hudi.utilities.transform.FlatteningTransformer",
      "abc:org.apache.hudi.utilities.transform.FlatteningTransformer,def:org.apache.hudi.utilities.transform.FlatteningTransformer",
      "org.apache.hudi.utilities.transform.FlatteningTransformer,org.apache.hudi.utilities.transform.FlatteningTransformer"
  })
  public void testChainedTransformerValidationPasses(String transformerName) {
    ChainedTransformer transformer = new ChainedTransformer(Arrays.asList(transformerName.split(",")), Option::empty);
    assertNotNull(transformer);
  }

  @Test
  public void testChainedTransformerTransformedSchema() {
    String transformerName = "org.apache.hudi.utilities.transform.FlatteningTransformer";
    ChainedTransformer transformer = new ChainedTransformer(Arrays.asList(transformerName.split(",")), () -> Option.of(NESTED_SCHEMA));
    StructType transformedSchema = transformer.transformedSchema(jsc(), spark(), null, new TypedProperties());
    // Verify transformed nested fields are present in the transformed schema
    assertTrue(Arrays.asList(transformedSchema.fieldNames()).contains("fare_amount"));
    assertTrue(Arrays.asList(transformedSchema.fieldNames()).contains("fare_currency"));
    assertNotNull(transformer);
  }

  @Test
  public void assertSchemaSupplierIsCalledPerInvocationOfTransformedSchema() {
    String transformerName = "org.apache.hudi.utilities.transform.FlatteningTransformer";
    AtomicInteger count = new AtomicInteger(0);
    Supplier<Option<HoodieSchema>> schemaSupplier = () -> {
      if (count.getAndIncrement() == 0) {
        return Option.of(HOODIE_SCHEMA);
      } else {
        return Option.of(NESTED_SCHEMA);
      }
    };
    ChainedTransformer transformer = new ChainedTransformer(Arrays.asList(transformerName.split(",")), schemaSupplier);
    StructType transformedSchema1 = transformer.transformedSchema(jsc(), spark(), null, new TypedProperties());
    StructType transformedSchema2 = transformer.transformedSchema(jsc(), spark(), null, new TypedProperties());
    assertNotEquals(transformedSchema1, transformedSchema2);
    assertEquals(2, count.get());
  }
}
