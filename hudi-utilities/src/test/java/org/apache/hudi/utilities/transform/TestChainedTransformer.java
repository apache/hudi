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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
