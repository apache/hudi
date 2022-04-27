/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.schemaevo;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hudi.HoodieSparkUtils;
import org.apache.spark.sql.hudi.TestSpark3DDL;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;

public class TestSchemaEvoBase extends TestSpark3DDL {

  @TempDir
  File tempFile;

  StreamExecutionEnvironment env;
  StreamTableEnvironment tEnv;

  @BeforeEach
  public void setUp() {
    env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    tEnv = StreamTableEnvironment.create(env);
  }

  public void checkAnswer(TableResult actualResult, String... expectedResult) {
    try (CloseableIterator<Row> iterator = actualResult.collect()) {
      Set<String> expected = new HashSet<>(Arrays.asList(expectedResult));
      Set<String> actual = new HashSet<>(expected.size());
      for (int i = 0; i < expected.size() && iterator.hasNext(); i++) {
        actual.add(iterator.next().toString());
      }
      assertEquals(expected, actual);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Retention(RetentionPolicy.RUNTIME)
  @ExtendWith(ITTestReadWithSchemaEvo.WhenSparkGreaterThan31.class)
  @Test
  public @interface TestWhenSparkGreaterThan31 {}

  public static final class WhenSparkGreaterThan31 implements ExecutionCondition {

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
      java.util.Optional<ITTestReadWithSchemaEvo.TestWhenSparkGreaterThan31> annotation = findAnnotation(context.getElement(), ITTestReadWithSchemaEvo.TestWhenSparkGreaterThan31.class);
      if (annotation.isPresent() && !HoodieSparkUtils.gteqSpark3_1()) {
        return ConditionEvaluationResult.disabled("Spark version should be greater than 3.1");
      }
      return ConditionEvaluationResult.enabled("OK");
    }
  }
}
