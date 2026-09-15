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

package org.apache.hudi.common.engine;

import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getDefaultStorage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestExecutorServiceBasedEngineContext {

  private ExecutorServiceBasedEngineContext context;

  @BeforeEach
  void setUp() {
    HoodieStorage storage = getDefaultStorage();
    context = new ExecutorServiceBasedEngineContext(storage.getConf());
  }

  @Test
  void testMapHappyPath() {
    List<Integer> result = context.map(Arrays.asList(1, 2, 3), x -> x * 2, 3);
    assertEquals(Arrays.asList(2, 4, 6), result.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  void testMapEmptyList() {
    List<Integer> result = context.map(java.util.Collections.<Integer>emptyList(), x -> x * 2, 0);
    assertTrue(result.isEmpty(), "map over empty list must return empty list");
  }

  @Test
  void testMapPreservesInputOrder() {
    List<Integer> input = IntStream.range(0, 100).boxed().collect(Collectors.toList());
    List<Integer> result = context.map(input, x -> x, 100);
    assertEquals(input, result, "map must return results in the same order as the input list");
  }

  @Test
  void testMapPropagatesRuntimeException() {
    RuntimeException original = new RuntimeException("boom");
    RuntimeException thrown = assertThrows(RuntimeException.class, () ->
        context.map(java.util.Collections.singletonList(1), x -> {
          throw original;
        }, 1));
    // throwingMapWrapper wraps ALL exceptions in HoodieException; original is the direct cause
    assertInstanceOf(HoodieException.class, thrown);
    assertSame(original, thrown.getCause());
  }

  @Test
  void testMapPropagatesHoodieException() {
    HoodieException original = new HoodieException("hoodie-boom");
    RuntimeException thrown = assertThrows(RuntimeException.class, () ->
        context.map(java.util.Collections.singletonList(1), x -> {
          throw original;
        }, 1));
    assertInstanceOf(HoodieException.class, thrown);
    assertSame(original, thrown.getCause());
  }

  @Test
  void testMapWrapsCheckedException() {
    Exception checkedCause = new Exception("checked!");
    RuntimeException thrown = assertThrows(RuntimeException.class, () ->
        context.map(java.util.Collections.singletonList(1), x -> {
          throw checkedCause;
        }, 1));
    assertInstanceOf(HoodieException.class, thrown);
    assertSame(checkedCause, thrown.getCause());
  }

  @Test
  void testWorkerThreadClassloader() {
    ClassLoader[] captured = new ClassLoader[1];
    context.map(java.util.Collections.singletonList(1), x -> {
      captured[0] = Thread.currentThread().getContextClassLoader();
      return x;
    }, 1);
    assertEquals(ExecutorServiceBasedEngineContext.class.getClassLoader(), captured[0],
        "Worker threads must use ExecutorServiceBasedEngineContext classloader to avoid ClassNotFoundException on Java 11+");
  }
}
