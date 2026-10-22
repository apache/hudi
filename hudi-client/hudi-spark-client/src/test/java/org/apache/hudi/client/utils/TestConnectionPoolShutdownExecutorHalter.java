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

package org.apache.hudi.client.utils;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieUpsertException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.client.utils.ConnectionPoolShutdownExecutorHalter.CONNECTION_POOL_SHUT_DOWN_MESSAGE;
import static org.apache.hudi.client.utils.ConnectionPoolShutdownExecutorHalter.HALT_EXIT_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestConnectionPoolShutdownExecutorHalter {

  private static IllegalStateException poolShutDown() {
    return new IllegalStateException(CONNECTION_POOL_SHUT_DOWN_MESSAGE);
  }

  /** A failure that carries {@code suppressed}, the shape a close() failing on the dead pool produces. */
  private static Throwable suppressing(Throwable failure, Throwable suppressed) {
    failure.addSuppressed(suppressed);
    return failure;
  }

  private static Stream<Arguments> failures() {
    // Shapes observed in executor traces: the pool error thrown directly from the merge handle, and nested
    // under the fallback handle's reflective constructor.
    Throwable cyclicA = new RuntimeException("a");
    Throwable cyclicB = new RuntimeException("b", cyclicA);
    cyclicA.initCause(cyclicB);
    return Stream.of(
        Arguments.of("pool shutdown thrown directly", poolShutDown(), true),
        Arguments.of("pool shutdown wrapped by the upsert task", new HoodieUpsertException("upsert", poolShutDown()), true),
        Arguments.of("pool shutdown under the fallback handle constructor",
            new HoodieUpsertException("upsert", new HoodieException("fallback",
                new HoodieException("instantiate", new InvocationTargetException(poolShutDown())))), true),
        Arguments.of("other IllegalStateException", new IllegalStateException("Connection is not open"), false),
        Arguments.of("same message on another type", new IOException(CONNECTION_POOL_SHUT_DOWN_MESSAGE), false),
        Arguments.of("unrelated failure", new HoodieUpsertException("upsert", new IOException("Read timed out")), false),
        Arguments.of("cyclic cause chain without the pool error", cyclicA, false),
        Arguments.of("pool shutdown suppressed by a failing close", suppressing(
            new HoodieUpsertException("upsert", new IOException("Read timed out")), poolShutDown()), true),
        Arguments.of("unrelated suppressed exception", suppressing(
            new HoodieUpsertException("upsert", new IOException("Read timed out")), new IOException("close failed")),
            false),
        Arguments.of("cyclic chain reached through a suppressed exception", suppressing(
            new HoodieUpsertException("upsert", new IOException("Read timed out")), cyclicA), false),
        Arguments.of("no failure at all", null, false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("failures")
  public void testHaltsOnlyOnConnectionPoolShutdown(String scenario, Throwable failure, boolean expectHalt) {
    List<Integer> haltExitCodes = new ArrayList<>();

    new ConnectionPoolShutdownExecutorHalter(haltExitCodes::add).haltIfConnectionPoolShutDown(failure);

    assertEquals(expectHalt ? Collections.singletonList(HALT_EXIT_CODE) : Collections.emptyList(), haltExitCodes);
  }

  @Test
  public void testHaltIsDisabledByDefault() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath("/tmp/test_table").build();

    assertFalse(config.shouldHaltExecutorOnConnectionPoolShutdown());
  }
}
