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

package org.apache.hudi.common;

import org.apache.hudi.common.metrics.ExecutorMetricsContext;
import org.apache.hudi.common.metrics.LocalRegistry;
import org.apache.hudi.common.metrics.Registry;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Characterizes how an executor reaches a driver-published registry by name. */
public class TestRegistryExecutorLookup {

  private static final String TABLE = "someTable";
  private static final String REGISTRY = "TestRegistryExecutorLookup_fs";

  /**
   * Replays what the driver does: {@code HoodieEngineContext.getMetricRegistry(tableName, registryName)} stores under the compound key, then {@code Hoodie
   */
  private static Registry publishAsDriverDoes(String tableName, String registryName) {
    Registry driverRegistry =
        Registry.getRegistryOfClass(tableName, registryName, LocalRegistry.class.getName());
    Registry.setRegistries(Collections.singletonList(driverRegistry));
    return driverRegistry;
  }

  /** The bare name an executor would naturally use does not resolve to the published registry. */
  @Test
  public void testBareNameLookupMissesPublishedRegistry() {
    Registry driverRegistry = publishAsDriverDoes(TABLE, REGISTRY);

    Registry asExecutorSeesIt = Registry.getRegistry(REGISTRY);

    assertNotSame(driverRegistry, asExecutorSeesIt,
        "an executor asking for the registry by its plain name must not silently get a different object");

    asExecutorSeesIt.increment("read");
    assertEquals(0, driverRegistry.getAllCounts(false).getOrDefault("read", 0L),
        "the increment landed somewhere the driver cannot see");
  }

  /**
   * The lookup that does resolve requires the caller to know the table name and to rebuild the prefixed name itself -- which is exactly the plumbing that
   */
  @Test
  public void testLookupResolvesOnlyWithTablePrefixedName() {
    Registry driverRegistry = publishAsDriverDoes(TABLE, REGISTRY + "_prefixed");

    Registry resolved = Registry.getRegistry(TABLE + "." + REGISTRY + "_prefixed");

    assertSame(driverRegistry, resolved,
        "the published registry is reachable, but only under the table-prefixed name");

    resolved.increment("read");
    assertEquals(1L, driverRegistry.getAllCounts(false).get("read"));
  }

  /**
   * Two tables writing in one JVM publish under distinct prefixed names, so the bare name cannot
   * disambiguate them even in principle. Whichever table an executor meant, it gets neither.
   */
  @Test
  public void testTwoTablesAreNotDistinguishableByBareName() {
    Registry tableA = publishAsDriverDoes("tableA", REGISTRY + "_multi");
    Registry tableB = publishAsDriverDoes("tableB", REGISTRY + "_multi");

    Registry asExecutorSeesIt = Registry.getRegistry(REGISTRY + "_multi");
    asExecutorSeesIt.increment("read");

    assertEquals(0, tableA.getAllCounts(false).getOrDefault("read", 0L));
    assertEquals(0, tableB.getAllCounts(false).getOrDefault("read", 0L));
  }

  /**
   * Inside a task scope the bare name resolves, which is the whole point of binding per task: the emitting code names the registry it wants and needs to k
   */
  @Test
  public void testBareNameResolvesInsideATaskBinding() {
    Registry tableRegistry = new LocalRegistry("bound");
    Map<String, Registry> previous =
        ExecutorMetricsContext.bind(Collections.singletonMap(REGISTRY, tableRegistry));
    try {
      assertSame(tableRegistry, Registry.getRegistry(REGISTRY));
      Registry.getRegistry(REGISTRY).increment("read");
    } finally {
      ExecutorMetricsContext.unbind(previous);
    }

    assertEquals(1L, tableRegistry.getAllCounts(false).get("read"));
  }

  /**
   * Two tasks for different tables, on the same thread in sequence, each reach their own registry
   * under the identical bare name. This is the case that name-keyed lookup cannot express at all.
   */
  @Test
  public void testTwoTablesAreDistinguishedByTheirTaskBinding() {
    Registry tableA = new LocalRegistry("tableA");
    Registry tableB = new LocalRegistry("tableB");

    for (Registry table : new Registry[] {tableA, tableB}) {
      Map<String, Registry> previous =
          ExecutorMetricsContext.bind(Collections.singletonMap(REGISTRY, table));
      try {
        Registry.getRegistry(REGISTRY).increment("read");
      } finally {
        ExecutorMetricsContext.unbind(previous);
      }
    }

    assertEquals(1L, tableA.getAllCounts(false).get("read"));
    assertEquals(1L, tableB.getAllCounts(false).get("read"));
  }

  /** Inside a task, a name nobody bound yields a registry that discards rather than one that collects. */
  @Test
  public void testUnboundNameInsideATaskDiscardsRatherThanCollects() {
    Map<String, Registry> previous = ExecutorMetricsContext.bind(Collections.emptyMap());
    try {
      Registry resolved = Registry.getRegistry("TestRegistryExecutorLookup_neverBound");
      resolved.increment("read");
      assertTrue(resolved.getAllCounts(false).isEmpty(),
          "a no-op registry retains nothing, so the loss is visible rather than pending");
    } finally {
      ExecutorMetricsContext.unbind(previous);
    }
  }

  /**
   * Off a task the old behaviour is untouched, so drivers and single-process engines keep collecting into a real registry.
   */
  @Test
  public void testUnbindRestoresDriverSideCreateOnMiss() {
    Map<String, Registry> previous = ExecutorMetricsContext.bind(Collections.emptyMap());
    ExecutorMetricsContext.unbind(previous);

    Registry resolved = Registry.getRegistry(REGISTRY + "_afterUnbind");
    resolved.increment("read");

    assertEquals(1L, resolved.getAllCounts(false).get("read"),
        "off a task, a miss still creates a collecting LocalRegistry");
  }
}
