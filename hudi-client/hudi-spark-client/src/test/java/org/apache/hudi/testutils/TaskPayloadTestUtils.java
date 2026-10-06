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

package org.apache.hudi.testutils;

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.spark.rdd.RDD;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Inspects what Spark ships to executors for an RDD: the Java-serialized lineage, which holds the
 * task functions and everything they capture (Spark serializes it into the task binary).
 */
public class TaskPayloadTestUtils {

  /**
   * Types that task functions must not capture: each one drags a full config or a meta client into
   * every task, and a deserialized table rebuilds its views and metadata readers per task.
   */
  public static final List<Class<?>> HEAVY_TYPES = Arrays.asList(
      HoodieTable.class, HoodieWriteConfig.class, HoodieTableMetaClient.class, HoodieIndex.class, HoodieEngineContext.class);

  /**
   * Returns the classes of all objects in the Java-serialized graph of {@code root}.
   */
  public static Set<Class<?>> serializedClasses(Object root) {
    try (RecordingObjectOutputStream out = new RecordingObjectOutputStream(new ByteArrayOutputStream())) {
      out.writeObject(root);
      return out.classes;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Asserts that the lineage of {@code rdd} holds none of the {@link #HEAVY_TYPES}.
   */
  public static void assertNoHeavyTypesInLineage(RDD<?> rdd) {
    Set<String> heavy = serializedClasses(rdd).stream()
        .filter(clazz -> HEAVY_TYPES.stream().anyMatch(heavyType -> heavyType.isAssignableFrom(clazz)))
        .map(Class::getName)
        .collect(Collectors.toCollection(TreeSet::new));
    assertTrue(heavy.isEmpty(), "Task payload carries " + heavy);
  }

  private static class RecordingObjectOutputStream extends ObjectOutputStream {
    private final Set<Class<?>> classes = new HashSet<>();

    RecordingObjectOutputStream(OutputStream out) throws IOException {
      super(out);
      enableReplaceObject(true);
    }

    @Override
    protected Object replaceObject(Object obj) {
      classes.add(obj.getClass());
      return obj;
    }
  }
}
