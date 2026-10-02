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

package org.apache.hudi.common.engine;

import org.apache.hudi.common.data.HoodieBroadcast;
import org.apache.hudi.storage.StorageConfiguration;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

/**
 * Tests {@link HoodieEngineContext#broadcast} for the engine contexts that run tasks in the current JVM.
 */
class TestHoodieEngineContextBroadcast {

  private static Stream<Arguments> localEngineContexts() {
    return Stream.of(
        Arguments.of(new HoodieLocalEngineContext(mock(StorageConfiguration.class))),
        Arguments.of(new ExecutorServiceBasedEngineContext(mock(StorageConfiguration.class))));
  }

  @ParameterizedTest
  @MethodSource("localEngineContexts")
  void testBroadcastValueRoundTrips(HoodieEngineContext context) throws Exception {
    List<String> value = new ArrayList<>(Arrays.asList("a", "b", "c"));
    HoodieBroadcast<List<String>> broadcast = context.broadcast(value);
    assertSame(value, broadcast.value());

    List<Integer> sizes = context.map(Arrays.asList(0, 1, 2), i -> broadcast.value().get(i).length(), 3);
    assertEquals(Arrays.asList(1, 1, 1), sizes);

    HoodieBroadcast<List<String>> copy = javaRoundTrip(broadcast);
    assertNotSame(value, copy.value());
    assertEquals(value, copy.value());
  }

  @SuppressWarnings("unchecked")
  private static <T> T javaRoundTrip(T object) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(object);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}
