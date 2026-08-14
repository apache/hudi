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

package org.apache.hudi.sink.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Constructor;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link CoordinationResponseSerDe}. */
class TestCoordinationResponseSerDe {

  @Test
  void testWrapAndUnwrap() {
    TestResponse expected = new TestResponse("instant-1", 3);

    TestResponse actual = CoordinationResponseSerDe.unwrap(
        CoordinationResponseSerDe.wrap(expected));

    assertEquals(expected, actual);
  }

  @Test
  void testUnwrapRejectsUnknownResponseType() {
    assertThrows(IllegalStateException.class,
        () -> CoordinationResponseSerDe.unwrap(new TestResponse("instant-1", 3)));
  }

  @Test
  void testSerializerContract() throws Exception {
    TypeSerializer<CoordinationResponse> serializer = serializer();
    TestResponse response = new TestResponse("instant-2", 5);

    assertTrue(serializer.isImmutableType());
    assertEquals(-1, serializer.getLength());
    assertNotSame(serializer, serializer.duplicate());
    assertTrue(serializer.createInstance() instanceof CoordinationResponse);
    assertEquals(serializer, serializer.duplicate());
    assertEquals(serializer.hashCode(), serializer.duplicate().hashCode());
    assertThrows(UnsupportedOperationException.class, () -> serializer.copy(response));
    assertThrows(UnsupportedOperationException.class, () -> serializer.copy(response, response));

    ByteArrayOutputStream serializedBytes = new ByteArrayOutputStream();
    serializer.serialize(response, new DataOutputViewStreamWrapper(serializedBytes));
    assertEquals(response, serializer.deserialize(
        response,
        new DataInputViewStreamWrapper(new ByteArrayInputStream(serializedBytes.toByteArray()))));

    ByteArrayOutputStream copiedBytes = new ByteArrayOutputStream();
    serializer.copy(
        new DataInputViewStreamWrapper(new ByteArrayInputStream(serializedBytes.toByteArray())),
        new DataOutputViewStreamWrapper(copiedBytes));
    assertEquals(response, serializer.deserialize(
        new DataInputViewStreamWrapper(new ByteArrayInputStream(copiedBytes.toByteArray()))));

    TypeSerializerSnapshot<CoordinationResponse> snapshot = serializer.snapshotConfiguration();
    assertEquals(serializer, snapshot.restoreSerializer());
  }

  @SuppressWarnings("unchecked")
  private static TypeSerializer<CoordinationResponse> serializer() throws Exception {
    Class<?> serializerClass = Class.forName(
        CoordinationResponseSerDe.class.getName() + "$CoordinationResponseSerializer");
    Constructor<?> constructor = serializerClass.getDeclaredConstructor();
    constructor.setAccessible(true);
    return (TypeSerializer<CoordinationResponse>) constructor.newInstance();
  }

  private static class TestResponse implements CoordinationResponse {
    private static final long serialVersionUID = 1L;

    private final String instant;
    private final int taskCount;

    private TestResponse(String instant, int taskCount) {
      this.instant = instant;
      this.taskCount = taskCount;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof TestResponse)) {
        return false;
      }
      TestResponse that = (TestResponse) obj;
      return taskCount == that.taskCount && Objects.equals(instant, that.instant);
    }

    @Override
    public int hashCode() {
      return Objects.hash(instant, taskCount);
    }
  }
}
