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

package org.apache.hudi.common.config;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Tests {@link HoodieConfig}.
 */
public class TestHoodieConfig {
  @Test
  public void testHoodieConfig() {
    // Case 1: defaults and infer function are used
    HoodieTestFakeConfig config1 = HoodieTestFakeConfig.newBuilder().build();
    assertEquals("1", config1.getFakeString());
    assertEquals(0, config1.getFakeInteger());
    assertEquals("value3", config1.getFakeStringNoDefaultWithInfer());
    assertEquals(null, config1.getFakeStringNoDefaultWithInferEmpty());

    // Case 2: FAKE_STRING_CONFIG is set.  FAKE_INTEGER_CONFIG,
    // FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER, and
    // FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY are inferred
    HoodieTestFakeConfig config2 = HoodieTestFakeConfig.newBuilder()
        .withFakeString("value1").build();
    assertEquals("value1", config2.getFakeString());
    assertEquals(0, config2.getFakeInteger());
    assertEquals("value2", config2.getFakeStringNoDefaultWithInfer());
    assertEquals("value10", config2.getFakeStringNoDefaultWithInferEmpty());

    // Case 3: FAKE_STRING_CONFIG is set to a different value.  FAKE_INTEGER_CONFIG,
    // FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER, and
    // FAKE_STRING_CONFIG_NO_DEFAULT_WITH_INFER_EMPTY are inferred
    HoodieTestFakeConfig config3 = HoodieTestFakeConfig.newBuilder()
        .withFakeString("5").build();
    assertEquals("5", config3.getFakeString());
    assertEquals(100, config3.getFakeInteger());
    assertEquals("value3", config3.getFakeStringNoDefaultWithInfer());
    assertEquals(null, config3.getFakeStringNoDefaultWithInferEmpty());

    // Case 4: all configs are set.  No default or infer function should be used
    HoodieTestFakeConfig config4 = HoodieTestFakeConfig.newBuilder()
        .withFakeString("5")
        .withFakeInteger(200)
        .withFakeStringNoDefaultWithInfer("xyz")
        .withFakeStringNoDefaultWithInferEmpty("uvw").build();
    assertEquals("5", config4.getFakeString());
    assertEquals(200, config4.getFakeInteger());
    assertEquals("xyz", config4.getFakeStringNoDefaultWithInfer());
    assertEquals("uvw", config4.getFakeStringNoDefaultWithInferEmpty());
  }

  @Test
  void testNestedConfigsSerializedAsDifferenceFromEnclosingConfig() throws Exception {
    TypedProperties props = new TypedProperties();
    for (int i = 0; i < 100; i++) {
      props.setProperty("key" + i, "value" + i);
    }
    CompositeConfig composite = new CompositeConfig(props);
    HoodieConfig changed = composite.nested.get(0);
    changed.setValue("key1", "changed");
    changed.setValue("added", "value");
    changed.getProps().remove("key2");
    changed.getProps().put("nonStringValue", 7);
    composite.nested.get(1).getProps().clear();
    composite.nested.get(1).setValue("unrelated", "value");
    composite.setValue("enclosingOnly", "value");

    CompositeConfig copy = roundTrip(composite);
    assertEquals(composite.getProps(), copy.getProps());
    for (int i = 0; i < composite.nested.size(); i++) {
      assertEquals(composite.nested.get(i).getProps(), copy.nested.get(i).getProps());
      assertSame(composite.nested.get(i).getProps().getClass(), copy.nested.get(i).getProps().getClass());
    }
    for (int i = 0; i < composite.nested.size() - 1; i++) {
      assertNotSame(copy.getProps(), copy.nested.get(i).getProps());
    }
    assertSame(copy.getProps(), copy.nested.get(6).getProps());
    assertSame(copy.nested.get(2), copy.nested.get(3));
    copy.nested.get(2).setValue("key3", "changed");
    assertEquals("value3", copy.getString("key3"));
    assertEquals("value3", copy.nested.get(4).getString("key3"));

    // The enclosing props, their snapshot, and the two nested props that are not mostly the same as the enclosing props
    assertEquals(4, countWrittenProperties(composite));
  }

  @Test
  void testSerialVersionUidMatchesEarlierReleases() {
    // Earlier releases did not declare it, so this is the value computed for them; changing it breaks reading their configs.
    assertEquals(449277607721112139L, ObjectStreamClass.lookup(HoodieConfig.class).getSerialVersionUID());
  }

  private static int countWrittenProperties(Object object) throws IOException {
    AtomicInteger count = new AtomicInteger();
    try (ObjectOutputStream out = new ObjectOutputStream(new ByteArrayOutputStream()) {
      {
        enableReplaceObject(true);
      }

      @Override
      protected Object replaceObject(Object obj) {
        if (obj instanceof Properties) {
          count.incrementAndGet();
        }
        return obj;
      }
    }) {
      out.writeObject(object);
    }
    return count.get();
  }

  private static byte[] serialize(Object object) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(object);
    }
    return bytes.toByteArray();
  }

  @SuppressWarnings("unchecked")
  private static <T> T roundTrip(T object) throws Exception {
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(serialize(object)))) {
      return (T) in.readObject();
    }
  }

  private static class CompositeConfig extends HoodieConfig {
    private final List<HoodieConfig> nested;

    CompositeConfig(TypedProperties props) {
      super(props);
      HoodieConfig shared = new HoodieConfig(TypedProperties.copy(props));
      this.nested = Arrays.asList(new HoodieConfig(TypedProperties.copy(props)), new HoodieConfig(TypedProperties.copy(props)),
          shared, shared, new HoodieConfig(TypedProperties.copy(props)), new HoodieConfig(new SubclassedProperties(props)),
          new HoodieConfig(props));
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
      defaultWriteObjectSharingProps(out);
    }
  }

  private static class SubclassedProperties extends TypedProperties {
    SubclassedProperties(TypedProperties props) {
      super(props);
    }
  }
}
