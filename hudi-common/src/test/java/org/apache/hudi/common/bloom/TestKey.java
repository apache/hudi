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

package org.apache.hudi.common.bloom;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link Key}.
 */
public class TestKey {

  @Test
  public void testDefaultWeightIsOne() {
    Key key = new Key("abc".getBytes());
    assertEquals(1.0, key.getWeight());
  }

  @Test
  public void testConstructorWithExplicitWeight() {
    Key key = new Key("abc".getBytes(), 2.5);
    assertEquals(2.5, key.getWeight());
    assertArrayEquals("abc".getBytes(), key.getBytes());
  }

  @Test
  public void testSetWithNullValueThrows() {
    Key key = new Key();
    assertThrows(IllegalArgumentException.class, () -> key.set(null, 1.0));
  }

  @Test
  public void testIncrementWeightByAmount() {
    Key key = new Key("abc".getBytes(), 1.0);
    key.incrementWeight(2.0);
    assertEquals(3.0, key.getWeight());
  }

  @Test
  public void testIncrementWeightByOne() {
    Key key = new Key("abc".getBytes(), 1.0);
    key.incrementWeight();
    assertEquals(2.0, key.getWeight());
  }

  @Test
  public void testEqualsAndHashCodeForIdenticalKeys() {
    Key key1 = new Key("abc".getBytes(), 1.0);
    Key key2 = new Key("abc".getBytes(), 1.0);
    assertTrue(key1.equals(key2));
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  public void testEqualsIsFalseForDifferentBytes() {
    Key key1 = new Key("abc".getBytes(), 1.0);
    Key key2 = new Key("xyz".getBytes(), 1.0);
    assertFalse(key1.equals(key2));
  }

  @Test
  public void testEqualsIsFalseForDifferentWeight() {
    Key key1 = new Key("abc".getBytes(), 1.0);
    Key key2 = new Key("abc".getBytes(), 2.0);
    assertFalse(key1.equals(key2));
  }

  @Test
  public void testEqualsIsFalseForNonKeyObject() {
    Key key1 = new Key("abc".getBytes(), 1.0);
    assertFalse(key1.equals("abc"));
  }

  @Test
  public void testCompareToDifferentLength() {
    Key shortKey = new Key("ab".getBytes());
    Key longKey = new Key("abc".getBytes());
    assertTrue(shortKey.compareTo(longKey) < 0);
    assertTrue(longKey.compareTo(shortKey) > 0);
  }

  @Test
  public void testCompareToSameLengthDifferentBytes() {
    Key key1 = new Key("aac".getBytes());
    Key key2 = new Key("abc".getBytes());
    assertTrue(key1.compareTo(key2) < 0);
  }

  @Test
  public void testCompareToSameBytesDifferentWeight() {
    Key key1 = new Key("abc".getBytes(), 1.0);
    Key key2 = new Key("abc".getBytes(), 2.0);
    assertTrue(key1.compareTo(key2) < 0);
    assertTrue(key2.compareTo(key1) > 0);
  }

  @Test
  public void testCompareToEqualKeys() {
    Key key1 = new Key("abc".getBytes(), 1.0);
    Key key2 = new Key("abc".getBytes(), 1.0);
    assertEquals(0, key1.compareTo(key2));
  }

  @Test
  public void testWriteAndReadFieldsRoundTrip() throws IOException {
    Key original = new Key("hello-world".getBytes(), 3.5);

    Key deserialized = new Key();
    deserialized.readFields(BloomSerDeTestUtils.asDataInput(BloomSerDeTestUtils.serialize(original::write)));

    assertArrayEquals(original.getBytes(), deserialized.getBytes());
    assertEquals(original.getWeight(), deserialized.getWeight());
    assertEquals(original, deserialized);
  }
}
