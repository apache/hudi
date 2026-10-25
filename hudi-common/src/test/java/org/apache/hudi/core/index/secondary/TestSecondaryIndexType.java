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

package org.apache.hudi.core.index.secondary;

import org.apache.hudi.exception.HoodieIndexException;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests {@link SecondaryIndexType}.
 */
public class TestSecondaryIndexType {

  @Test
  public void testGetValueReturnsByteCode() {
    assertEquals((byte) 1, SecondaryIndexType.LUCENE.getValue());
  }

  @Test
  public void testOfByteReturnsMatchingType() {
    assertEquals(SecondaryIndexType.LUCENE, SecondaryIndexType.of((byte) 1));
  }

  @Test
  public void testOfByteThrowsForUnknownType() {
    HoodieIndexException e = assertThrows(HoodieIndexException.class, () -> SecondaryIndexType.of((byte) 99));
    assertEquals("Unknown hoodie index type:99", e.getMessage());
  }

  @Test
  public void testOfStringReturnsMatchingTypeCaseInsensitively() {
    assertEquals(SecondaryIndexType.LUCENE, SecondaryIndexType.of("lucene"));
    assertEquals(SecondaryIndexType.LUCENE, SecondaryIndexType.of("LUCENE"));
    assertEquals(SecondaryIndexType.LUCENE, SecondaryIndexType.of("Lucene"));
  }

  @Test
  public void testOfStringThrowsForUnknownType() {
    HoodieIndexException e = assertThrows(HoodieIndexException.class, () -> SecondaryIndexType.of("bloom"));
    assertEquals("Unknown hoodie index type:bloom", e.getMessage());
  }
}
