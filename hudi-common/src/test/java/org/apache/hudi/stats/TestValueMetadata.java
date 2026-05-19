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

package org.apache.hudi.stats;

import org.apache.hudi.common.util.collection.Pair;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestValueMetadata {

  /**
   * Do not change how we encode decimal without
   * handling upgrade/downgrade and backwards compatibility
   */
  @Test
  public void testDecimalEncoding() {
    ValueMetadata.DecimalValueMetadata dm = ValueMetadata.DecimalMetadata.create(1,4);
    assertEquals("1,4", ValueMetadata.DecimalValueMetadata.encodeData(dm));
    dm = ValueMetadata.DecimalMetadata.create(12,4);
    assertEquals("12,4", ValueMetadata.DecimalValueMetadata.encodeData(dm));
    dm = ValueMetadata.DecimalMetadata.create(12,14);
    assertEquals("12,14", ValueMetadata.DecimalValueMetadata.encodeData(dm));
  }

  /**
   * Do not change how we encode decimal without
   * handling upgrade/downgrade and backwards compatibility
   */
  @Test
  public void testDecimalDecoding() {
    Pair<Integer, Integer> p = ValueMetadata.DecimalValueMetadata.decodeData("1,4");
    assertEquals(1, p.getLeft());
    assertEquals(4, p.getRight());
    p = ValueMetadata.DecimalValueMetadata.decodeData("12,4");
    assertEquals(12, p.getLeft());
    assertEquals(4, p.getRight());
    p = ValueMetadata.DecimalValueMetadata.decodeData("12,14");
    assertEquals(12, p.getLeft());
    assertEquals(14, p.getRight());
  }
}
