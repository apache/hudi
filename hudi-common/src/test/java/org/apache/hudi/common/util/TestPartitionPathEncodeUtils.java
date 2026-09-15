/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link PartitionPathEncodeUtils}.
 */
public class TestPartitionPathEncodeUtils {

  @Test
  public void testEscapeFileNameWithDotInColumnName() {
    // Dot in column name should be escaped
    assertEquals("fare%2Ecurrency%3DUSD", PartitionPathEncodeUtils.escapeFileName("fare.currency=USD"));
  }

  @Test
  public void testEscapeFileNamePreservesDotInValue() {
    // Dot in partition value should NOT be escaped (backward compatibility)
    assertEquals("date%3D2024.01.01", PartitionPathEncodeUtils.escapeFileName("date=2024.01.01"));
    assertEquals("version%3D1.2.3", PartitionPathEncodeUtils.escapeFileName("version=1.2.3"));
  }

  @Test
  public void testEscapeFileNameWithDotInBothColumnAndValue() {
    // Dot in column name escaped, dot in value preserved
    assertEquals("col%2Ename%3Dval.ue", PartitionPathEncodeUtils.escapeFileName("col.name=val.ue"));
  }

  @Test
  public void testEscapeFileNameWithoutEquals() {
    // No '=' means no hive-style partitioning; dots are NOT escaped (no column name to protect)
    assertEquals("simple", PartitionPathEncodeUtils.escapeFileName("simple"));
    assertEquals("path.with.dots", PartitionPathEncodeUtils.escapeFileName("path.with.dots"));
  }

  @Test
  public void testEscapeFileNameNullAndEmpty() {
    assertNull(PartitionPathEncodeUtils.escapeFileName(null));
    assertEquals("", PartitionPathEncodeUtils.escapeFileName(""));
  }

  @Test
  public void testEscapeFileNameNoDotsHiveStyle() {
    // Standard hive-style without dots
    assertEquals("country%3DUS", PartitionPathEncodeUtils.escapeFileName("country=US"));
  }

  @Test
  public void testUnescapeRoundTrip() {
    String original = "fare.currency=USD";
    String escaped = PartitionPathEncodeUtils.escapeFileName(original);
    assertEquals("fare%2Ecurrency%3DUSD", escaped);
    assertEquals(original, PartitionPathEncodeUtils.unescapePathName(escaped));
  }

  @Test
  public void testUnescapeRoundTripWithDotInValue() {
    String original = "date=2024.01.01";
    String escaped = PartitionPathEncodeUtils.escapeFileName(original);
    assertEquals("date%3D2024.01.01", escaped);
    assertEquals(original, PartitionPathEncodeUtils.unescapePathName(escaped));
  }
}
