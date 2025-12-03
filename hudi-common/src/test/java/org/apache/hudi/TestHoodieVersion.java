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

package org.apache.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class TestHoodieVersion {
  @Test
  public void testVersion() {

    HoodieVersion.setVersionOverride("0.12.2");
    assertEquals(HoodieVersion.get(), "0.12.2");
    assertEquals(HoodieVersion.major(), "0");
    assertEquals(HoodieVersion.minor(), "12");
    assertEquals(HoodieVersion.patch(), "2");
    assertEquals(HoodieVersion.majorAsInt(), 0);
    assertEquals(HoodieVersion.minorAsInt(), 12);
    assertEquals(HoodieVersion.patchAsInt(), 2);

    HoodieVersion.setVersionOverride("0.12.2-spark3");
    assertEquals(HoodieVersion.get(), "0.12.2-spark3");
    assertEquals(HoodieVersion.major(), "0");
    assertEquals(HoodieVersion.minor(), "12");
    assertEquals(HoodieVersion.patch(), "2-spark3");
    assertEquals(HoodieVersion.majorAsInt(), 0);
    assertEquals(HoodieVersion.minorAsInt(), 12);
    assertEquals(HoodieVersion.patchAsInt(), 2);

    HoodieVersion.setVersionOverride("1.2.spark3");
    assertEquals(HoodieVersion.majorAsInt(), 1);
    assertEquals(HoodieVersion.minorAsInt(), 2);
    assertEquals(HoodieVersion.patchAsInt(), 0);

    HoodieVersion.setVersionOverride("1.2.34.35");
    assertEquals(HoodieVersion.majorAsInt(), 1);
    assertEquals(HoodieVersion.minorAsInt(), 2);
    assertEquals(HoodieVersion.patchAsInt(), 34);

    HoodieVersion.setVersionOverride("1.2.34-spark-5");
    assertEquals(HoodieVersion.majorAsInt(), 1);
    assertEquals(HoodieVersion.minorAsInt(), 2);
    assertEquals(HoodieVersion.patchAsInt(), 34);

    HoodieVersion.setVersionOverride("a.b.c");
    assertEquals(HoodieVersion.majorAsInt(), 0);
    assertEquals(HoodieVersion.minorAsInt(), 0);
    assertEquals(HoodieVersion.patchAsInt(), 0);
  }
}
