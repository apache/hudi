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

package org.apache.hudi.io.storage;

import org.apache.hudi.storage.HoodieLocation;
import org.apache.hudi.storage.HoodieLocationFilter;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link HoodieLocationFilter}
 */
public class TestHoodieLocationFilter {
  @Test
  public void testFilter() {
    HoodieLocation location1 = new HoodieLocation("/x/y/1");
    HoodieLocation location2 = new HoodieLocation("/x/y/2");
    HoodieLocation location3 = new HoodieLocation("/x/z/1");
    HoodieLocation location4 = new HoodieLocation("/x/z/2");

    List<HoodieLocation> locationList = Arrays.stream(
        new HoodieLocation[] {location1, location2, location3, location4}
    ).collect(Collectors.toList());

    List<HoodieLocation> expected = Arrays.stream(
        new HoodieLocation[] {location1, location2}
    ).collect(Collectors.toList());

    assertEquals(expected.stream().sorted().collect(Collectors.toList()),
        locationList.stream()
            .filter(e -> new HoodieLocationFilter() {
              @Override
              public boolean accept(HoodieLocation location) {
                return location.getParent().equals(new HoodieLocation("/x/y"));
              }
            }.accept(e))
            .sorted()
            .collect(Collectors.toList()));
    assertEquals(locationList,
        locationList.stream()
            .filter(e -> new HoodieLocationFilter() {
              @Override
              public boolean accept(HoodieLocation location) {
                return true;
              }
            }.accept(e))
            .sorted()
            .collect(Collectors.toList()));
  }
}
