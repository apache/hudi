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

import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathFilter;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link StoragePathFilter}
 */
public class TestStoragePathFilter {
  @Test
  public void testFilter() {
    StoragePath path1 = new StoragePath("/x/y/1");
    StoragePath path2 = new StoragePath("/x/y/2");
    StoragePath path3 = new StoragePath("/x/z/1");
    StoragePath path4 = new StoragePath("/x/z/2");

    List<StoragePath> pathList = Arrays.stream(
        new StoragePath[] {path1, path2, path3, path4}
    ).collect(Collectors.toList());

    List<StoragePath> expected = Arrays.stream(
        new StoragePath[] {path1, path2}
    ).collect(Collectors.toList());

    assertEquals(expected.stream().sorted().collect(Collectors.toList()),
        pathList.stream()
            .filter(e -> new StoragePathFilter() {
              @Override
              public boolean accept(StoragePath path) {
                return path.getParent().equals(new StoragePath("/x/y"));
              }
            }.accept(e))
            .sorted()
            .collect(Collectors.toList()));
    assertEquals(pathList,
        pathList.stream()
            .filter(e -> new StoragePathFilter() {
              @Override
              public boolean accept(StoragePath path) {
                return true;
              }
            }.accept(e))
            .sorted()
            .collect(Collectors.toList()));
  }
}
