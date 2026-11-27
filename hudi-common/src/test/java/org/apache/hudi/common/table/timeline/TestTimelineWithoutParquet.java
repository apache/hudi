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

package org.apache.hudi.common.table.timeline;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URL;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestTimelineWithoutParquet {
  @ParameterizedTest
  @ValueSource(strings = {
      "org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineLoaderV2",
      "org.apache.hudi.core.io.storage.HoodieAvroFileReader"
  })
  void testLoadWithoutParquet(String className) throws Exception {
    // Timeline services may use common timeline classes without Parquet on their classpath.
    URL classes = HoodieTimeline.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {classes}, getClass().getClassLoader()) {
      @Override
      protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (name.startsWith("org.apache.parquet.")) {
          throw new ClassNotFoundException(name);
        }
        if (name.equals(className)) {
          Class<?> loaded = findLoadedClass(name);
          if (loaded == null) {
            loaded = findClass(name);
          }
          if (resolve) {
            resolveClass(loaded);
          }
          return loaded;
        }
        return super.loadClass(name, resolve);
      }
    }) {
      assertThrows(ClassNotFoundException.class, () -> loader.loadClass("org.apache.parquet.filter2.predicate.FilterPredicate"));
      assertDoesNotThrow(() -> {
        Class<?> loaded = Class.forName(className, true, loader);
        loaded.getDeclaredMethods();
        if (ArchivedTimelineLoader.class.isAssignableFrom(loaded)) {
          loaded.getDeclaredConstructor().newInstance();
        }
      });
    }
  }
}
