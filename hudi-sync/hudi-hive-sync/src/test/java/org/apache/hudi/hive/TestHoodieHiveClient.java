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

package org.apache.hudi.hive;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.apache.hudi.hive.HoodieHiveClient.validateHiveVersion;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieHiveClient {

  @ParameterizedTest
  @ValueSource(strings = {"1.2.1.spark2", "2.3.0", "2.3.1", "2.3.3"})
  void testValidHiveVersions(String version) {
    assertDoesNotThrow(() -> validateHiveVersion(version));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "1.0", "1.0.0", "1.0.1", "2.0", "2.0.0", "2.1", "2.1.1", "2.2.", "2.4.0", "3.0", "3.0.0"})
  void testInvalidUnsupportedHiveVersions(String version) {
    Throwable t = assertThrows(IllegalStateException.class, () -> validateHiveVersion(version));
    assertTrue(t.getMessage().startsWith("Unsupported hive version:"));
  }
}
