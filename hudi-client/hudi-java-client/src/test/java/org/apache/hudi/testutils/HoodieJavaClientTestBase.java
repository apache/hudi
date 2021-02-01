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

package org.apache.hudi.testutils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

/**
 * Base Class providing setup/cleanup and utility methods for testing Hoodie Client facing tests.
 */
public class HoodieJavaClientTestBase extends HoodieJavaClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initResources();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  // Functional Interfaces for passing lambda and Hoodie Write API contexts

  @FunctionalInterface
  public interface Function2<R, T1, T2> {

    R apply(T1 v1, T2 v2) throws IOException;
  }
}
