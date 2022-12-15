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

package org.apache.hudi.common.model;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link HoodieKey}.
 */
public class TestHoodieKey {

  @Test
  public void testPartitionPathValid() {
    HoodieKey hoodieKey = new HoodieKey(UUID.randomUUID().toString(), "0000/00/00");
    assertThrows(IllegalArgumentException.class, () -> {
      hoodieKey.getPartitionPath();
      }, "should fail since partition path cannot begin with a /");
  }

}
