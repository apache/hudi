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

package org.apache.hudi.config;

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Test;

import static org.apache.hudi.config.HoodieErrorTableConfig.ENABLE_ERROR_TABLE_WRITE_UNIFICATION;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieErrorTableConfig {

  @Test
  void testErrorAndBaseTableUnionConfig() {
    // documentation is not null
    assertNotNull(ENABLE_ERROR_TABLE_WRITE_UNIFICATION.doc());
    assertNotEquals("", ENABLE_ERROR_TABLE_WRITE_UNIFICATION.doc());

    // disabled by default
    assertFalse(ENABLE_ERROR_TABLE_WRITE_UNIFICATION.defaultValue());

    // enabled when set to true
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.errortable.write.union.enable", "true");
    assertTrue(props.getBoolean(ENABLE_ERROR_TABLE_WRITE_UNIFICATION.key(), ENABLE_ERROR_TABLE_WRITE_UNIFICATION.defaultValue()));
  }
}