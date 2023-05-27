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

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.utilities.deltastreamer.ConfigurationHotUpdateStrategyUtils.mergeProperties;

public class TestConfigurationHotUpdateStrategyUtils {

  @Test
  public void testMergeProperties() {
    TypedProperties defaultProps = new TypedProperties();
    defaultProps.put("a", "A");
    defaultProps.put("b", "B");

    TypedProperties propsToUpdate = new TypedProperties(defaultProps);
    TypedProperties propsNotChange = new TypedProperties(defaultProps);
    Assertions.assertFalse(mergeProperties(propsToUpdate, propsNotChange));
    Assertions.assertEquals(propsToUpdate, propsNotChange);

    propsToUpdate = new TypedProperties(defaultProps);
    TypedProperties propsNew = new TypedProperties(defaultProps);
    propsNew.put("c", "C");
    Assertions.assertTrue(mergeProperties(propsToUpdate, propsNew));
    Assertions.assertEquals(propsToUpdate, propsNew);

    propsToUpdate = new TypedProperties(defaultProps);
    TypedProperties propsUpdated = new TypedProperties(defaultProps);
    propsUpdated.put("a", "AA");
    Assertions.assertTrue(mergeProperties(propsToUpdate, propsUpdated));
    Assertions.assertEquals(propsToUpdate, propsUpdated);

    propsToUpdate = new TypedProperties(defaultProps);
    TypedProperties propsDeleted = new TypedProperties(defaultProps);
    propsDeleted.remove("b");
    Assertions.assertTrue(mergeProperties(propsToUpdate, propsDeleted));
    Assertions.assertEquals(propsToUpdate, propsDeleted);
  }
}
