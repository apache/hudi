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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Property admission runs while preparing a job, including an empty or caught-up source. */
class TestUnstructuredEmbeddingStartup {
  @Test
  void refusesComputeSettingsBeforeAnEmptySourceCanStart() {
    TypedProperties props = prepOnly();
    String key = "hoodie.unstructured.embedding.compute.batch.size";
    props.setProperty(key, "0");
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> new UnstructuredSourceContract(props).prepareJob(props));
    assertTrue(failure.getMessage().contains(key), failure.getMessage());
  }

  @Test
  void validPrepOnlyJobRemainsAccepted() {
    TypedProperties props = prepOnly();
    assertDoesNotThrow(() -> new UnstructuredSourceContract(props).prepareJob(props));
  }

  private static TypedProperties prepOnly() {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "embedding");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    props.putAll(new UnstructuredSourceContract(props).tableProperties());
    return props;
  }
}
