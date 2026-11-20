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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.prep.DummyPdfPreprocessor;

import java.nio.charset.StandardCharsets;

/** Shared single-writer round over the unstructured data source on S3 for the REST provider integration assertions. */
public abstract class RestProviderRoundTestBase extends EmbeddingRoundTestBase {

  protected static final String PREFIX = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;

  protected TypedProperties providerProps(String provider, String model, int dimensions, String endpoint) {
    TypedProperties props = props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        PREFIX + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, provider,
        PREFIX + EmbeddingTableConfig.MODEL_NAME_SUFFIX, model,
        PREFIX + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, Integer.toString(dimensions),
        PREFIX + ".endpoint", endpoint,
        PREFIX + ".retry.max.attempts", "1",
        PREFIX + ".outage.consecutive.failures", "1",
        EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "pdf" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        DummyPdfPreprocessor.class.getName(),
        EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "20",
        EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key(), "1",
        EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "1");
    props.remove(PREFIX + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    return props;
  }

  protected static FixtureFile pdf(String name) {
    return object(name + ".pdf", ("fake PDF bytes " + name).getBytes(StandardCharsets.UTF_8));
  }
}
