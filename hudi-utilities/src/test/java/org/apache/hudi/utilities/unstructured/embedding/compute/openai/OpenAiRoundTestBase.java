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

package org.apache.hudi.utilities.unstructured.embedding.compute.openai;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.prep.DummyPdfPreprocessor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** The single-writer round over the unstructured data source on S3, with fake PDF preparation and a local OpenAI endpoint. */
public abstract class OpenAiRoundTestBase extends EmbeddingRoundTestBase {

  protected static final int DIM = EmbeddingStreamers.MODEL_DIMENSIONS;
  protected static final String KEY = "sk-test-E2EKEYZYXWVUTSRQP9876";
  protected static final String PATH = "/v1/embeddings";
  protected static final String PREFIX = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;

  @TempDir
  Path secrets;

  protected StubEmbeddingServer server;
  private Path keyFile;

  @BeforeEach
  public void startEndpoint() throws IOException {
    server = new StubEmbeddingServer();
    keyFile = secrets.resolve("fake-openai.key");
    Files.write(keyFile, (KEY + "\n").getBytes(StandardCharsets.UTF_8));
    FakeClockOpenAiClient.CLOCK.set(1_000_000L);
  }

  @AfterEach
  public void stopEndpoint() {
    server.close();
  }

  protected TypedProperties providerProps(String target, String... overrides) {
    TypedProperties props = props(target, EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        PREFIX + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "openai",
        PREFIX + EmbeddingTableConfig.MODEL_NAME_SUFFIX, "text-embedding-3-small",
        EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "pdf" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        DummyPdfPreprocessor.class.getName(),
        EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "20",
        EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "1",
        PREFIX + OpenAiClientConfig.ENDPOINT_SUFFIX, server.url() + "/v1",
        PREFIX + OpenAiClientConfig.CREDENTIAL_FILE_SUFFIX, keyFile.toString(),
        PREFIX + OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX, "1",
        PREFIX + OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX, "1");
    props.remove(PREFIX + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    for (int i = 0; i + 1 < overrides.length; i += 2) {
      props.setProperty(overrides[i], overrides[i + 1]);
    }
    return props;
  }

  protected static FixtureFile pdf(String name) {
    return object(name + ".pdf", ("fake PDF bytes " + name).getBytes(StandardCharsets.UTF_8));
  }
}
