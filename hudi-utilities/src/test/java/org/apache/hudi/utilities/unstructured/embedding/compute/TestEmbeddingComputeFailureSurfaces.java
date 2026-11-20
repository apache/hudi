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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiClientConfig;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiCompatibleEmbeddingClient;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How a compute step fails: a client that cannot start, a rejected configuration and a broken platform invariant fail
 * the step without a retry; a provider outage fails it at once; a PERMANENT result keeps the row with its prepared columns and
 * the error in the working column.
 */
class TestEmbeddingComputeFailureSurfaces {

  private static final StructType SCHEMA = new StructType()
      .add(EmbeddingTableSchema.SOURCE_PATH_SHA, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.SOURCE_FILE_SIZE, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.MODALITY, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.CHUNK_ID, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.PREPARED_TEXT_CHUNK, DataTypes.StringType, true)
      .add(EmbeddingTableSchema.preparedImageField())
      .add("emb_m1", DataTypes.createArrayType(DataTypes.FloatType, false), true)
      .add(ComputeRound.errorWorkingField());

  /** Counts its calls and fails as {@link #failure} says. */
  public static class FailingClient implements EmbeddingClient {
    static final AtomicInteger CALLS = new AtomicInteger();
    static final AtomicInteger CLOSES = new AtomicInteger();
    static volatile RuntimeException failure;
    static volatile boolean failInit;
    static volatile EmbeddingClient delegate;

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      if (failInit) {
        throw new IllegalStateException("cannot start");
      }
    }

    @Override
    public EmbeddingModelSpec spec() {
      return null;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) throws InterruptedException {
      CALLS.incrementAndGet();
      if (delegate != null) {
        return delegate.embed(inputs);
      }
      if (failure != null) {
        throw failure;
      }
      List<EmbeddingResult> results = new ArrayList<>();
      for (EmbeddingInput input : inputs) {
        results.add(EmbeddingResult.permanent(input.getChunkId() == 0 ? "bad input 0" : null));
      }
      return results;
    }

    @Override
    public void close() {
      CLOSES.incrementAndGet();
    }
  }

  private static EmbeddingModelSpec model() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "4");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1.modalities", "text");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX, FailingClient.class.getName());
    return EmbeddingTableConfig.from(props).model("m1");
  }

  private static TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.COMPUTE_MAX_ATTEMPTS.key(), "3");
    props.setProperty(EmbeddingTableConfig.COMPUTE_RETRY_BACKOFF_MS.key(), "0");
    return props;
  }

  private static ComputeCounters counters() {
    return new ComputeCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator());
  }

  private static Iterator<Row> rows() {
    return Arrays.asList(RowFactory.create("s", 101L, 17L, "text", 0L, "alpha", null, null, null),
        RowFactory.create("s", 101L, 17L, "text", 1L, "beta", null, null, null)).iterator();
  }

  private static List<Row> drain(RuntimeException failure) {
    return drain(failure, 8, counters());
  }

  private static List<Row> drain(RuntimeException failure, int batchSize, ComputeCounters counters) {
    FailingClient.CALLS.set(0);
    FailingClient.failure = failure;
    FailingClient.failInit = false;
    MicroBatchEmbeddingIterator iterator = new MicroBatchEmbeddingIterator(rows(), model(), props(), SCHEMA, batchSize, counters);
    List<Row> out = new ArrayList<>();
    iterator.forEachRemaining(out::add);
    return out;
  }

  /** The iterator reads one micro batch ahead of what it yields, no more, and closes its client once, at exhaustion. */
  @Test
  void readsOneBatchAheadAndClosesTheClientOnce() {
    FailingClient.CALLS.set(0);
    FailingClient.CLOSES.set(0);
    FailingClient.failure = null;
    FailingClient.failInit = false;
    FailingClient.delegate = null;
    Iterator<Row> source = Arrays.asList(RowFactory.create("s", 101L, 17L, "text", 0L, "alpha", null, null, null),
        RowFactory.create("s", 101L, 17L, "text", 1L, "beta", null, null, null),
        RowFactory.create("s", 101L, 17L, "text", 2L, "gamma", null, null, null)).iterator();
    AtomicInteger consumed = new AtomicInteger();
    Iterator<Row> counted = new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return source.hasNext();
      }

      @Override
      public Row next() {
        consumed.incrementAndGet();
        return source.next();
      }
    };
    MicroBatchEmbeddingIterator iterator = new MicroBatchEmbeddingIterator(counted, model(), props(), SCHEMA, 2, counters());
    assertEquals(0, consumed.get(), "nothing is read at construction");
    iterator.next();
    assertEquals(2, consumed.get(), "the first micro batch is read whole");
    iterator.next();
    assertEquals(2, consumed.get(), "the second row comes from the batch already read");
    assertEquals(0, FailingClient.CLOSES.get(), "the client stays open while rows remain");
    iterator.next();
    assertEquals(3, consumed.get(), "the last micro batch is read when the first is exhausted");
    assertFalse(iterator.hasNext());
    assertEquals(1, FailingClient.CLOSES.get(), "exhaustion closes the client");
    iterator.close();
    assertEquals(1, FailingClient.CLOSES.get(), "an explicit close after exhaustion closes it no second time");
    assertEquals(2, FailingClient.CALLS.get(), "one provider call per micro batch");
  }

  @Test
  void clientThatCannotStartFailsTheStep() {
    FailingClient.CLOSES.set(0);
    FailingClient.failInit = true;
    Iterator<Row> rows = rows();
    EmbeddingModelSpec model = model();
    TypedProperties props = props();
    ComputeCounters counters = counters();
    EmbeddingComputeTransientException failure = assertThrows(EmbeddingComputeTransientException.class,
        () -> new MicroBatchEmbeddingIterator(rows, model, props, SCHEMA, 8, counters));
    FailingClient.failInit = false;
    assertEquals(1, FailingClient.CLOSES.get(), "a client whose init fails is closed");
    assertTrue(failure.getMessage().contains("EMBED-COMPUTE-CLIENT-INIT-FAILED"), failure.getMessage());
  }

  @Test
  public void identicalLocalInputChecksRemainRowErrors(@TempDir Path temp) throws Exception {
    Path credential = temp.resolve("local-validation.key");
    Files.write(credential, "local-test-value".getBytes(StandardCharsets.UTF_8));
    TypedProperties properties = props();
    String prefix = EmbeddingTableConfig.MODEL_PREFIX + "m1";
    properties.setProperty(prefix + OpenAiClientConfig.ENDPOINT_SUFFIX, "http://127.0.0.1:1/v1");
    properties.setProperty(prefix + OpenAiClientConfig.CREDENTIAL_FILE_SUFFIX, credential.toString());
    EmbeddingModelSpec model = new EmbeddingModelSpec("m1", "emb_m1", 4, OpenAiCompatibleEmbeddingClient.class.getName(),
        "openai", "text-embedding-3-small");
    List<Row> input = Arrays.asList(
        RowFactory.create("first", 101L, 17L, "text", 0L, "", null, null, null),
        RowFactory.create("second", 101L, 17L, "text", 0L, "", null, null, null));
    ComputeCounters counts = counters();
    List<Row> output = new ArrayList<>();
    try (MicroBatchEmbeddingIterator iterator = new MicroBatchEmbeddingIterator(input.iterator(), model, properties, SCHEMA, 8, counts)) {
      iterator.forEachRemaining(output::add);
    }
    assertEquals(2, output.size());
    for (int i = 0; i < input.size(); i++) {
      for (int column = 0; column < 6; column++) {
        assertEquals(input.get(i).get(column), output.get(i).get(column));
      }
      assertNull(output.get(i).get(7));
      assertTrue(output.get(i).getString(8).contains("empty"));
      assertFalse(output.get(i).getString(8).startsWith(ComputeRound.REFUSED_PREFIX));
    }
    assertEquals(2, counts.getRowsPermanent().value());
    assertEquals(0, counts.getCallsRefused().value());
  }

  @Test
  void permanentRefusalEndsTheTaskAfterOneRequestWithoutAThrow() {
    // batches of one: the second row would be a second request, but it is never read
    ComputeCounters counters = counters();
    List<Row> out = drain(new EmbeddingClientConfigException("HTTP 429 insufficient_quota: You exceeded your current quota"), 1,
        counters);

    assertEquals(1, FailingClient.CALLS.get(), "one request, then none");
    assertEquals(1, out.size(), "one marker row, the first row of the refused batch");
    Row marker = out.get(0);
    assertEquals("s", marker.getString(0));
    assertEquals(0L, marker.getLong(4));
    assertNull(marker.get(7), "no vector");
    assertEquals(ComputeRound.REFUSED_PREFIX + "HTTP 429 insufficient_quota: You exceeded your current quota", marker.getString(8));
    assertEquals(1, counters.getCallsRefused().value());
    assertEquals(0, counters.getRowsPermanent().value());
    assertEquals(0, counters.getRowsEmbedded().value());
  }

  @Test
  void transientRateLimitAnswerIsStillRetried() {
    // a 429 rate_limit_exceeded the client could not absorb comes back RETRYABLE: retried alone, then embedded
    AtomicInteger calls = new AtomicInteger();
    FailingClient.delegate = new DummyEmbeddingClient() {
      @Override
      public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
        if (calls.getAndIncrement() == 0) {
          return Collections.nCopies(inputs.size(), EmbeddingResult.retryable("HTTP 429 rate_limit_exceeded"));
        }
        return super.embed(inputs);
      }
    };
    try {
      FailingClient.delegate.init(model(), new TypedProperties());
      ComputeCounters counters = counters();
      List<Row> out = drain(null, 8, counters);

      assertEquals(3, FailingClient.CALLS.get(), "the batch, then each of its two inputs alone");
      assertEquals(2, out.size());
      out.forEach(row -> assertNotNull(row.get(7), row.toString()));
      out.forEach(row -> assertNull(row.get(8), row.toString()));
      assertEquals(0, counters.getCallsRefused().value());
      assertEquals(2, counters.getRowsRetried().value());
    } finally {
      FailingClient.delegate = null;
    }
  }

  @Test
  void transientFailureStillFailsTheTaskForASparkRetry() {
    // an outage ends the step with a transient failure, as before: Spark re-attempts the task
    EmbeddingProviderUnavailableException outage = new EmbeddingProviderUnavailableException("outage open");
    EmbeddingComputeTransientException failure = assertThrows(EmbeddingComputeTransientException.class, () -> drain(outage));
    assertTrue(failure.getMessage().contains("EMBED-COMPUTE-PROVIDER-UNAVAILABLE"), failure.getMessage());
    assertEquals(1, FailingClient.CALLS.get());
  }

  @Test
  void brokenPlatformInvariantIsNotRetried() {
    EmbeddingComputePlatformException broken = new EmbeddingComputePlatformException("broken");
    assertSame(broken, assertThrows(EmbeddingComputePlatformException.class, () -> drain(broken)));
    assertEquals(1, FailingClient.CALLS.get());
  }

  @Test
  void providerOutageFailsTheStepAtOnce() {
    EmbeddingProviderUnavailableException down = new EmbeddingProviderUnavailableException("down");
    EmbeddingComputeTransientException failure = assertThrows(EmbeddingComputeTransientException.class, () -> drain(down));
    assertTrue(failure.getMessage().contains("EMBED-COMPUTE-PROVIDER-UNAVAILABLE"), failure.getMessage());
    assertEquals(1, FailingClient.CALLS.get());
  }

  /** A client whose every call is interrupted. */
  private static final class InterruptedClient implements EmbeddingClient {
    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      // nothing to set up: the client only fails
    }

    @Override
    public EmbeddingModelSpec spec() {
      return null;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) throws InterruptedException {
      throw new InterruptedException("stopped");
    }
  }

  /** An interrupted call ends the step with a transient failure at once, never a retry, and keeps the interrupt flag. */
  @Test
  void interruptedCallEndsTheStepAndKeepsTheInterruptFlag() {
    FailingClient.delegate = new InterruptedClient();
    try {
      EmbeddingComputeTransientException failure = assertThrows(EmbeddingComputeTransientException.class, () -> drain(null));
      assertTrue(Thread.interrupted(), "the interrupt flag is restored");
      assertTrue(failure.getMessage().contains("was interrupted"), failure.getMessage());
      assertTrue(failure.getCause() instanceof InterruptedException, String.valueOf(failure.getCause()));
      assertEquals(1, FailingClient.CALLS.get());
    } finally {
      Thread.interrupted();
      FailingClient.delegate = null;
    }
  }

  @Test
  void sourceVersionsSharingAChunkReachOneProviderRequest(@TempDir Path temp) throws Exception {
    Path credential = temp.resolve("openai.key");
    Files.write(credential, "fake-version-key".getBytes(StandardCharsets.UTF_8));
    ObjectMapper mapper = new ObjectMapper();
    try (StubHttpTransport<StubHttpTransport.Exchange> server = new StubHttpTransport<>(exchange -> exchange, request -> {
      try {
        JsonNode texts = mapper.readTree(request.body).get("input");
        ArrayNode data = mapper.createArrayNode();
        for (int i = 0; i < texts.size(); i++) {
          int offset = 4 * Arrays.asList("alpha", "beta", "gamma").indexOf(texts.get(i).asText());
          data.addObject().put("index", i).set("embedding", mapper.valueToTree(new int[] {offset + 1, offset + 2, offset + 3, offset + 4}));
        }
        return new StubHttpTransport.Response(200, Collections.emptyMap(),
            mapper.writeValueAsBytes(mapper.createObjectNode().put("model", "text-embedding-3-small").set("data", data)));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    })) {
      TypedProperties providerProps = props();
      String prefix = EmbeddingTableConfig.MODEL_PREFIX + "m1";
      providerProps.setProperty(prefix + OpenAiClientConfig.ENDPOINT_SUFFIX, server.url() + "/v1");
      providerProps.setProperty(prefix + OpenAiClientConfig.CREDENTIAL_FILE_SUFFIX, credential.toString());
      EmbeddingClient provider = new OpenAiCompatibleEmbeddingClient();
      provider.init(new EmbeddingModelSpec("m1", "emb_m1", 4, null, "openai", "text-embedding-3-small"), providerProps);
      List<Row> prepared = Arrays.asList(
          RowFactory.create("s", 101L, 17L, "text", 0L, "alpha", null, null, null),
          RowFactory.create("s", 102L, 17L, "text", 0L, "beta", null, null, null),
          RowFactory.create("s", 101L, 18L, "text", 0L, "gamma", null, null, null));
      FailingClient.failure = null;
      FailingClient.failInit = false;
      FailingClient.delegate = provider;
      List<Row> out = new ArrayList<>();
      try (MicroBatchEmbeddingIterator iterator = new MicroBatchEmbeddingIterator(prepared.iterator(), model(), props(), SCHEMA, 8, counters())) {
        iterator.forEachRemaining(out::add);
      } finally {
        FailingClient.delegate = null;
        provider.close();
      }
      assertEquals(3, out.size());
      for (int i = 0; i < prepared.size(); i++) {
        for (int column = 0; column < 6; column++) {
          assertEquals(prepared.get(i).get(column), out.get(i).get(column));
        }
        assertArrayEquals(new float[] {1 + 4 * i, 2 + 4 * i, 3 + 4 * i, 4 + 4 * i}, (float[]) out.get(i).get(7));
        assertNull(out.get(i).get(8));
      }
      assertEquals(1, server.requests().size());
      assertEquals("/v1/embeddings", server.requests().get(0).path);
      JsonNode sent = new ObjectMapper().readTree(server.requests().get(0).body);
      assertEquals(new ObjectMapper().readTree("[\"alpha\",\"beta\",\"gamma\"]"), sent.get("input"));
    }
  }

  @Test
  void exhaustedRetriesReportTheFullSourceVersionAndChunk() {
    IllegalStateException temporary = new IllegalStateException("temporary failure");
    EmbeddingComputeTransientException failure = assertThrows(EmbeddingComputeTransientException.class, () -> drain(temporary));
    String message = failure.getMessage();
    assertTrue(message.contains("source_path_sha=s"), message);
    assertTrue(message.contains("source_modification_time=101"), message);
    assertTrue(message.contains("source_file_size=17"), message);
    assertTrue(message.contains("modality=text"), message);
    assertTrue(message.contains("chunk_id=0"), message);
  }

  @Test
  void permanentResultKeepsTheRowWithItsPreparedColumnsAndTheError() {
    List<Row> out = drain(null);
    assertEquals(Arrays.asList(
        RowFactory.create("s", 101L, 17L, "text", 0L, "alpha", null, null, "bad input 0"),
        RowFactory.create("s", 101L, 17L, "text", 1L, "beta", null, null, "PERMANENT result without a message")), out,
        "prepared columns survive and every PERMANENT result carries its error in the working column");
  }
}
