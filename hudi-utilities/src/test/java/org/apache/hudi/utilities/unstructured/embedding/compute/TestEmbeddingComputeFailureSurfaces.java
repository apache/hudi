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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How a compute step fails: a client that cannot start, a rejected configuration and a broken platform invariant fail
 * the step without a retry; a provider outage fails it at once; a PERMANENT result keeps the row with its prepared columns and
 * the error in the working column.
 */
public class TestEmbeddingComputeFailureSurfaces {

  private static final StructType SCHEMA = new StructType()
      .add(EmbeddingTableSchema.SOURCE_PATH_SHA, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.SOURCE_FILE_SIZE, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.MODALITY, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.CHUNK_ID, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.PREPARED_TEXT_CHUNK, DataTypes.StringType, true)
      .add("emb_m1", DataTypes.createArrayType(DataTypes.FloatType, false), true)
      .add(ComputeRound.errorWorkingField());

  /** Counts its calls and fails as {@link #failure} says. */
  public static class FailingClient implements EmbeddingClient {
    static final AtomicInteger CALLS = new AtomicInteger();
    static final AtomicInteger CLOSES = new AtomicInteger();
    static volatile RuntimeException failure;
    static volatile boolean failInit;

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
    return Arrays.asList(RowFactory.create("s", 101L, 17L, "text", 0L, "alpha", null, null),
        RowFactory.create("s", 101L, 17L, "text", 1L, "beta", null, null)).iterator();
  }

  private static List<Row> drain(RuntimeException failure) {
    FailingClient.CALLS.set(0);
    FailingClient.failure = failure;
    FailingClient.failInit = false;
    MicroBatchEmbeddingIterator iterator = new MicroBatchEmbeddingIterator(rows(), model(), props(), SCHEMA, 8, counters());
    List<Row> out = new ArrayList<>();
    iterator.forEachRemaining(out::add);
    return out;
  }

  /** The iterator reads one micro batch ahead of what it yields, no more, and closes its client once, at exhaustion. */
  @Test
  public void readsOneBatchAheadAndClosesTheClientOnce() {
    FailingClient.CALLS.set(0);
    FailingClient.CLOSES.set(0);
    FailingClient.failure = null;
    FailingClient.failInit = false;
    Iterator<Row> source = Arrays.asList(RowFactory.create("s", 101L, 17L, "text", 0L, "alpha", null, null),
        RowFactory.create("s", 101L, 17L, "text", 1L, "beta", null, null),
        RowFactory.create("s", 101L, 17L, "text", 2L, "gamma", null, null)).iterator();
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
  public void clientThatCannotStartFailsTheStep() {
    FailingClient.failInit = true;
    EmbeddingComputeTransientException failure = assertThrows(EmbeddingComputeTransientException.class,
        () -> new MicroBatchEmbeddingIterator(rows(), model(), props(), SCHEMA, 8, counters()));
    FailingClient.failInit = false;
    assertTrue(failure.getMessage().contains("EMBED-COMPUTE-CLIENT-INIT-FAILED"), failure.getMessage());
  }

  @Test
  public void rejectedConfigurationIsNotRetried() {
    EmbeddingClientConfigException rejected = new EmbeddingClientConfigException("rejected");
    assertSame(rejected, assertThrows(EmbeddingClientConfigException.class, () -> drain(rejected)));
    assertEquals(1, FailingClient.CALLS.get());
  }

  @Test
  public void brokenPlatformInvariantIsNotRetried() {
    EmbeddingComputePlatformException broken = new EmbeddingComputePlatformException("broken");
    assertSame(broken, assertThrows(EmbeddingComputePlatformException.class, () -> drain(broken)));
    assertEquals(1, FailingClient.CALLS.get());
  }

  @Test
  public void providerOutageFailsTheStepAtOnce() {
    EmbeddingComputeTransientException failure = assertThrows(EmbeddingComputeTransientException.class,
        () -> drain(new EmbeddingProviderUnavailableException("down")));
    assertTrue(failure.getMessage().contains("EMBED-COMPUTE-PROVIDER-UNAVAILABLE"), failure.getMessage());
    assertEquals(1, FailingClient.CALLS.get());
  }

  @Test
  public void exhaustedRetriesReportTheFullSourceVersionAndChunk() {
    EmbeddingComputeTransientException failure = assertThrows(EmbeddingComputeTransientException.class,
        () -> drain(new IllegalStateException("temporary failure")));
    String message = failure.getMessage();
    assertTrue(message.contains("source_path_sha=s"), message);
    assertTrue(message.contains("source_modification_time=101"), message);
    assertTrue(message.contains("source_file_size=17"), message);
    assertTrue(message.contains("modality=text"), message);
    assertTrue(message.contains("chunk_id=0"), message);
  }

  @Test
  public void permanentResultKeepsTheRowWithItsPreparedColumnsAndTheError() {
    List<Row> out = drain(null);
    assertEquals(Arrays.asList(
        RowFactory.create("s", 101L, 17L, "text", 0L, "alpha", null, "bad input 0"),
        RowFactory.create("s", 101L, 17L, "text", 1L, "beta", null, "PERMANENT result without a message")), out,
        "prepared columns survive and every PERMANENT result carries its error in the working column");
  }
}
