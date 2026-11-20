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
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A permanent service refusal ends one iterator normally after one marker and no further source reads. */
class TestProviderRefusalMarkers {
  private static final String MESSAGE = "configured service refuses this request";
  private static final StructType SCHEMA = new StructType()
      .add(EmbeddingTableSchema.SOURCE_PATH_SHA, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.SOURCE_FILE_SIZE, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.MODALITY, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.CHUNK_ID, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.PREPARED_TEXT_CHUNK, DataTypes.StringType, true)
      .add(EmbeddingTableSchema.PREPARED_IMAGE, BlobType.dataType(), true)
      .add("emb_m1", DataTypes.createArrayType(DataTypes.FloatType, false), true)
      .add(ComputeRound.errorWorkingField());

  @BeforeEach
  void reset() {
    ServiceClient.CALLS.set(0);
    ServiceClient.CLOSES.set(0);
    ServiceClient.INITS.set(0);
    ServiceClient.refuseOnInit = false;
    ServiceClient.refuse = false;
  }

  @Test
  void refusalReturnsOneMarkerAndStopsBeforeAnotherBatch() {
    ServiceClient.refuse = true;
    ComputeCounters counters = counters();
    AtomicInteger consumed = new AtomicInteger();
    try (MicroBatchEmbeddingIterator iterator = iterator(counters, consumed)) {
      assertTrue(assertDoesNotThrow(iterator::hasNext), "the task returns normally for driver refusal handling");
      Row marker = iterator.next();
      assertNull(marker.get(7), "a refusal marker has no vector");
      assertTrue(marker.getString(8).startsWith("EMBED-COMPUTE-PROVIDER-REFUSED "), marker.toString());
      assertTrue(marker.getString(8).contains(MESSAGE), marker.toString());
      assertFalse(iterator.hasNext(), "the refused task reads no later batch");
      assertEquals(2, consumed.get());
      assertEquals(1, counters.getCallsRefused().value());
      assertEquals(1, ServiceClient.CALLS.get());
    }
    assertEquals(1, ServiceClient.CLOSES.get());
  }

  @Test
  void initializationRefusalReturnsOneMarkerWithoutAnEmbeddingRequest() {
    ServiceClient.refuseOnInit = true;
    ComputeCounters counters = counters();
    AtomicInteger consumed = new AtomicInteger();
    try (MicroBatchEmbeddingIterator iterator = assertDoesNotThrow(() -> iterator(counters, consumed))) {
      assertTrue(iterator.hasNext());
      Row marker = iterator.next();
      assertNull(marker.get(7));
      assertTrue(marker.getString(8).startsWith("EMBED-COMPUTE-PROVIDER-REFUSED "));
      assertTrue(marker.getString(8).contains(MESSAGE));
      assertFalse(iterator.hasNext());
    }
    assertEquals(1, consumed.get());
    assertEquals(1, counters.getCallsRefused().value());
    assertEquals(1, ServiceClient.INITS.get());
    assertEquals(0, ServiceClient.CALLS.get());
    assertEquals(1, ServiceClient.CLOSES.get());
  }

  @Test
  void acceptedCallsKeepAllRowsAndCloseTheClientOnce() {
    ComputeCounters counters = counters();
    AtomicInteger consumed = new AtomicInteger();
    int processed = 0;
    try (MicroBatchEmbeddingIterator iterator = iterator(counters, consumed)) {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        assertNotNull(row.get(7));
        assertNull(row.get(8));
        processed++;
      }
    }
    assertEquals(4, processed);
    assertEquals(4, consumed.get());
    assertEquals(2, ServiceClient.CALLS.get());
    assertEquals(1, ServiceClient.CLOSES.get());
    assertEquals(0, counters.getCallsRefused().value());
    assertEquals(4, counters.getRowsEmbedded().value());
  }

  @Test
  void emptyPartitionDoesNotInitializeAClient() {
    ServiceClient.refuseOnInit = true;
    ComputeCounters counters = counters();
    try (MicroBatchEmbeddingIterator iterator = iterator(counters, new AtomicInteger(), 0)) {
      assertFalse(iterator.hasNext());
    }
    assertEquals(0, ServiceClient.INITS.get());
    assertEquals(0, ServiceClient.CALLS.get());
    assertEquals(0, counters.getCallsRefused().value());
  }

  private static MicroBatchEmbeddingIterator iterator(ComputeCounters counters, AtomicInteger consumed) {
    return iterator(counters, consumed, 4);
  }

  private static MicroBatchEmbeddingIterator iterator(ComputeCounters counters, AtomicInteger consumed, int size) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "2");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "test");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX, ServiceClient.class.getName());
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1.modalities", "text");
    EmbeddingModelSpec model = EmbeddingTableConfig.from(props).activeModel();
    Iterator<Row> source = new Iterator<Row>() {
      @Override
      public boolean hasNext() {
        return consumed.get() < size;
      }

      @Override
      public Row next() {
        int chunk = consumed.getAndIncrement();
        return RowFactory.create("source", 1L, 2L, "text", (long) chunk, "chunk " + chunk, null, null, null);
      }
    };
    return new MicroBatchEmbeddingIterator(source, model, props, SCHEMA, 2, counters);
  }

  private static ComputeCounters counters() {
    return new ComputeCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
  }

  /** A local service implementation with no network or timing dependency. */
  public static class ServiceClient implements EmbeddingClient {
    private static final AtomicInteger CALLS = new AtomicInteger();
    private static final AtomicInteger CLOSES = new AtomicInteger();
    private static boolean refuse;
    private static boolean refuseOnInit;
    private static final AtomicInteger INITS = new AtomicInteger();
    private EmbeddingModelSpec model;

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      model = spec;
      INITS.incrementAndGet();
      if (refuseOnInit) {
        throw new EmbeddingClientConfigException(MESSAGE);
      }
    }

    @Override
    public EmbeddingModelSpec spec() {
      return model;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      CALLS.incrementAndGet();
      if (refuse) {
        throw new EmbeddingClientConfigException(MESSAGE);
      }
      List<EmbeddingResult> results = new ArrayList<>();
      inputs.forEach(input -> results.add(EmbeddingResult.ok(new float[] {1, 2})));
      return results;
    }

    @Override
    public void close() {
      CLOSES.incrementAndGet();
    }
  }
}
