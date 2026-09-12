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

package org.apache.hudi.utilities.transform.embedding;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.LazyIterableIterator;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.transform.Transformer;

import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.TaskCompletionListener;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.BATCH_SIZE;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.DIMENSION;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.INPUT_MAX_CHARS;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.MAX_INFLIGHT_REQUESTS;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.PROVIDER_CLASS;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.SOURCE_COLUMN;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.TARGET_COLUMN;

/**
 * Appends a VECTOR(dimension) embedding column by calling an embedding API for the text
 * in {@code source.column}. Batching happens at the record level within each partition:
 * up to {@code batch.size} records' texts go into one API request, and the batch is
 * streamed back out row by row before the next one is pulled, so {@code batch.size}
 * bounds the rows resident per partition. Retry and backoff (in the provider) are the
 * only flow control. Rows with no text (e.g. images, videos, failed parses) receive a
 * null vector and are never sent to the API.
 *
 * <p>Because the streamer only feeds new or changed records through the transformer chain
 * each sync, embeddings stay current with the ingested data at no extra cost.
 */
public class EmbeddingTransformer implements Transformer {

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
      TypedProperties properties) {
    String sourceColumn = getStringWithAltKeys(properties, SOURCE_COLUMN, true);
    String targetColumn = getStringWithAltKeys(properties, TARGET_COLUMN, true);
    int dimension = getIntWithAltKeys(properties, DIMENSION);
    int batchSize = getIntWithAltKeys(properties, BATCH_SIZE);
    int inputMaxChars = getIntWithAltKeys(properties, INPUT_MAX_CHARS);
    int maxInflight = getIntWithAltKeys(properties, MAX_INFLIGHT_REQUESTS);
    String providerClass = getStringWithAltKeys(properties, PROVIDER_CLASS, true);

    StructType inputSchema = rowDataset.schema();
    int sourceIndex = inputSchema.fieldIndex(sourceColumn);
    StructType outputSchema = withVectorColumn(inputSchema, targetColumn, dimension);

    // Encoders.row(schema) only exists on Spark 3.5+; the adapter covers 3.3/3.4/4.x too
    Dataset<Row> withVectors = rowDataset.mapPartitions(
        (org.apache.spark.api.java.function.MapPartitionsFunction<Row, Row>) partition ->
            new EmbeddingIterator(partition, providerClass, properties, sourceIndex,
                dimension, batchSize, inputMaxChars, maxInflight),
        SparkAdapterSupport$.MODULE$.sparkAdapter().getCatalystExpressionUtils().getEncoder(outputSchema));
    // the row encoder drops StructField metadata; re-attach VECTOR(dim) so the
    // writer detects the column (and StreamSync deduces the right target schema)
    Metadata vectorMetadata = outputSchema.apply(targetColumn).metadata();
    return withVectors.withColumn(targetColumn,
        withVectors.col(targetColumn).as(targetColumn, vectorMetadata));
  }

  static StructType withVectorColumn(StructType schema, String targetColumn, int dimension) {
    Metadata vectorMetadata = new MetadataBuilder()
        .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(" + dimension + ")")
        .build();
    ArrayType vectorType = DataTypes.createArrayType(DataTypes.FloatType, false);
    return schema.add(new StructField(targetColumn, vectorType, true, vectorMetadata));
  }

  /**
   * Pulls up to {@code batch.size} input rows per batch, keeps up to
   * {@code max.inflight.requests} batches' API calls in flight on a small worker pool,
   * and streams each completed batch out row by row (releasing every buffered row as it
   * is emitted) in input order. Rows resident per partition are bounded by
   * batch.size x max.inflight.requests.
   */
  private static class EmbeddingIterator extends LazyIterableIterator<Row, Row> {

    private final String providerClass;
    private final TypedProperties props;
    private final int sourceIndex;
    private final int dimension;
    private final int batchSize;
    private final int inputMaxChars;
    private final int maxInflight;

    private EmbeddingProvider provider;
    private ExecutorService executor;
    private final ArrayDeque<PendingBatch> inflight = new ArrayDeque<>();
    private Row[] batch;
    private List<Float>[] batchVectors;
    private int batchCount;
    private int emitIndex;

    EmbeddingIterator(Iterator<Row> input, String providerClass, TypedProperties props,
        int sourceIndex, int dimension, int batchSize, int inputMaxChars, int maxInflight) {
      super(input);
      this.providerClass = providerClass;
      this.props = props;
      this.sourceIndex = sourceIndex;
      this.dimension = dimension;
      this.batchSize = batchSize;
      this.inputMaxChars = inputMaxChars;
      this.maxInflight = maxInflight;
    }

    /**
     * One batch of buffered rows whose embedding request is submitted but not yet drained.
     */
    private static class PendingBatch {
      final Row[] rows;
      final int count;
      final List<Integer> textRowIndexes;
      final Future<List<float[]>> vectors;

      PendingBatch(Row[] rows, int count, List<Integer> textRowIndexes, Future<List<float[]>> vectors) {
        this.rows = rows;
        this.count = count;
        this.textRowIndexes = textRowIndexes;
        this.vectors = vectors;
      }
    }

    @Override
    public boolean hasNext() {
      // drain buffered and in-flight batches before consulting the input; the
      // short-circuit keeps super.hasNext() (and its end() hook) from firing early
      return emitIndex < batchCount || !inflight.isEmpty() || super.hasNext();
    }

    @Override
    protected Row computeNext() {
      if (emitIndex >= batchCount) {
        promoteNextBatch();
      }
      Row row = batch[emitIndex];
      List<Float> vector = batchVectors[emitIndex];
      batch[emitIndex] = null;
      batchVectors[emitIndex] = null;
      emitIndex++;

      Object[] values = new Object[row.length() + 1];
      for (int f = 0; f < row.length(); f++) {
        values[f] = row.get(f);
      }
      // the Row encoder expects a scala Seq as the external type for array columns
      values[row.length()] = vector == null
          ? null : scala.collection.JavaConverters.asScalaBuffer(vector);
      return RowFactory.create(values);
    }

    @Override
    protected void end() {
      shutdownExecutor();
    }

    /**
     * Tops the in-flight window up, then blocks on the oldest batch's response and makes
     * it the draining batch. Batch order equals input order.
     */
    @SuppressWarnings("unchecked")
    private void promoteNextBatch() {
      submitUpToWindow();
      PendingBatch pending = inflight.poll();
      submitUpToWindow();

      List<float[]> vectors;
      try {
        vectors = pending.vectors.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HoodieException("Interrupted waiting for embeddings response", e);
      } catch (ExecutionException e) {
        throw e.getCause() instanceof HoodieException
            ? (HoodieException) e.getCause()
            : new HoodieException("Embedding request failed", e.getCause());
      }

      batch = pending.rows;
      batchCount = pending.count;
      batchVectors = new List[batchCount];
      emitIndex = 0;
      for (int i = 0; i < vectors.size(); i++) {
        float[] vector = vectors.get(i);
        if (vector.length != dimension) {
          throw new HoodieException("Embeddings API returned dimension " + vector.length
              + " but " + DIMENSION.key() + "=" + dimension);
        }
        List<Float> boxed = new ArrayList<>(vector.length);
        for (float v : vector) {
          boxed.add(v);
        }
        batchVectors[pending.textRowIndexes.get(i)] = boxed;
      }
    }

    private void submitUpToWindow() {
      while (inflight.size() < maxInflight && inputItr.hasNext()) {
        Row[] rows = new Row[batchSize];
        int count = 0;
        List<String> texts = new ArrayList<>(batchSize);
        List<Integer> textRowIndexes = new ArrayList<>(batchSize);
        while (inputItr.hasNext() && count < batchSize) {
          Row row = inputItr.next();
          rows[count] = row;
          String text = row.isNullAt(sourceIndex) ? null : row.getString(sourceIndex);
          if (text != null && !text.trim().isEmpty()) {
            texts.add(text.length() > inputMaxChars ? text.substring(0, inputMaxChars) : text);
            textRowIndexes.add(count);
          }
          count++;
        }
        Future<List<float[]>> vectors = texts.isEmpty()
            ? CompletableFuture.completedFuture(java.util.Collections.<float[]>emptyList())
            : executor().submit(() -> embed(texts));
        inflight.add(new PendingBatch(rows, count, textRowIndexes, vectors));
      }
    }

    private synchronized ExecutorService executor() {
      if (executor == null) {
        executor = Executors.newFixedThreadPool(maxInflight,
            new CustomizedThreadFactory("embedding-transformer", true));
        // end() runs only once the input drains normally. A task killed by an embeddings
        // failure would otherwise strand maxInflight threads on an executor JVM that Spark
        // goes on reusing, so release them on task completion however the task ends.
        TaskContext taskContext = TaskContext.get();
        if (taskContext != null) {
          taskContext.addTaskCompletionListener(
              (TaskCompletionListener) context -> shutdownExecutor());
        }
      }
      return executor;
    }

    private synchronized void shutdownExecutor() {
      if (executor != null) {
        executor.shutdownNow();
        executor = null;
      }
    }

    private List<float[]> embed(List<String> texts) {
      return providerInstance().embed(texts);
    }

    // called from the worker pool threads; synchronized so exactly one provider is built
    private synchronized EmbeddingProvider providerInstance() {
      if (provider == null) {
        EmbeddingProvider loaded = (EmbeddingProvider) ReflectionUtils.loadClass(providerClass);
        loaded.init(props);
        provider = loaded;
      }
      return provider;
    }
  }
}
