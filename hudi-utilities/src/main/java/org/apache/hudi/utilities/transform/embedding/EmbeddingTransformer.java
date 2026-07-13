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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.transform.Transformer;

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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.hudi.common.util.ConfigUtils.getIntWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.BATCH_SIZE;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.DIMENSION;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.INPUT_MAX_CHARS;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.PROVIDER_CLASS;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.SOURCE_COLUMN;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.TARGET_COLUMN;

/**
 * Appends a VECTOR(dimension) embedding column by calling an embedding API for the text
 * in {@code source.column}. Batching happens at the record level within each partition:
 * up to {@code batch.size} records' texts go into one API request, then the next buffer —
 * executors stay busy and large request batches keep the request rate low, with retry and
 * backoff (in the provider) as the only flow control. Rows with no text (e.g. images,
 * videos, failed parses) receive a null vector and are never sent to the API.
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
    String providerClass = getStringWithAltKeys(properties, PROVIDER_CLASS, true);

    StructType inputSchema = rowDataset.schema();
    int sourceIndex = inputSchema.fieldIndex(sourceColumn);
    StructType outputSchema = withVectorColumn(inputSchema, targetColumn, dimension);

    // Encoders.row(schema) only exists on Spark 3.5+; the adapter covers 3.3/3.4/4.x too
    Dataset<Row> withVectors = rowDataset.mapPartitions(
        (org.apache.spark.api.java.function.MapPartitionsFunction<Row, Row>) partition ->
            new EmbeddingIterator(partition, providerClass, properties, sourceIndex,
                dimension, batchSize, inputMaxChars),
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
   * Pulls up to {@code batchSize} input rows, embeds the texts in one API call, then
   * streams the augmented rows out before pulling the next buffer.
   */
  private static class EmbeddingIterator implements Iterator<Row> {

    private final Iterator<Row> input;
    private final String providerClass;
    private final TypedProperties props;
    private final int sourceIndex;
    private final int dimension;
    private final int batchSize;
    private final int inputMaxChars;

    private EmbeddingProvider provider;
    private Iterator<Row> pendingOutput = java.util.Collections.emptyIterator();

    EmbeddingIterator(Iterator<Row> input, String providerClass, TypedProperties props,
        int sourceIndex, int dimension, int batchSize, int inputMaxChars) {
      this.input = input;
      this.providerClass = providerClass;
      this.props = props;
      this.sourceIndex = sourceIndex;
      this.dimension = dimension;
      this.batchSize = batchSize;
      this.inputMaxChars = inputMaxChars;
    }

    @Override
    public boolean hasNext() {
      return pendingOutput.hasNext() || input.hasNext();
    }

    @Override
    public Row next() {
      if (!pendingOutput.hasNext()) {
        fillNextBuffer();
      }
      return pendingOutput.next();
    }

    private void fillNextBuffer() {
      List<Row> buffered = new ArrayList<>(batchSize);
      List<String> texts = new ArrayList<>(batchSize);
      List<Integer> textRowIndexes = new ArrayList<>(batchSize);
      while (input.hasNext() && buffered.size() < batchSize) {
        Row row = input.next();
        buffered.add(row);
        String text = row.isNullAt(sourceIndex) ? null : row.getString(sourceIndex);
        if (text != null && !text.trim().isEmpty()) {
          texts.add(text.length() > inputMaxChars ? text.substring(0, inputMaxChars) : text);
          textRowIndexes.add(buffered.size() - 1);
        }
      }

      List<float[]> vectors = texts.isEmpty() ? java.util.Collections.emptyList() : embed(texts);
      List<Float>[] vectorPerRow = new List[buffered.size()];
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
        vectorPerRow[textRowIndexes.get(i)] = boxed;
      }

      List<Row> output = new ArrayList<>(buffered.size());
      for (int i = 0; i < buffered.size(); i++) {
        Row row = buffered.get(i);
        Object[] values = new Object[row.length() + 1];
        for (int f = 0; f < row.length(); f++) {
          values[f] = row.get(f);
        }
        // the Row encoder expects a scala Seq as the external type for array columns
        values[row.length()] = vectorPerRow[i] == null
            ? null : scala.collection.JavaConverters.asScalaBuffer(vectorPerRow[i]);
        output.add(RowFactory.create(values));
      }
      pendingOutput = output.iterator();
    }

    private List<float[]> embed(List<String> texts) {
      if (provider == null) {
        provider = (EmbeddingProvider) ReflectionUtils.loadClass(providerClass);
        provider.init(props);
      }
      return provider.embed(texts);
    }
  }
}
