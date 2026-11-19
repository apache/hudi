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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.exception.HoodieTransformPlanException;
import org.apache.hudi.utilities.streamer.HoodieStreamer;

import lombok.AccessLevel;
import lombok.Getter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A {@link Transformer} to chain other {@link Transformer}s and apply sequentially.
 */
public class ChainedTransformer implements Transformer {

  // Delimiter used to separate class name and the property key suffix. The suffix comes first.
  private static final String ID_TRANSFORMER_CLASS_NAME_DELIMITER = ":";

  protected final List<TransformerInfo> transformers;
  private final Supplier<Option<HoodieSchema>> sourceSchemaSupplier;

  public ChainedTransformer(List<Transformer> transformersList) {
    this.transformers = new ArrayList<>(transformersList.size());
    for (Transformer transformer : transformersList) {
      this.transformers.add(new TransformerInfo(transformer));
    }
    this.sourceSchemaSupplier = Option::empty;
  }

  /**
   * Creates a chained transformer using the input transformer class names. Refer {@link HoodieStreamer.Config#transformerClassNames}
   * for more information on how the transformers can be configured.
   *
   * @param sourceSchemaSupplier              Supplies the schema (if schema provider is present) for the dataset the transform is applied to
   * @param configuredTransformers            List of configured transformer class names.
   */
  public ChainedTransformer(List<String> configuredTransformers, Supplier<Option<HoodieSchema>> sourceSchemaSupplier) {
    this.transformers = new ArrayList<>(configuredTransformers.size());
    this.sourceSchemaSupplier = sourceSchemaSupplier;

    Set<String> identifiers = new HashSet<>();
    for (String configuredTransformer : configuredTransformers) {
      Pair<Option<String>, String> name = splitTransformer(configuredTransformer);
      if (name.getLeft().isPresent()) {
        validateIdentifier(name.getLeft().get(), identifiers, configuredTransformer);
      }
      transformers.add(TransformerInfo.load(name));
    }
    if (!(transformers.stream().allMatch(TransformerInfo::hasIdentifier)
        || transformers.stream().noneMatch(TransformerInfo::hasIdentifier))) {
      throw new HoodieTransformPlanException("Either all transformers should have identifier or none should");
    }
  }

  /** Splits and trims the stock class or id:class syntax for execution and schema planning alike. */
  public static Pair<Option<String>, String> splitTransformer(String configured) {
    String[] parts = configured.trim().split(ID_TRANSFORMER_CLASS_NAME_DELIMITER, -1);
    if (parts.length > 2) {
      throw new HoodieTransformPlanException("There should only be one colon in a configured transformer");
    }
    String className = parts[parts.length - 1].trim();
    if (className.isEmpty() || (parts.length == 2 && parts[0].trim().isEmpty())) {
      throw new HoodieTransformPlanException("Transformer identifier or class is empty for " + configured);
    }
    return Pair.of(parts.length == 2 ? Option.of(parts[0].trim()) : Option.empty(), className);
  }

  public List<String> getTransformersNames() {
    return transformers.stream().map(t -> t.getTransformer().getClass().getName()).collect(Collectors.toList());
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
    Dataset<Row> dataset = rowDataset;
    for (TransformerInfo transformerInfo : transformers) {
      Transformer transformer = transformerInfo.getTransformer();
      dataset = transformer.apply(jsc, sparkSession, dataset, transformerInfo.getProperties(properties, transformers));
    }
    return dataset;
  }

  private void validateIdentifier(String id, Set<String> identifiers, String configuredTransformer) {
    if (identifiers.contains(id)) {
      throw new HoodieTransformPlanException(String.format("Duplicate identifier %s found for transformer %s", id, configuredTransformer));
    } else {
      identifiers.add(id);
    }
  }

  private StructType getExpectedTransformedSchema(TransformerInfo transformerInfo, JavaSparkContext jsc, SparkSession sparkSession,
                                                  Option<StructType> incomingStructOpt, Option<Dataset<Row>> rowDatasetOpt, TypedProperties properties) {
    Option<HoodieSchema> sourceSchemaOpt = sourceSchemaSupplier.get();
    if (!sourceSchemaOpt.isPresent() && !rowDatasetOpt.isPresent()) {
      throw new HoodieTransformPlanException("Either source schema or source dataset should be available to fetch the schema");
    }
    StructType incomingStruct = incomingStructOpt
        .orElseGet(() -> sourceSchemaOpt.isPresent() ? HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(sourceSchemaOpt.get()) : rowDatasetOpt.get().schema());
    return transformerInfo.getTransformer().transformedSchema(jsc, sparkSession, incomingStruct, properties).asNullable();
  }

  @Override
  public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
    Option<StructType> expectedTargetStructOpt = Option.ofNullable(incomingStruct);
    for (TransformerInfo transformerInfo : transformers) {
      expectedTargetStructOpt = Option.of(
          getExpectedTransformedSchema(transformerInfo, jsc, sparkSession, expectedTargetStructOpt, Option.empty(), transformerInfo.getProperties(properties, transformers)));
    }
    return expectedTargetStructOpt.get();
  }

  public static class TransformerInfo {

    @Getter(AccessLevel.PUBLIC)
    private final Transformer transformer;
    private final Option<String> idOpt;

    private TransformerInfo(Transformer transformer, String id) {
      this.transformer = transformer;
      this.idOpt = Option.of(id);
    }

    private TransformerInfo(Transformer transformer) {
      this.transformer = transformer;
      this.idOpt = Option.empty();
    }

    /** Resolves one configured transformer with the same name parsing as its chain. */
    public static TransformerInfo from(String configured) {
      return load(splitTransformer(configured));
    }

    /** Loads the transformer of an entry {@link #splitTransformer} has already parsed. */
    private static TransformerInfo load(Pair<Option<String>, String> name) {
      Transformer transformer = ReflectionUtils.loadClass(name.getRight());
      return name.getLeft().isPresent() ? new TransformerInfo(transformer, name.getLeft().get()) : new TransformerInfo(transformer);
    }

    private boolean hasIdentifier() {
      return idOpt.isPresent();
    }

    protected TypedProperties getProperties(TypedProperties properties, List<TransformerInfo> transformers) {
      Set<String> transformerIds = transformers.stream().map(transformerInfo -> transformerInfo.idOpt.orElse(null))
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
      return getProperties(properties, transformerIds);
    }

    /** The properties of this entry, excluding overrides of every other id in the chain. */
    public TypedProperties getProperties(TypedProperties properties, Set<String> transformerIds) {
      TypedProperties transformerProps = properties;
      if (idOpt.isPresent()) {
        // Transformer specific property keys end with the id associated with the transformer.
        // Ex. For id tr1, key `hoodie.streamer.transformer.sql.tr1` would be converted to
        // `hoodie.streamer.transformer.sql` and then passed to the transformer.
        String id = idOpt.get();
        transformerProps = TypedProperties.copy(properties);
        Map<String, Object> overrideKeysMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
          String key = (String) entry.getKey();
          String keyId = key.replaceAll(".*\\.", "");
          if (keyId.equals(id)) {
            overrideKeysMap.put(key.substring(0, key.length() - (id.length() + 1)), entry.getValue());
          }
          if (transformerIds.contains(keyId)) {
            transformerProps.remove(key);
          }
        }
        transformerProps.putAll(overrideKeysMap);
      }

      return transformerProps;
    }
  }
}
