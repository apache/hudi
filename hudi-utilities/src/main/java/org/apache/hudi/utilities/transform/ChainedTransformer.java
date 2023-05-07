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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link Transformer} to chain other {@link Transformer}s and apply sequentially.
 */
public class ChainedTransformer implements Transformer {

  // Delimiter used to separate class name and the property key suffix. The suffix comes first.
  private static final String ID_TRANSFORMER_CLASS_NAME_DELIMITER = ":";

  private final List<TransformerInfo> transformers;

  public ChainedTransformer(List<Transformer> transformersList) {
    this.transformers = new ArrayList<>(transformersList.size());
    for (Transformer transformer : transformersList) {
      this.transformers.add(new TransformerInfo(transformer));
    }
  }

  /**
   * Creates a chained transformer using the input transformer class names. Refer {@link HoodieDeltaStreamer.Config#transformerClassNames}
   * for more information on how the transformers can be configured.
   *
   * @param configuredTransformers List of configured transformer class names.
   * @param ignore Added for avoiding two methods with same erasure. Ignored.
   */
  public ChainedTransformer(List<String> configuredTransformers, int... ignore) {
    this.transformers = new ArrayList<>(configuredTransformers.size());

    Set<String> identifiers = new HashSet<>();
    for (String configuredTransformer : configuredTransformers) {
      if (!configuredTransformer.contains(ID_TRANSFORMER_CLASS_NAME_DELIMITER)) {
        transformers.add(new TransformerInfo(ReflectionUtils.loadClass(configuredTransformer)));
      } else {
        String[] splits = configuredTransformer.split(ID_TRANSFORMER_CLASS_NAME_DELIMITER);
        if (splits.length > 2) {
          throw new IllegalArgumentException("There should only be one colon in a configured transformer");
        }
        String id = splits[0];
        validateIdentifier(id, identifiers, configuredTransformer);
        Transformer transformer = ReflectionUtils.loadClass(splits[1]);
        transformers.add(new TransformerInfo(transformer, id));
      }
    }

    ValidationUtils.checkArgument(transformers.stream().allMatch(TransformerInfo::hasIdentifier)
            || transformers.stream().noneMatch(TransformerInfo::hasIdentifier),
        "Either all transformers should have identifier or none should");
  }

  public List<String> getTransformersNames() {
    return transformers.stream().map(t -> t.getTransformer().getClass().getName()).collect(Collectors.toList());
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
    Dataset<Row> dataset = rowDataset;
    for (TransformerInfo transformerInfo : transformers) {
      Transformer transformer = transformerInfo.getTransformer();
      dataset = transformer.apply(jsc, sparkSession, dataset, transformerInfo.getProperties(properties));
    }
    return dataset;
  }

  private void validateIdentifier(String id, Set<String> identifiers, String configuredTransformer) {
    ValidationUtils.checkArgument(StringUtils.nonEmpty(id), String.format("Transformer identifier is empty for %s", configuredTransformer));
    if (identifiers.contains(id)) {
      throw new IllegalArgumentException(String.format("Duplicate identifier %s found for transformer %s", id, configuredTransformer));
    } else {
      identifiers.add(id);
    }
  }

  private static class TransformerInfo {
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

    private Transformer getTransformer() {
      return transformer;
    }

    private boolean hasIdentifier() {
      return idOpt.isPresent();
    }

    private TypedProperties getProperties(TypedProperties properties) {
      TypedProperties transformerProps = properties;
      if (idOpt.isPresent()) {
        // Transformer specific property keys end with the id associated with the transformer.
        // Ex. For id tr1, key `hoodie.deltastreamer.transformer.sql.tr1` would be converted to
        // `hoodie.deltastreamer.transformer.sql` and then passed to the transformer.
        String id = idOpt.get();
        transformerProps = new TypedProperties(properties);
        Map<String, Object> overrideKeysMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
          String key = (String) entry.getKey();
          if (key.endsWith("." + id)) {
            overrideKeysMap.put(key.substring(0, key.length() - (id.length() + 1)), entry.getValue());
          }
        }
        transformerProps.putAll(overrideKeysMap);
      }

      return transformerProps;
    }
  }
}
