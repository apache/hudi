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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A {@link Transformer} to chain other {@link Transformer}s and apply sequentially.
 */
public class ChainedTransformer implements Transformer {

  // Delimiter used to separate class name and the property key suffix. The suffix comes first.
  private static final String TRANSFORMER_CLASS_NAME_KEY_SUFFIX_DELIMITER = ":";

  private final List<Transformer> transformers;
  private final Map<Transformer, String> transformerToPropKeySuffix;

  public ChainedTransformer(List<Transformer> transformers) {
    this.transformers = transformers;
    this.transformerToPropKeySuffix = new HashMap<>(transformers.size());
    for (Transformer transformer : this.transformers) {
      transformerToPropKeySuffix.put(transformer, "");
    }
  }

  /**
   * Creates a chained transformer using the input transformer class names. The name can also include
   * a suffix. This suffix can be appended with the property keys to identify properties related to the transformer.
   * E:g - tr1:org.apache.hudi.utilities.transform.SqlQueryBasedTransformer can be used along with property key
   * hoodie.deltastreamer.transformer.sql.tr1. Here tr1 is a suffix used to identify the keys specific to this transformer.
   * This suffix is removed from the configuration keys when the transformer is used. This is useful when there are two or more
   * transformers using the same config keys and expect different values for those keys.
   *
   * @param configuredTransformers List of configured transformer class names.
   * @param ignore Added for avoiding two methods with same erasure. Ignored.
   */
  public ChainedTransformer(List<String> configuredTransformers, int... ignore) {
    this.transformerToPropKeySuffix = new HashMap<>(configuredTransformers.size());
    this.transformers = new ArrayList<>(configuredTransformers.size());

    List<Pair<String, String>> transformerClassNamesToSuffixList = new ArrayList<>(configuredTransformers.size());
    for (String configuredTransformer : configuredTransformers) {
      if (!configuredTransformer.contains(":")) {
        transformerClassNamesToSuffixList.add(Pair.of(configuredTransformer, ""));
      } else {
        String[] splits = configuredTransformer.split(":");
        if (splits.length > 2) {
          throw new IllegalArgumentException("There should only be one colon in a configured transformer");
        }
        transformerClassNamesToSuffixList.add(Pair.of(splits[1], splits[0]));
      }
    }

    for (Pair<String, String> pair : transformerClassNamesToSuffixList) {
      Transformer transformer = ReflectionUtils.loadClass(pair.getKey());
      transformerToPropKeySuffix.put(transformer, pair.getValue());
      transformers.add(transformer);
    }
  }

  public List<String> getTransformersNames() {
    return transformers.stream().map(t -> t.getClass().getName()).collect(Collectors.toList());
  }

  @Override
  public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset, TypedProperties properties) {
    Dataset<Row> dataset = rowDataset;
    for (Transformer t : transformers) {
      String suffix = transformerToPropKeySuffix.get(t);
      TypedProperties transformerProps = properties;
      if (StringUtils.nonEmpty(suffix)) {
        transformerProps = new TypedProperties(properties);
        Map<String, Object> overrideKeysMap = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
          String key = (String) entry.getKey();
          if (key.endsWith("." + suffix)) {
            overrideKeysMap.put(key.substring(0, key.length() - (suffix.length() + 1)), entry.getValue());
          }
        }
        transformerProps.putAll(overrideKeysMap);
      }
      dataset = t.apply(jsc, sparkSession, dataset, transformerProps);
    }
    return dataset;
  }
}
