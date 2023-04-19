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

  public ChainedTransformer(List<String> configuredTransformers, int... ignore) {
    this.transformerToPropKeySuffix = new HashMap<>(configuredTransformers.size());
    this.transformers = new ArrayList<>(configuredTransformers.size());

    Map<String, String> transformerClassNamesToSuffixMap = new HashMap<>(configuredTransformers.size());
    for (String configuredTransformer : configuredTransformers) {
      if (!configuredTransformer.contains(":")) {
        transformerClassNamesToSuffixMap.put(configuredTransformer, "");
      } else {
        String[] splits = configuredTransformer.split(":");
        if (splits.length > 2) {
          throw new IllegalArgumentException("There should only be one colon in a configured transformer");
        }
        transformerClassNamesToSuffixMap.put(splits[1], splits[0]);
      }
    }

    for (Map.Entry<String, String> entry : transformerClassNamesToSuffixMap.entrySet()) {
      Transformer transformer = ReflectionUtils.loadClass(entry.getKey());
      transformerToPropKeySuffix.put(transformer, entry.getValue());
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
        transformerProps = new TypedProperties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
          String key = (String) entry.getKey();
          key = key.endsWith("." + suffix) ? key.substring(0, key.length() - (suffix.length() + 1)) : key;
          transformerProps.put(key, entry.getValue());
        }
      }
      dataset = t.apply(jsc, sparkSession, dataset, transformerProps);
    }
    return dataset;
  }
}
