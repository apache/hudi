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

package org.apache.hudi.metrics;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class MetricUtils {

  // Example metric:- with_label_metric;group:a,job:0. Here `with_label_metric` is the metric name.
  // `group:a` and `job:0` are the labels for this metric.
  // Metric name and labels are separated by `;`
  private static final String METRIC_NAME_AND_LABELS_SEPARATOR = ";";
  // Multiple Labels are separated by `,`
  private static final String LABELS_SEPARATOR = ",";
  // Label key and value is separated by `:`
  private static final String LABELS_KEY_AND_VALUE_SEPARATOR = ":";

  private static Pair<String, String> splitToPair(String label) {
    String[] keyValues = label.split(LABELS_KEY_AND_VALUE_SEPARATOR, 2);
    ValidationUtils.checkArgument(StringUtils.nonEmpty(keyValues[0]), String.format("Key is empty for label %s", label));
    return  Pair.of(keyValues[0], keyValues.length == 2 ? keyValues[1] : "");
  }

  public static Pair<String,Map<String, String>> getLabelsAndMetricMap(String metric) {
    Pair<String, List<String>> labelsList = getLabelsAndMetricList(metric);
    return Pair.of(labelsList.getLeft(), getLabelsAsMap(labelsList.getValue()));
  }

  public static Pair<String,String> getMetricAndLabels(String metric) {
    String[] tokens = metric.split(METRIC_NAME_AND_LABELS_SEPARATOR);
    if (tokens.length > 2) {
      throw new RuntimeException("more than one ';' detected in metric string");
    }
    if (tokens.length == 2) {
      return  Pair.of(tokens[0], tokens[1]);
    }
    return Pair.of(tokens[0], "");
  }

  public static Map<String, String> getLabelsAsMap(String labels) {
    return getLabelsAsMap(getLabels(labels));
  }

  public static Map<String, String> getLabelsAsMap(List<String> labels) {
    return labels.stream().filter(StringUtils::nonEmpty).map(MetricUtils::splitToPair)
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight, (v1, v2) -> {
          throw new IllegalStateException(String.format("Multiple values {%s, %s} for same key", v1, v2));
        }));
  }

  public static List<String> getLabels(String labels) {
    return Arrays.stream(labels.split(LABELS_SEPARATOR)).filter(StringUtils::nonEmpty).collect(Collectors.toList());
  }

  public static Pair<String,List<String>> getLabelsAndMetricList(String metric) {
    Pair<String, String> metricAndLabels = getMetricAndLabels(metric);
    return Pair.of(metricAndLabels.getLeft(), getLabels(metricAndLabels.getRight()));
  }
}
