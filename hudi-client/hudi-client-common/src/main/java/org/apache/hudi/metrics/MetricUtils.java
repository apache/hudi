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

public class MetricUtils {

  private static Pair<String, String> splitToPair(String label) {
    String[] keyValues = label.split(":");
    ValidationUtils.checkArgument(StringUtils.nonEmpty(keyValues[0]), String.format("Key is empty for label %s", label));
    return  Pair.of(keyValues[0], keyValues.length == 2 ? keyValues[1] : "");
  }

  public static Pair<String,Map<String, String>> getLabelsAndMetricMap(String metric) {
    Pair<String, List<String>> labelsList = getLabelsAndMetricList(metric);
    return Pair.of(labelsList.getLeft(),labelsList.getValue().stream().filter(s -> s.contains(":")).map(MetricUtils::splitToPair)
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight))
    );
  }

  public static Pair<String,String> getMetricAndLabels(String metric) {
    String[] tokens = metric.split(";");
    if (tokens.length > 2) {
      throw new RuntimeException("more than one ';' detected in metric string");
    }
    if (tokens.length == 2) {
      return  Pair.of(tokens[0], tokens[1]);
    }
    return Pair.of(tokens[0], "");
  }

  public static Pair<String,List<String>> getLabelsAndMetricList(String metric) {
    Pair<String, String> metricAndLabels = getMetricAndLabels(metric);
    return Pair.of(metricAndLabels.getLeft(),
        Arrays.stream(metricAndLabels.getRight().split(",")).collect(Collectors.toList())
    );
  }
}
