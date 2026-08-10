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

package org.apache.hudi.utilities.sources.processor;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * A {@link JsonKafkaSourcePostProcessor} to chain other {@link JsonKafkaSourcePostProcessor}s and apply sequentially.
 */
public class ChainedJsonKafkaSourcePostProcessor extends JsonKafkaSourcePostProcessor {

  private final List<JsonKafkaSourcePostProcessor> processors;

  public ChainedJsonKafkaSourcePostProcessor(List<JsonKafkaSourcePostProcessor> processors, TypedProperties props) {
    super(props);
    this.processors = processors;
  }

  @Override
  public JavaRDD<String> process(JavaRDD<String> inputJsonRecords) {
    JavaRDD<String> targetRDD = inputJsonRecords;
    for (JsonKafkaSourcePostProcessor processor : processors) {
      targetRDD = processor.process(targetRDD);
    }
    return targetRDD;
  }
}
