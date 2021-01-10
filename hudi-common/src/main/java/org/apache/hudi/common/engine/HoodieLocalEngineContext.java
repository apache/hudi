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

package org.apache.hudi.common.engine;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;

import org.apache.hudi.common.util.collection.Pair;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.function.FunctionWrapper.throwingFlatMapWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingForeachWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * A java based engine context, use this implementation on the query engine integrations if needed.
 */
public final class HoodieLocalEngineContext extends HoodieEngineContext {

  public HoodieLocalEngineContext(Configuration conf) {
    this(conf, new LocalTaskContextSupplier());
  }

  public HoodieLocalEngineContext(Configuration conf, TaskContextSupplier taskContextSupplier) {
    super(new SerializableConfiguration(conf), taskContextSupplier);
  }

  @Override
  public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
    return data.stream().parallel().map(throwingMapWrapper(func)).collect(toList());
  }

  @Override
  public <I, O> List<O> flatMap(List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism) {
    return data.stream().parallel().flatMap(throwingFlatMapWrapper(func)).collect(toList());
  }

  @Override
  public <I> void foreach(List<I> data, SerializableConsumer<I> consumer, int parallelism) {
    data.stream().forEach(throwingForeachWrapper(consumer));
  }

  @Override
  public <I, K, V> Map<K, V> mapToPair(List<I> data, SerializablePairFunction<I, K, V> func, Integer parallelism) {
    return data.stream().map(throwingMapToPairWrapper(func)).collect(
        Collectors.toMap(Pair::getLeft, Pair::getRight, (oldVal, newVal) -> newVal)
    );
  }

  @Override
  public void setProperty(EngineProperty key, String value) {
    // no operation for now
  }

  @Override
  public Option<String> getProperty(EngineProperty key) {
    return Option.empty();
  }

  @Override
  public void setJobStatus(String activeModule, String activityDescription) {
    // no operation for now
  }
}
