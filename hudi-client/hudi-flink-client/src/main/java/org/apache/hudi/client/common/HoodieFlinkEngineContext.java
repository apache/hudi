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

package org.apache.hudi.client.common;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieList;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;

import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.util.FlinkClientUtil;

import static org.apache.hudi.common.function.FunctionWrapper.throwingFlatMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingFlatMapWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingForeachWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapToPairWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;
import static org.apache.hudi.common.function.FunctionWrapper.throwingReduceWrapper;

/**
 * A flink engine implementation of HoodieEngineContext.
 */
public class HoodieFlinkEngineContext extends HoodieEngineContext {
  public static final HoodieFlinkEngineContext DEFAULT = new HoodieFlinkEngineContext();

  private final RuntimeContext runtimeContext;

  private HoodieFlinkEngineContext() {
    this(new SerializableConfiguration(FlinkClientUtil.getHadoopConf()), new DefaultTaskContextSupplier());
  }

  public HoodieFlinkEngineContext(TaskContextSupplier taskContextSupplier) {
    this(new SerializableConfiguration(FlinkClientUtil.getHadoopConf()), taskContextSupplier);
  }

  public HoodieFlinkEngineContext(SerializableConfiguration hadoopConf, TaskContextSupplier taskContextSupplier) {
    super(hadoopConf, taskContextSupplier);
    this.runtimeContext = ((FlinkTaskContextSupplier) taskContextSupplier).getFlinkRuntimeContext();
  }

  @Override
  public HoodieAccumulator newAccumulator() {
    return HoodieAtomicLongAccumulator.create();
  }

  @Override
  public <T> HoodieData<T> emptyHoodieData() {
    return HoodieList.of(Collections.emptyList());
  }

  @Override
  public <T> HoodieData<T> parallelize(List<T> data, int parallelism) {
    return HoodieList.of(data);
  }

  public RuntimeContext getRuntimeContext() {
    return this.runtimeContext;
  }

  @Override
  public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
    return data.stream().parallel().map(throwingMapWrapper(func)).collect(Collectors.toList());
  }

  @Override
  public <I, K, V> List<V> mapToPairAndReduceByKey(List<I> data, SerializablePairFunction<I, K, V> mapToPairFunc, SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
    return data.stream().parallel().map(throwingMapToPairWrapper(mapToPairFunc))
        .collect(Collectors.groupingBy(p -> p.getKey())).values().stream()
        .map(list -> list.stream().map(e -> e.getValue()).reduce(throwingReduceWrapper(reduceFunc)).orElse(null))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public <I, K, V> Stream<ImmutablePair<K, V>> mapPartitionsToPairAndReduceByKey(
      Stream<I> data, SerializablePairFlatMapFunction<Iterator<I>, K, V> flatMapToPairFunc,
      SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
    return throwingFlatMapToPairWrapper(flatMapToPairFunc).apply(data.parallel().iterator())
        .collect(Collectors.groupingBy(Pair::getKey)).entrySet().stream()
        .map(entry -> new ImmutablePair<>(entry.getKey(), entry.getValue().stream().map(
            Pair::getValue).reduce(throwingReduceWrapper(reduceFunc)).orElse(null)))
        .filter(Objects::nonNull);
  }

  @Override
  public <I, K, V> List<V> reduceByKey(
      List<Pair<K, V>> data, SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
    return data.stream().parallel()
        .collect(Collectors.groupingBy(p -> p.getKey())).values().stream()
        .map(list -> list.stream().map(e -> e.getValue()).reduce(throwingReduceWrapper(reduceFunc)).orElse(null))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public <I, O> List<O> flatMap(List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism) {
    return data.stream().parallel().flatMap(throwingFlatMapWrapper(func)).collect(Collectors.toList());
  }

  @Override
  public <I> void foreach(List<I> data, SerializableConsumer<I> consumer, int parallelism) {
    data.forEach(throwingForeachWrapper(consumer));
  }

  @Override
  public <I, K, V> Map<K, V> mapToPair(List<I> data, SerializablePairFunction<I, K, V> func, Integer parallelism) {
    return data.stream().parallel().map(throwingMapToPairWrapper(func)).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  @Override
  public void setProperty(EngineProperty key, String value) {
    // no operation for now
  }

  @Override
  public Option<String> getProperty(EngineProperty key) {
    // no operation for now
    return Option.empty();
  }

  @Override
  public void setJobStatus(String activeModule, String activityDescription) {
    // no operation for now
  }

  /**
   * Override the flink context supplier to return constant write token.
   */
  private static class DefaultTaskContextSupplier extends FlinkTaskContextSupplier {

    public DefaultTaskContextSupplier() {
      this(null);
    }

    public DefaultTaskContextSupplier(RuntimeContext flinkRuntimeContext) {
      super(flinkRuntimeContext);
    }

    public Supplier<Integer> getPartitionIdSupplier() {
      return () -> 0;
    }

    public Supplier<Integer> getStageIdSupplier() {
      return () -> 1;
    }

    public Supplier<Long> getAttemptIdSupplier() {
      return () -> 0L;
    }

    public Option<String> getProperty(EngineProperty prop) {
      return Option.empty();
    }
  }
}
