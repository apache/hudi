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

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Base class contains the context information needed by the engine at runtime. It will be extended by different
 * engine implementation if needed.
 */
public abstract class HoodieEngineContext {

  /**
   * A wrapped hadoop configuration which can be serialized.
   */
  private SerializableConfiguration hadoopConf;

  protected TaskContextSupplier taskContextSupplier;

  public HoodieEngineContext(SerializableConfiguration hadoopConf, TaskContextSupplier taskContextSupplier) {
    this.hadoopConf = hadoopConf;
    this.taskContextSupplier = taskContextSupplier;
  }

  public SerializableConfiguration getHadoopConf() {
    return hadoopConf;
  }

  public TaskContextSupplier getTaskContextSupplier() {
    return taskContextSupplier;
  }

  public abstract HoodieAccumulator newAccumulator();

  public abstract <T> HoodieData<T> emptyHoodieData();

  public <T> HoodieData<T> parallelize(List<T> data) {
    return parallelize(data, data.size());
  }

  public abstract <T> HoodieData<T> parallelize(List<T> data, int parallelism);

  public abstract <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism);

  public abstract <I, K, V> List<V> mapToPairAndReduceByKey(
      List<I> data, SerializablePairFunction<I, K, V> mapToPairFunc, SerializableBiFunction<V, V, V> reduceFunc, int parallelism);

  public abstract <I, K, V> Stream<ImmutablePair<K, V>> mapPartitionsToPairAndReduceByKey(
      Stream<I> data, SerializablePairFlatMapFunction<Iterator<I>, K, V> flatMapToPairFunc,
      SerializableBiFunction<V, V, V> reduceFunc, int parallelism);

  public abstract <I, K, V> List<V> reduceByKey(
      List<Pair<K, V>> data, SerializableBiFunction<V, V, V> reduceFunc, int parallelism);

  public abstract <I, O> List<O> flatMap(List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism);

  public abstract <I> void foreach(List<I> data, SerializableConsumer<I> consumer, int parallelism);

  public abstract <I, K, V> Map<K, V> mapToPair(List<I> data, SerializablePairFunction<I, K, V> func, Integer parallelism);

  public abstract void setProperty(EngineProperty key, String value);

  public abstract Option<String> getProperty(EngineProperty key);

  public abstract void setJobStatus(String activeModule, String activityDescription);

}
