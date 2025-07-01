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

import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Base class contains the context information needed by the engine at runtime. It will be extended by different
 * engine implementation if needed.
 */
public abstract class HoodieEngineContext {

  /**
   * A wrapped hadoop configuration which can be serialized.
   */
  private final StorageConfiguration<?> storageConf;

  protected TaskContextSupplier taskContextSupplier;

  public HoodieEngineContext(StorageConfiguration<?> storageConf, TaskContextSupplier taskContextSupplier) {
    this.storageConf = storageConf;
    this.taskContextSupplier = taskContextSupplier;
  }

  public StorageConfiguration<?> getStorageConf() {
    return storageConf;
  }

  public TaskContextSupplier getTaskContextSupplier() {
    return taskContextSupplier;
  }

  public abstract HoodieAccumulator newAccumulator();

  public abstract <T> HoodieData<T> emptyHoodieData();

  public abstract <K, V> HoodiePairData<K, V> emptyHoodiePairData();

  public <T> HoodieData<T> parallelize(List<T> data) {
    if (data.isEmpty()) {
      return emptyHoodieData();
    } else {
      return parallelize(data, data.size());
    }
  }

  public abstract <T> HoodieData<T> parallelize(List<T> data, int parallelism);

  public abstract <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism);

  public abstract <I, K, V> List<V> mapToPairAndReduceByKey(List<I> data, SerializablePairFunction<I, K, V> mapToPairFunc,
                                                            SerializableBiFunction<V, V, V> reduceFunc, int parallelism);

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

  public abstract void putCachedDataIds(HoodieDataCacheKey cacheKey, int... ids);

  public abstract List<Integer> getCachedDataIds(HoodieDataCacheKey cacheKey);

  public abstract List<Integer> removeCachedDataIds(HoodieDataCacheKey cacheKey);

  public abstract void cancelJob(String jobId);

  public abstract void cancelAllJobs();

  /**
   * Aggregate the elements of each partition, and then the results for all the partitions, using given combine functions and a neutral "zero value".
   *
   * @param data
   * @param zeroValue the initial value for the accumulated result of each partition for the seqOp operator
   * @param seqOp     function to aggregate the elements of each partition
   * @param combOp    function to combine results from different partitions
   * @param <I>       input object type
   * @param <O>       output object type
   * @return the result of the aggregation
   */
  public abstract <I, O> O aggregate(HoodieData<I> data, O zeroValue, Functions.Function2<O, I, O> seqOp, Functions.Function2<O, O, O> combOp);

  public abstract <T> ReaderContextFactory<T> getReaderContextFactory(HoodieTableMetaClient metaClient);

  /**
   * Groups values by key and applies a function to each group of values.
   * [1 iterator maps to 1 key] It only guarantees that items returned by the same iterator shares to the same key.
   * [exact once across iterators] The item returned by the same iterator will not be returned by other iterators.
   * [1 key maps to >= 1 iterators] Items belong to the same shard can be load-balanced across multiple iterators. It's up to API implementations to decide
   *                                load balancing pattern and how many iterators to split into.
   *
   * @param data The input pair<ShardIndex, Item> to process.
   * @param func Function to apply to each group of items with the same shard
   * @param shardIndices Set of all possible shard indices that may appear in the data. This is used for efficient partitioning and load balancing.
   * @param preservesPartitioning whether to preserve partitioning in the resulting collection.
   * @param <V> Type of the value in the input data (must be Comparable)
   * @param <R> Type of the result
   * @return Result of applying the function to each group
   */
  public abstract <V extends Comparable<V>, R> HoodieData<R> processValuesOfTheSameShards(
      HoodiePairData<Integer, V> data, SerializableFunction<Iterator<V>, Iterator<R>> func, Set<Integer> shardIndices, boolean preservesPartitioning);
}
