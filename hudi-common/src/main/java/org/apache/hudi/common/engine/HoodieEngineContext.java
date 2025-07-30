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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableSortingIterator;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.storage.StorageConfiguration;

import java.io.IOException;
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

  public ReaderContextFactory<?> getReaderContextFactoryDuringWrite(HoodieTableMetaClient metaClient, HoodieRecord.HoodieRecordType recordType) {
    if (recordType == HoodieRecord.HoodieRecordType.AVRO) {
      return new AvroReaderContextFactory(metaClient);
    }
    return getDefaultContextFactory(metaClient);
  }

  /**
   * Returns default reader context factory for the engine.
   * @param metaClient Table metadata client
   */
  public abstract ReaderContextFactory<?> getDefaultContextFactory(HoodieTableMetaClient metaClient);

  /**
   * Groups values by key and applies a processing function to each group.
   *
   * <p>This method takes key-value pairs, groups all values that share the same key,
   * and applies a processing function to each group. The processing function receives
   * the grouped values as an Iterator and can transform them into new results.
   *
   * <p><b>Preconditions:</b>
   * <ul>
   *   <li>data must contain key-value pairs</li>
   *   <li>processFunc must not be null</li>
   *   <li>keySpace must contain all possible keys that may appear in the data</li>
   *   <li>Both key type (K) and value type (V) must implement Comparable</li>
   * </ul>
   *
   * <p><b>Postconditions:</b>
   * <ul>
   *   <li>All values with the same key are grouped together for processing</li>
   *   <li>Each value from the input data is processed exactly once</li>
   *   <li>Values passed to processFunc are sorted within each group</li>
   *   <li>For performance, a single key's values may be split across multiple calls to processFunc</li>
   *   <li>The function returns a collection containing all results from processFunc</li>
   * </ul>
   *
   * @param data The input key-value pairs to process
   * @param processFunc Function that processes a group of values (as Iterator) and produces results
   * @param keySpace Complete set of all possible keys in the data. Used for efficient data distribution
   * @param preservesPartitioning whether to maintain the same data partitioning in the output
   * @param <K> Type of the key (must be Comparable)
   * @param <V> Type of the value (must be Comparable)
   * @param <R> Type of the result produced by processFunc
   * @return Collection of all results produced by processFunc
   */
  public <K extends Comparable<K>, V extends Comparable<V>, R> HoodieData<R> mapGroupsByKey(HoodiePairData<K, V> data,
                                                                                            SerializableFunction<Iterator<V>, Iterator<R>> processFunc,
                                                                                            List<K> keySpace,
                                                                                            boolean preservesPartitioning) {
    // Group values by key and apply the function to each group
    return data.groupByKey()
            .values()
            .flatMap(it -> processFunc.apply(new ClosableSortingIterator<>(it.iterator())));
  }

  public abstract KeyGenerator createKeyGenerator(TypedProperties props) throws IOException;
}
