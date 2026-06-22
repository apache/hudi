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
import org.apache.hudi.common.data.HoodieAtomicLongAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodieListPairData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.function.FunctionWrapper;
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
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.storage.StorageConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A general-purpose {@link HoodieEngineContext} that executes all parallel operations on a
 * dedicated classloader-aware {@link ExecutorService}, so classes resolved by the application
 * classloader remain visible to worker threads on Java 11+.
 *
 * <p>The pool is lazily created once per JVM (JLS §12.4.2) and shared across all instances.
 * Worker threads are daemon threads and carry the classloader of this class, preventing
 * {@code ClassNotFoundException} that occurs when the common {@link ForkJoinPool} is used
 * because its workers do not inherit the submitting thread's context classloader.
 */
public class ExecutorServiceBasedEngineContext extends HoodieEngineContext {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutorServiceBasedEngineContext.class);

  // Lazy-initialized, daemon, fixed thread pool whose workers carry the correct classloader.
  // JLS §12.4.2 guarantees thread-safe initialization via class-loading locks.
  private static class PoolHolder {
    static final ExecutorService INSTANCE = createExecutorService();
  }

  private static ExecutorService createExecutorService() {
    int parallelism = ForkJoinPool.commonPool().getParallelism();
    ClassLoader cl = ExecutorServiceBasedEngineContext.class.getClassLoader();
    ExecutorService executor = Executors.newFixedThreadPool(parallelism, r -> {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setContextClassLoader(cl);
      t.setDaemon(true);
      return t;
    });
    LOG.info("Created ExecutorServiceBasedEngineContext pool with {} threads", parallelism);
    return executor;
  }

  public ExecutorServiceBasedEngineContext(StorageConfiguration<?> conf) {
    super(conf, new LocalTaskContextSupplier());
  }

  // ---- Core parallel helpers ----

  /**
   * Submits each element to the executor pool and collects results in input order.
   * RuntimeExceptions / Errors from {@code func} are re-thrown as-is after unwrapping
   * the {@link CompletionException} wrapper.
   */
  private <I, O> List<O> mapAsync(List<I> data, Function<I, O> func) {
    List<CompletableFuture<O>> futures = data.stream()
        .map(item -> CompletableFuture.supplyAsync(() -> func.apply(item), PoolHolder.INSTANCE))
        .collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    } catch (CompletionException e) {
      throw rethrowUnwrapped(e);
    }
    return futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
  }

  private static RuntimeException rethrowUnwrapped(CompletionException e) {
    Throwable cause = e.getCause();
    if (cause instanceof RuntimeException) {
      throw (RuntimeException) cause;
    }
    if (cause instanceof Error) {
      throw (Error) cause;
    }
    throw e;
  }

  // ---- HoodieEngineContext implementations ----

  @Override
  public HoodieAccumulator newAccumulator() {
    return HoodieAtomicLongAccumulator.create();
  }

  @Override
  public <T> HoodieData<T> emptyHoodieData() {
    return HoodieListData.eager(Collections.emptyList());
  }

  @Override
  public <K, V> HoodiePairData<K, V> emptyHoodiePairData() {
    return HoodieListPairData.eager(Collections.emptyList());
  }

  @Override
  public <T> HoodieData<T> parallelize(List<T> data, int parallelism) {
    return HoodieListData.eager(data);
  }

  @Override
  public <T> HoodieData<T> union(List<HoodieData<T>> dataList) {
    return HoodieListData.eager(dataList.stream()
        .flatMap(hoodieData -> hoodieData.collectAsList().stream())
        .collect(Collectors.toList()));
  }

  @Override
  public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
    return mapAsync(data, FunctionWrapper.throwingMapWrapper(func));
  }

  @Override
  public <I, K, V> List<V> mapToPairAndReduceByKey(List<I> data, SerializablePairFunction<I, K, V> mapToPairFunc,
                                                    SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
    List<Pair<K, V>> pairs = mapAsync(data, FunctionWrapper.throwingMapToPairWrapper(mapToPairFunc));
    return pairs.stream()
        .collect(Collectors.groupingBy(Pair::getKey)).values().stream()
        .map(list -> list.stream().map(Pair::getValue)
            .reduce(FunctionWrapper.throwingReduceWrapper(reduceFunc)).orElse(null))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public <I, K, V> Stream<ImmutablePair<K, V>> mapPartitionsToPairAndReduceByKey(
      Stream<I> data, SerializablePairFlatMapFunction<Iterator<I>, K, V> flatMapToPairFunc,
      SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
    try {
      return CompletableFuture.supplyAsync(() ->
          FunctionWrapper.throwingFlatMapToPairWrapper(flatMapToPairFunc).apply(data.iterator())
              .collect(Collectors.groupingBy(Pair::getKey)).entrySet().stream()
              .map(entry -> new ImmutablePair<>(entry.getKey(),
                  entry.getValue().stream().map(Pair::getValue)
                      .reduce(FunctionWrapper.throwingReduceWrapper(reduceFunc)).orElse(null)))
              .filter(Objects::nonNull),
          PoolHolder.INSTANCE).join();
    } catch (CompletionException e) {
      throw rethrowUnwrapped(e);
    }
  }

  @Override
  public <I, K, V> List<V> reduceByKey(List<Pair<K, V>> data, SerializableBiFunction<V, V, V> reduceFunc,
                                        int parallelism) {
    // Group by key (sequential), then reduce each group in parallel on the executor.
    Map<K, List<V>> grouped = data.stream()
        .collect(Collectors.groupingBy(Pair::getKey,
            Collectors.mapping(Pair::getValue, Collectors.toList())));
    return mapAsync(new ArrayList<>(grouped.entrySet()),
        entry -> entry.getValue().stream()
            .reduce(FunctionWrapper.throwingReduceWrapper(reduceFunc)).orElse(null))
        .stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  public <I, O> List<O> flatMap(List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism) {
    return mapAsync(data, FunctionWrapper.throwingFlatMapWrapper(func))
        .stream().flatMap(s -> s).collect(Collectors.toList());
  }

  @Override
  public <I> void foreach(List<I> data, SerializableConsumer<I> consumer, int parallelism) {
    List<CompletableFuture<Void>> futures = data.stream()
        .map(item -> CompletableFuture.runAsync(
            () -> FunctionWrapper.throwingForeachWrapper(consumer).accept(item), PoolHolder.INSTANCE))
        .collect(Collectors.toList());
    try {
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    } catch (CompletionException e) {
      throw rethrowUnwrapped(e);
    }
  }

  @Override
  public <I, K, V> Map<K, V> mapToPair(List<I> data, SerializablePairFunction<I, K, V> func, Integer parallelism) {
    return mapAsync(data, FunctionWrapper.throwingMapToPairWrapper(func))
        .stream().collect(Collectors.toMap(Pair::getLeft, Pair::getRight, (oldVal, newVal) -> newVal));
  }

  @Override
  public void setProperty(EngineProperty key, String value) {
    // no operation
  }

  @Override
  public Option<String> getProperty(EngineProperty key) {
    return Option.empty();
  }

  @Override
  public void setJobStatus(String activeModule, String activityDescription) {
    // no operation
  }

  @Override
  public void clearJobStatus() {
    // no operation
  }

  @Override
  public void putCachedDataIds(HoodieDataCacheKey cacheKey, int... ids) {
    // no operation
  }

  @Override
  public List<Integer> getCachedDataIds(HoodieDataCacheKey cacheKey) {
    return Collections.emptyList();
  }

  @Override
  public List<Integer> removeCachedDataIds(HoodieDataCacheKey cacheKey) {
    return Collections.emptyList();
  }

  @Override
  public void cancelJob(String jobId) {
    // no operation
  }

  @Override
  public void cancelAllJobs() {
    // no operation
  }

  @Override
  public <I, O> O aggregate(HoodieData<I> data, O zeroValue, Functions.Function2<O, I, O> seqOp,
                             Functions.Function2<O, O, O> combOp) {
    return data.collectAsList().stream().reduce(zeroValue, seqOp::apply, combOp::apply);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ReaderContextFactory<T> getReaderContextFactory(HoodieTableMetaClient metaClient) {
    return (ReaderContextFactory<T>) getEngineReaderContextFactory(metaClient);
  }

  @Override
  public ReaderContextFactory<?> getEngineReaderContextFactory(HoodieTableMetaClient metaClient) {
    return new AvroReaderContextFactory(metaClient, new TypedProperties());
  }

  @Override
  public KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    throw new UnsupportedOperationException("Not yet implemented");
  }
}
