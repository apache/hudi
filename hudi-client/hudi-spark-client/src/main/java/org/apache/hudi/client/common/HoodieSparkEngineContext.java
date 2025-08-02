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

import org.apache.hudi.client.SparkTaskContextSupplier;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieData.HoodieDataCacheKey;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.AvroReaderContextFactory;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.ReaderContextFactory;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.data.HoodieSparkLongAccumulator;
import org.apache.hudi.data.partitioner.ConditionalRangePartitioner;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieSparkKeyGeneratorFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.InternalRow;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import scala.Tuple2;

/**
 * A Spark engine implementation of HoodieEngineContext.
 */
@ThreadSafe
public class HoodieSparkEngineContext extends HoodieEngineContext {

  private final JavaSparkContext javaSparkContext;
  private final SQLContext sqlContext;
  private final Map<HoodieDataCacheKey, List<Integer>> cachedRddIds = new HashMap<>();

  public HoodieSparkEngineContext(JavaSparkContext jsc) {
    this(jsc, SQLContext.getOrCreate(jsc.sc()));
  }

  public HoodieSparkEngineContext(JavaSparkContext jsc, SQLContext sqlContext) {
    super(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), new SparkTaskContextSupplier());
    this.javaSparkContext = jsc;
    this.sqlContext = sqlContext;
  }

  public JavaSparkContext getJavaSparkContext() {
    return javaSparkContext;
  }

  public JavaSparkContext jsc() {
    return javaSparkContext;
  }

  public SQLContext getSqlContext() {
    return sqlContext;
  }

  public static JavaSparkContext getSparkContext(HoodieEngineContext context) {
    return ((HoodieSparkEngineContext) context).getJavaSparkContext();
  }

  @Override
  public HoodieAccumulator newAccumulator() {
    HoodieSparkLongAccumulator accumulator = HoodieSparkLongAccumulator.create();
    javaSparkContext.sc().register(accumulator.getAccumulator());
    return accumulator;
  }

  @Override
  public <T> HoodieData<T> emptyHoodieData() {
    return HoodieJavaRDD.of(javaSparkContext.emptyRDD());
  }

  @Override
  public <K, V> HoodiePairData<K, V> emptyHoodiePairData() {
    return HoodieJavaPairRDD.of(JavaPairRDD.fromJavaRDD(javaSparkContext.emptyRDD()));
  }

  @Override
  public <T> HoodieData<T> parallelize(List<T> data, int parallelism) {
    return HoodieJavaRDD.of(javaSparkContext.parallelize(data, parallelism));
  }

  @Override
  public <I, O> List<O> map(List<I> data, SerializableFunction<I, O> func, int parallelism) {
    return javaSparkContext.parallelize(data, parallelism).map(func::apply).collect();
  }

  @Override
  public <I, K, V> List<V> mapToPairAndReduceByKey(List<I> data, SerializablePairFunction<I, K, V> mapToPairFunc, SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
    return javaSparkContext.parallelize(data, parallelism).mapToPair(input -> {
      Pair<K, V> pair = mapToPairFunc.call(input);
      return new Tuple2<>(pair.getLeft(), pair.getRight());
    }).reduceByKey(reduceFunc::apply).map(Tuple2::_2).collect();
  }

  @Override
  public <I, K, V> Stream<ImmutablePair<K, V>> mapPartitionsToPairAndReduceByKey(
      Stream<I> data, SerializablePairFlatMapFunction<Iterator<I>, K, V> flatMapToPairFunc,
      SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
    return javaSparkContext.parallelize(data.collect(Collectors.toList()), parallelism)
        .mapPartitionsToPair((PairFlatMapFunction<Iterator<I>, K, V>) iterator ->
            flatMapToPairFunc.call(iterator)
                .map(e -> new Tuple2<>(e.getKey(), e.getValue())).iterator()
        )
        .reduceByKey(reduceFunc::apply, parallelism)
        .map(e -> new ImmutablePair<>(e._1, e._2))
        .collect().stream();
  }

  @Override
  public <I, K, V> List<V> reduceByKey(
      List<Pair<K, V>> data, SerializableBiFunction<V, V, V> reduceFunc, int parallelism) {
    return javaSparkContext.parallelize(data, parallelism).mapToPair(pair -> new Tuple2<K, V>(pair.getLeft(), pair.getRight()))
        .reduceByKey(reduceFunc::apply).map(Tuple2::_2).collect();
  }

  @Override
  public <I, O> List<O> flatMap(List<I> data, SerializableFunction<I, Stream<O>> func, int parallelism) {
    return javaSparkContext.parallelize(data, parallelism).flatMap(x -> func.apply(x).iterator()).collect();
  }

  @Override
  public <I> void foreach(List<I> data, SerializableConsumer<I> consumer, int parallelism) {
    javaSparkContext.parallelize(data, parallelism).foreach(consumer::accept);
  }

  @Override
  public <I, K, V> Map<K, V> mapToPair(List<I> data, SerializablePairFunction<I, K, V> func, Integer parallelism) {
    if (Objects.nonNull(parallelism)) {
      return javaSparkContext.parallelize(data, parallelism).mapToPair(input -> {
        Pair<K, V> pair = func.call(input);
        return new Tuple2(pair.getLeft(), pair.getRight());
      }).collectAsMap();
    } else {
      return javaSparkContext.parallelize(data).mapToPair(input -> {
        Pair<K, V> pair = func.call(input);
        return new Tuple2(pair.getLeft(), pair.getRight());
      }).collectAsMap();
    }
  }

  @Override
  public void setProperty(EngineProperty key, String value) {
    if (key.equals(EngineProperty.COMPACTION_POOL_NAME)
        || key.equals(EngineProperty.CLUSTERING_POOL_NAME)
        || key.equals(EngineProperty.DELTASYNC_POOL_NAME)) {
      javaSparkContext.setLocalProperty("spark.scheduler.pool", value);
    } else {
      throw new HoodieException("Unknown engine property :" + key);
    }
  }

  @Override
  public Option<String> getProperty(EngineProperty key) {
    if (key == EngineProperty.EMBEDDED_SERVER_HOST) {
      return Option.ofNullable(javaSparkContext.getConf().get("spark.driver.host", null));
    }
    throw new HoodieException("Unknown engine property :" + key);
  }

  @Override
  public void setJobStatus(String activeModule, String activityDescription) {
    javaSparkContext.setJobDescription(String.format("%s:%s", activeModule, activityDescription));
  }

  @Override
  public void putCachedDataIds(HoodieDataCacheKey cacheKey, int... ids) {
    synchronized (cachedRddIds) {
      cachedRddIds.putIfAbsent(cacheKey, new ArrayList<>());
      for (int id : ids) {
        cachedRddIds.get(cacheKey).add(id);
      }
    }
  }

  @Override
  public List<Integer> getCachedDataIds(HoodieDataCacheKey cacheKey) {
    synchronized (cachedRddIds) {
      return cachedRddIds.getOrDefault(cacheKey, Collections.emptyList());
    }
  }

  @Override
  public List<Integer> removeCachedDataIds(HoodieDataCacheKey cacheKey) {
    synchronized (cachedRddIds) {
      List<Integer> removed = cachedRddIds.remove(cacheKey);
      return removed == null ? Collections.emptyList() : removed;
    }
  }

  @Override
  public void cancelJob(String groupId) {
    javaSparkContext.cancelJobGroup(groupId);
  }

  @Override
  public void cancelAllJobs() {
    javaSparkContext.cancelAllJobs();
  }

  @Override
  public <I, O> O aggregate(HoodieData<I> data, O zeroValue, Functions.Function2<O, I, O> seqOp, Functions.Function2<O, O, O> combOp) {
    Function2<O, I, O> seqOpFunc = seqOp::apply;
    Function2<O, O, O> combOpFunc = combOp::apply;
    return HoodieJavaRDD.getJavaRDD(data).aggregate(zeroValue, seqOpFunc, combOpFunc);
  }

  @Override
  public ReaderContextFactory<?> getReaderContextFactory(HoodieTableMetaClient metaClient) {
    // metadata table are only supported by the AvroReaderContext.
    if (metaClient.isMetadataTable()) {
      return new AvroReaderContextFactory(metaClient);
    }
    return getDefaultContextFactory(metaClient, metaClient.getTableConfig().populateMetaFields());
  }

  @Override
  public ReaderContextFactory<InternalRow> getDefaultContextFactory(HoodieTableMetaClient metaClient, boolean shouldUseMetaFields) {
    return new SparkReaderContextFactory(this, metaClient, shouldUseMetaFields);
  }

  public SparkConf getConf() {
    return javaSparkContext.getConf();
  }

  public Configuration hadoopConfiguration() {
    return javaSparkContext.hadoopConfiguration();
  }

  public <T> JavaRDD<T> emptyRDD() {
    return javaSparkContext.emptyRDD();
  }

  /**
   * Maps groups by key with automatic repartitioning based on sample key ranges.
   *
   * This Spark-specific implementation performs range-based repartitioning of the input data
   * before applying the process function. The repartitioning ensures a statistically even distribution
   * of data across partitions based on the provided key space, which can significantly improve
   * performance for skewed datasets.
   *
   * Note:
   * 1. This repartitioning behavior is specific to the Spark engine context and is not
   * applicable to other engine contexts like HoodieLocalEngineContext.
   * 2. The algorithm is not deterministic across different runs since the sample seed is based on current time.
   *
   * @param data The input key-value pair data
   * @param processFunc Function to apply to each group of values
   * @param keySpace List of keys to define the partitioning ranges
   * @param preservesPartitioning Whether the operation preserves partitioning
   * @return Processed data after grouping by key and applying the process function
   */
  @Override
  public <K extends Comparable<K>, V extends Comparable<V>, R> HoodieData<R> mapGroupsByKey(HoodiePairData<K, V> data,
                                                                                            SerializableFunction<Iterator<V>, Iterator<R>> processFunc,
                                                                                            List<K> keySpace,
                                                                                            boolean preservesPartitioning) {
    HoodiePairData<K, V> repartitionedData = rangeBasedRepartitionForEachKey(
        data, keySpace, 0.02, 100000, System.nanoTime());
    return repartitionedData.values().mapPartitions(processFunc, preservesPartitioning);
  }

  @Override
  public KeyGenerator createKeyGenerator(TypedProperties props) throws IOException {
    return HoodieSparkKeyGeneratorFactory.createKeyGenerator(props);
  }

  /**
   * Performs range-based repartitioning of data based on key distribution to optimize partition sizes.
   *
   * <p>This method achieves efficient data distribution by:</p>
   * <ol>
   *   <li><strong>Sampling:</strong> Samples a fraction of data for each key to understand the distribution
   *       without processing the entire dataset</li>
   *   <li><strong>Range Analysis:</strong> Analyzes the sampled data to determine optimal partition boundaries
   *       that ensure each partition contains a balanced number of keys</li>
   *   <li><strong>Repartitioning:</strong> Redistributes the original data across partitions based on the
   *       computed range boundaries, ensuring keys within the same range are co-located</li>
   *   <li><strong>Sorting:</strong> Sorts data within each partition for efficient processing</li>
   * </ol>
   *
   * <p>The method is particularly useful for: Balancing workload across partitions for better parallel processing</p>
   *
   * @param data The input data as key-value pairs where keys are integers and values are of type V
   * @param partitioningKeySpace The set must cover all possible keys of the given data
   * @param sampleFraction The fraction of data to sample for each key (between 0 and 1).
   *                       A higher fraction provides better distribution analysis but increases sampling overhead.
   *                       It typically should be smaller than 0.05 for large datasets.
   * @param maxKeyPerBucket The maximum number of keys allowed per partition to prevent partition skew
   * @param seed The random seed for reproducible sampling results
   * @param <V> Type of the value in the input data (must be Comparable)
   * @return A repartitioned and sorted HoodiePairData with optimized key distribution across partitions
   * @throws IllegalArgumentException if sampleFraction is not between 0 and 1
   */
  public <S extends Comparable<S>, V extends Comparable<V>> HoodiePairData<S, V> rangeBasedRepartitionForEachKey(
      HoodiePairData<S, V> data, List<S> partitioningKeySpace, double sampleFraction, int maxKeyPerBucket, long seed) {
    ValidationUtils.checkState(sampleFraction > 0 && sampleFraction <= 1, "sampleFraction must be between 0 and 1");
    Map<S, Double> samplingFractions = new HashMap<>();
    for (S s : partitioningKeySpace) {
      samplingFractions.put(s, sampleFraction);
    }
    JavaPairRDD<S, V> pairRddDataSKV = HoodieJavaPairRDD.getJavaPairRDD(data);
    JavaPairRDD<S, V> sampled = pairRddDataSKV.sampleByKeyExact(false, samplingFractions, seed);
    Map<S, List<V>> splitPointsMap = ConditionalRangePartitioner.computeSplitPointMapDistributed(sampled, sampleFraction, maxKeyPerBucket);
    ConditionalRangePartitioner<S, V> partitioner = new ConditionalRangePartitioner<>(splitPointsMap);
    JavaPairRDD<Tuple2<S, V>, V> compositeKeyRdd = pairRddDataSKV.mapToPair(t -> new Tuple2<>(t, null));
    return HoodieJavaPairRDD.of(
        compositeKeyRdd.repartitionAndSortWithinPartitions(
                partitioner,
                new ConditionalRangePartitioner.CompositeKeyComparator<>())
            .mapToPair(e -> e._1));
  }
}
