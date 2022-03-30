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
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.data.HoodieAccumulator;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.EngineProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.function.SerializableBiFunction;
import org.apache.hudi.common.function.SerializableConsumer;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.function.SerializablePairFlatMapFunction;
import org.apache.hudi.common.function.SerializablePairFunction;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.data.HoodieSparkLongAccumulator;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Spark engine implementation of HoodieEngineContext.
 */
public class HoodieSparkEngineContext extends HoodieEngineContext {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkEngineContext.class);
  private final JavaSparkContext javaSparkContext;
  private SQLContext sqlContext;

  public HoodieSparkEngineContext(JavaSparkContext jsc) {
    super(new SerializableConfiguration(jsc.hadoopConfiguration()), new SparkTaskContextSupplier());
    this.javaSparkContext = jsc;
    this.sqlContext = SQLContext.getOrCreate(jsc.sc());
  }

  public void setSqlContext(SQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  public JavaSparkContext getJavaSparkContext() {
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
            flatMapToPairFunc.call(iterator).collect(Collectors.toList()).stream()
                .map(e -> new Tuple2<>(e.getKey(), e.getValue())).iterator()
        )
        .reduceByKey(reduceFunc::apply)
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
    if (key == EngineProperty.COMPACTION_POOL_NAME) {
      javaSparkContext.setLocalProperty("spark.scheduler.pool", value);
    } else if (key == EngineProperty.CLUSTERING_POOL_NAME) {
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
    javaSparkContext.setJobGroup(activeModule, activityDescription);
  }
}
