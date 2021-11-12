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

package org.apache.hudi.common.function;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Function wrapper util class, which catches the exception thrown by input function and return a similar function
 * with no exception thrown.
 */
public class FunctionWrapper {

  public static <I, O> Function<I, O> throwingMapWrapper(SerializableFunction<I, O> throwingMapFunction) {
    return v1 -> {
      try {
        return throwingMapFunction.apply(v1);
      } catch (Exception e) {
        throw new HoodieException("Error occurs when executing map", e);
      }
    };
  }

  public static <I, O> Function<I, Stream<O>> throwingFlatMapWrapper(SerializableFunction<I, Stream<O>> throwingFlatMapFunction) {
    return v1 -> {
      try {
        return throwingFlatMapFunction.apply(v1);
      } catch (Exception e) {
        throw new HoodieException("Error occurs when executing flatMap", e);
      }
    };
  }

  public static <I> Consumer<I> throwingForeachWrapper(SerializableConsumer<I> throwingConsumer) {
    return v1 -> {
      try {
        throwingConsumer.accept(v1);
      } catch (Exception e) {
        throw new HoodieException("Error occurs when executing foreach", e);
      }
    };
  }

  public static <I, K, V> Function<I, Pair<K, V>> throwingMapToPairWrapper(SerializablePairFunction<I, K, V> throwingPairFunction) {
    return v1 -> {
      try {
        return throwingPairFunction.call(v1);
      } catch (Exception e) {
        throw new HoodieException("Error occurs when executing mapToPair", e);
      }
    };
  }

  public static <I, K, V> Function<I, Stream<Pair<K, V>>> throwingFlatMapToPairWrapper(
      SerializablePairFlatMapFunction<I, K, V> throwingPairFlatMapFunction) {
    return v1 -> {
      try {
        return throwingPairFlatMapFunction.call(v1);
      } catch (Exception e) {
        throw new HoodieException("Error occurs when executing mapToPair", e);
      }
    };
  }

  public static <V> BinaryOperator<V> throwingReduceWrapper(SerializableBiFunction<V, V, V> throwingReduceFunction) {
    return (v1, v2) -> {
      try {
        return throwingReduceFunction.apply(v1, v2);
      } catch (Exception e) {
        throw new HoodieException("Error occurs when executing mapToPair", e);
      }
    };
  }
}
