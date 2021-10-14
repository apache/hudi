/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.data;

import org.apache.hudi.common.function.SerializableFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.hudi.common.function.FunctionWrapper.throwingMapWrapper;

/**
 * Holds a {@link List} of objects.
 *
 * @param <T> type of object.
 */
public class HoodieListData<T> extends HoodieData<T> {

  private final List<T> listData;

  private HoodieListData(List<T> listData) {
    this.listData = listData;
  }

  /**
   * @param listData a {@link List} of objects in type T.
   * @param <T>      type of object.
   * @return a new instance containing the {@link List<T>} reference.
   */
  public static <T> HoodieListData<T> of(List<T> listData) {
    return new HoodieListData<>(listData);
  }

  /**
   * @param hoodieData {@link HoodieListData<T>} instance containing the {@link List} of objects.
   * @param <T>        type of object.
   * @return the a {@link List} of objects in type T.
   */
  public static <T> List<T> getList(HoodieData<T> hoodieData) {
    return ((HoodieListData<T>) hoodieData).get();
  }

  @Override
  public List<T> get() {
    return listData;
  }

  @Override
  public boolean isEmpty() {
    return listData.isEmpty();
  }

  @Override
  public void persist(Properties properties) {
    // No OP
  }

  @Override
  public <O> HoodieData<O> map(SerializableFunction<T, O> func) {
    return HoodieListData.of(listData.stream().parallel()
        .map(throwingMapWrapper(func)).collect(Collectors.toList()));
  }

  @Override
  public <O> HoodieData<O> flatMap(SerializableFunction<T, Iterator<O>> func) {
    Function<T, Iterator<O>> throwableFunc = throwingMapWrapper(func);
    return HoodieListData.of(listData.stream().flatMap(e -> {
      List<O> result = new ArrayList<>();
      Iterator<O> iterator = throwableFunc.apply(e);
      iterator.forEachRemaining(result::add);
      return result.stream();
    }).collect(Collectors.toList()));
  }

  @Override
  public List<T> collectAsList() {
    return listData;
  }
}
