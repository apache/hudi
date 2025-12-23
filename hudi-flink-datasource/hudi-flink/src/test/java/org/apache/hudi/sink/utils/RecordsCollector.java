/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.utils;

import lombok.Getter;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * A mock {@link Collector} that used in  {@link TestFunctionWrapper}.
 */
@Getter
class RecordsCollector<T> implements Collector<T> {
  private List<T> val;

  public RecordsCollector() {
    this.val = new ArrayList<>();
  }

  public static <T> RecordsCollector<T> getInstance() {
    return new RecordsCollector<>();
  }

  @Override
  public void collect(T t) {
    this.val.add(t);
  }

  @Override
  public void close() {
    this.val.clear();
    this.val = null;
  }
}
