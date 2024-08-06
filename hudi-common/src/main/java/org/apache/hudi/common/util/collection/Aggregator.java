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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.common.util.Option;

public class Aggregator<K, V, C> {
  CombineFunc<K, V, C> combineFunc;
  K currentKey;
  C currentCombinedValue;

  private Aggregator(CombineFunc<K, V, C> combineFunc, K initKey, V initValue) {
    this.combineFunc = combineFunc;
    this.currentKey = initKey;
    this.currentCombinedValue = combineFunc.initCombine(currentKey, initValue);
  }

  public static <K, V, C> Aggregator init(CombineFunc<K, V, C> combineFunc, K initKey, V initValue) {
    return new Aggregator(combineFunc, initKey, initValue);
  }

  public Option<Pair<K, C>> combine(K newKey, V newValue) {
    Option<Pair<K, C>> output = Option.empty();
    if (newKey.equals(currentKey)) {
      currentCombinedValue = combineFunc.combine(newKey, newValue, currentCombinedValue);
    } else {
      output = Option.of(Pair.of(currentKey, currentCombinedValue));
      currentKey = newKey;
      currentCombinedValue = combineFunc.initCombine(newKey, newValue);
    }
    return output;
  }

  public Pair<K, C> current() {
    return Pair.of(currentKey, currentCombinedValue);
  }
}