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

package org.apache.hudi.metrics;

import com.codahale.metrics.Gauge;
import lombok.Getter;
import lombok.Setter;

/**
 * Similar to {@link Gauge}, but metric value can be updated by {@link #setValue(T)}.
 */
@Getter
@Setter
public class HoodieGauge<T> implements Gauge<T> {
  /**
   * -- SETTER --
   *  Set the metric to a new value.
   */
  private volatile T value;

  /**
   * Create an instance with a default value.
   */
  public HoodieGauge(T value) {
    this.value = value;
  }
}
