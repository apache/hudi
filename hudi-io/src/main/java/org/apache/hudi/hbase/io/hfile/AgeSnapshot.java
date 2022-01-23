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

package org.apache.hudi.hbase.io.hfile;

import org.apache.hudi.hbase.metrics.impl.FastLongHistogram;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Snapshot of block cache age in cache.
 * This object is preferred because we can control how it is serialized out when JSON'ing.
 */
@InterfaceAudience.Private
public class AgeSnapshot {

  private transient final FastLongHistogram ageHistogram;
  private transient final long[] quantiles;

  AgeSnapshot(final FastLongHistogram ageHistogram) {
    this.ageHistogram = ageHistogram;
    this.quantiles = ageHistogram.getQuantiles(new double[]{0.75, 0.95, 0.98, 0.99, 0.999});
  }

  public double get75thPercentile() {
    return quantiles[0];
  }

  public double get95thPercentile() {
    return quantiles[1];
  }

  public double get98thPercentile() {
    return quantiles[2];
  }

  public double get99thPercentile() {
    return quantiles[3];
  }

  public double get999thPercentile() {
    return quantiles[4];
  }


  public double getMean() {
    return this.ageHistogram.getMean();
  }

  public double getMax() {
    return this.ageHistogram.getMax();
  }

  public double getMin() {
    return this.ageHistogram.getMin();
  }
}
