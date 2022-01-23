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

package org.apache.hudi.hbase.metrics;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * A statictical sample of histogram values.
 */
@InterfaceAudience.Private
public interface Snapshot {

  /**
   * Return the values with the given quantiles.
   * @param quantiles the requested quantiles.
   * @return the value for the quantiles.
   */
  long[] getQuantiles(double[] quantiles);

  /**
   * Return the values with the default quantiles.
   * @return the value for default the quantiles.
   */
  long[] getQuantiles();

  /**
   * Returns the number of values in the snapshot.
   *
   * @return the number of values
   */
  long getCount();

  /**
   * Returns the total count below the given value
   * @param val the value
   * @return the total count below the given value
   */
  long getCountAtOrBelow(long val);

  /**
   * Returns the value at the 25th percentile in the distribution.
   *
   * @return the value at the 25th percentile
   */
  long get25thPercentile();

  /**
   * Returns the value at the 75th percentile in the distribution.
   *
   * @return the value at the 75th percentile
   */
  long get75thPercentile();

  /**
   * Returns the value at the 90th percentile in the distribution.
   *
   * @return the value at the 90th percentile
   */
  long get90thPercentile();

  /**
   * Returns the value at the 95th percentile in the distribution.
   *
   * @return the value at the 95th percentile
   */
  long get95thPercentile();

  /**
   * Returns the value at the 98th percentile in the distribution.
   *
   * @return the value at the 98th percentile
   */
  long get98thPercentile();

  /**
   * Returns the value at the 99th percentile in the distribution.
   *
   * @return the value at the 99th percentile
   */
  long get99thPercentile();

  /**
   * Returns the value at the 99.9th percentile in the distribution.
   *
   * @return the value at the 99.9th percentile
   */
  long get999thPercentile();

  /**
   * Returns the median value in the distribution.
   *
   * @return the median value
   */
  long getMedian();

  /**
   * Returns the highest value in the snapshot.
   *
   * @return the highest value
   */
  long getMax();

  /**
   * Returns the arithmetic mean of the values in the snapshot.
   *
   * @return the arithmetic mean
   */
  long getMean();

  /**
   * Returns the lowest value in the snapshot.
   *
   * @return the lowest value
   */
  long getMin();

  // TODO: Dropwizard histograms also track stddev
}
