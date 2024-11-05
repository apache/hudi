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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;

import java.util.ArrayList;
import java.util.List;

public class HoodieHistogram extends Histogram {

  public HoodieHistogram() {
    super(new HoodieUnlimitedReservoir());
  }

  /**
   * UNSAFE: This class will cache all measurements in memory and may cause OOM if the number of measurements is too large.
   */
  private static class HoodieUnlimitedReservoir implements Reservoir {

    private final List<Long> measurements = new ArrayList<>();

    @Override
    public int size() {
      return measurements.size();
    }

    @Override
    public void update(long value) {
      measurements.add(value);
    }

    @Override
    public Snapshot getSnapshot() {
      return new UniformSnapshot(measurements);
    }
  }
}
