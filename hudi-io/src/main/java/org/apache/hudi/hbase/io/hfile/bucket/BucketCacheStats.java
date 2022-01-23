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

package org.apache.hudi.hbase.io.hfile.bucket;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hudi.hbase.io.hfile.CacheStats;
import org.apache.hudi.hbase.util.EnvironmentEdgeManager;

/**
 * Class that implements cache metrics for bucket cache.
 */
@InterfaceAudience.Private
public class BucketCacheStats extends CacheStats {
  private final LongAdder ioHitCount = new LongAdder();
  private final LongAdder ioHitTime = new LongAdder();
  private static final long NANO_TIME = TimeUnit.MILLISECONDS.toNanos(1);
  private long lastLogTime = EnvironmentEdgeManager.currentTime();

  /* Tracing failed Bucket Cache allocations. */
  private LongAdder allocationFailCount = new LongAdder();

  BucketCacheStats() {
    super("BucketCache");

    allocationFailCount.reset();
  }

  @Override
  public String toString() {
    return super.toString() + ", ioHitsPerSecond=" + getIOHitsPerSecond() +
        ", ioTimePerHit=" + getIOTimePerHit() + ", allocationFailCount=" +
        getAllocationFailCount();
  }

  public void ioHit(long time) {
    ioHitCount.increment();
    ioHitTime.add(time);
  }

  public long getIOHitsPerSecond() {
    long now = EnvironmentEdgeManager.currentTime();
    long took = (now - lastLogTime) / 1000;
    lastLogTime = now;
    return took == 0 ? 0 : ioHitCount.sum() / took;
  }

  public double getIOTimePerHit() {
    long time = ioHitTime.sum() / NANO_TIME;
    long count = ioHitCount.sum();
    return ((float) time / (float) count);
  }

  public void reset() {
    ioHitCount.reset();
    ioHitTime.reset();
    allocationFailCount.reset();
  }

  public long getAllocationFailCount() {
    return allocationFailCount.sum();
  }

  public void allocationFailed () {
    allocationFailCount.increment();
  }
}
