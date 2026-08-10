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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.StorageConfiguration;

import java.util.function.Consumer;

/**
 * Time generator that ensures all clocks pass an expected max clock skew to ensure TrueTime semantics.
 */
public class SkewAdjustingTimeGenerator extends TimeGeneratorBase {
  private final long maxExpectedClockSkewMs;

  public SkewAdjustingTimeGenerator(HoodieTimeGeneratorConfig config, StorageConfiguration<?> storageConf) {
    super(config, storageConf);
    this.maxExpectedClockSkewMs = config.getMaxExpectedClockSkewMs();
  }

  @Override
  public long generateTime(boolean skipLocking) {
    try {
      if (!skipLocking) {
        lock();
      }
      long ts = System.currentTimeMillis();
      Thread.sleep(maxExpectedClockSkewMs);
      return ts;
    } catch (InterruptedException e) {
      throw new HoodieException("Interrupted when get the current time", e);
    } finally {
      if (!skipLocking) {
        unlock();
      }
    }
  }

  @Override
  public void consumeTime(boolean skipLocking, Consumer<Long> func) {
    try {
      if (!skipLocking) {
        lock();
      }
      long currentTimeMillis = generateTime(true);
      func.accept(currentTimeMillis);
    } finally {
      if (!skipLocking) {
        unlock();
      }
    }
  }
}
