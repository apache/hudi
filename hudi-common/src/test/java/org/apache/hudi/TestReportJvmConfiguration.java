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

package org.apache.hudi;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

public class TestReportJvmConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(TestReportJvmConfiguration.class);

  @Test
  public void testReportMemoryUsage() {
    reportMemoryUsageWithMXBean();
    reportMemoryUsageWithRuntime();
  }

  private void reportMemoryUsageWithMXBean() {
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

    MemoryUsage heapMemoryUsage = memoryBean.getHeapMemoryUsage();
    MemoryUsage nonHeapMemoryUsage = memoryBean.getNonHeapMemoryUsage();

    LOG.warn("Heap Memory Usage (MemoryMXBean):");
    LOG.warn("  Used: " + heapMemoryUsage.getUsed() + " bytes");
    LOG.warn("  Committed: " + heapMemoryUsage.getCommitted() + " bytes");
    LOG.warn("  Max: " + heapMemoryUsage.getMax() + " bytes");

    LOG.warn("Non-Heap Memory Usage (MemoryMXBean):");
    LOG.warn("  Used: " + nonHeapMemoryUsage.getUsed() + " bytes");
    LOG.warn("  Committed: " + nonHeapMemoryUsage.getCommitted() + " bytes");
    LOG.warn("  Max: " + nonHeapMemoryUsage.getMax() + " bytes");
  }

  private void reportMemoryUsageWithRuntime() {
    Runtime runtime = Runtime.getRuntime();

    long totalMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();
    long usedMemory = totalMemory - freeMemory;

    LOG.warn("Memory Usage (Runtime):");
    LOG.warn("  Total Memory: " + totalMemory + " bytes");
    LOG.warn("  Free Memory: " + freeMemory + " bytes");
    LOG.warn("  Used Memory: " + usedMemory + " bytes");
  }
}
