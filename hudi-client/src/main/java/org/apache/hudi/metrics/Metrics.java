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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.io.Closeables;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Closeable;

/**
 * This is the main class of the metrics system.
 */
public class Metrics {
  private static Logger logger = LogManager.getLogger(Metrics.class);

  private static volatile boolean initialized = false;
  private static Metrics metrics = null;
  private final MetricRegistry registry;
  private MetricsReporter reporter;

  private Metrics(HoodieWriteConfig metricConfig) {
    registry = new MetricRegistry();

    reporter = MetricsReporterFactory.createReporter(metricConfig, registry);
    if (reporter == null) {
      throw new RuntimeException("Cannot initialize Reporter.");
    }
    // reporter.start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          reporter.report();
          Closeables.close(reporter.getReporter(), true);
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  public static Metrics getInstance() {
    assert initialized;
    return metrics;
  }

  public static synchronized void init(HoodieWriteConfig metricConfig) {
    if (initialized) {
      return;
    }
    try {
      metrics = new Metrics(metricConfig);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
    initialized = true;
  }

  public static void registerGauge(String metricName, final long value) {
    try {
      MetricRegistry registry = Metrics.getInstance().getRegistry();
      registry.register(metricName, (Gauge<Long>) () -> value);
    } catch (Exception e) {
      // Here we catch all exception, so the major upsert pipeline will not be affected if the
      // metrics system
      // has some issues.
      logger.error("Failed to send metrics: ", e);
    }
  }

  public MetricRegistry getRegistry() {
    return registry;
  }

  public Closeable getReporter() {
    return reporter.getReporter();
  }
}
