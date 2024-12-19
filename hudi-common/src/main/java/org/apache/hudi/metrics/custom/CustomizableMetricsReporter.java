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

package org.apache.hudi.metrics.custom;

import org.apache.hudi.metrics.MetricsReporter;

import com.codahale.metrics.MetricRegistry;

import java.util.Properties;

/**
 * Extensible metrics reporter for custom implementation.
 */
public abstract class CustomizableMetricsReporter extends MetricsReporter {
  private final Properties props;
  private final MetricRegistry registry;

  public CustomizableMetricsReporter(Properties props, MetricRegistry registry) {
    this.props = props;
    this.registry = registry;
  }

  public Properties getProps() {
    return props;
  }

  public MetricRegistry getRegistry() {
    return registry;
  }
}
