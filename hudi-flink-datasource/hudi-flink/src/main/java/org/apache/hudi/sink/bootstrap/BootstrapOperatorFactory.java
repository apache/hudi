/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.bootstrap;

import org.apache.hudi.configuration.OptionsResolver;

import org.apache.flink.configuration.Configuration;

/**
 * Factory that resolves the concrete {@link AbstractBootstrapOperator} implementation to use for
 * the index bootstrap pipeline, keeping the pipeline construction agnostic of the specific
 * bootstrap operator selection logic.
 */
public final class BootstrapOperatorFactory {

  private BootstrapOperatorFactory() {
  }

  public static AbstractBootstrapOperator createInstance(Configuration conf) {
    if (OptionsResolver.isGlobalRecordLevelIndex(conf)) {
      return new RLIBootstrapOperator(conf);
    } else if (OptionsResolver.isTimeBoundedRLIBootstrapEnabled(conf)) {
      return new TimeBoundedRLIBootstrapOperator(conf);
    }
    return new BootstrapOperator(conf);
  }
}
