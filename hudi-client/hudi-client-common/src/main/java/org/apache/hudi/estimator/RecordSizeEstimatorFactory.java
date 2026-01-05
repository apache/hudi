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

package org.apache.hudi.estimator;

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import lombok.extern.slf4j.Slf4j;

/**
 * Factory class to instantiate the configured implementation of {@link RecordSizeEstimator}.
 * Since record estimation is best effort, we use the {@link AverageRecordSizeEstimator} (default implementation)
 * as a backup in case of fatal exceptions.
 */
@Slf4j
public class RecordSizeEstimatorFactory {

  public static RecordSizeEstimator createRecordSizeEstimator(HoodieWriteConfig writeConfig) {
    String recordSizeEstimatorClass = writeConfig.getRecordSizeEstimator();
    try {
      return (RecordSizeEstimator) ReflectionUtils.loadClass(recordSizeEstimatorClass, writeConfig);
    } catch (Throwable e) {
      log.warn("Unable to instantiate the record estimator implementation {}. Falling back to use default AverageRecordSizeEstimator.\" ", recordSizeEstimatorClass, e);
    }
    return new AverageRecordSizeEstimator(writeConfig);
  }
}
