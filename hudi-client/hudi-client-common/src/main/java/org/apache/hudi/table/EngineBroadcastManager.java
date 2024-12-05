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

package org.apache.hudi.table;

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.conf.Configuration;

import java.io.Serializable;

/**
 * Broadcast variable management for engines.
 */
public class EngineBroadcastManager implements Serializable {

  /**
   * Prepares and broadcasts necessary information needed by compactor.
   */
  public void prepareAndBroadcast() {
    // NO operation.
  }

  /**
   * Returns the {@link HoodieReaderContext} instance needed by the file group reader based on
   * the broadcast variables.
   *
   * @param basePath Table base path
   */
  public Option<HoodieReaderContext> retrieveFileGroupReaderContext(StoragePath basePath) {
    return Option.empty();
  }

  /**
   * Retrieves the broadcast configuration.
   */
  public Option<Configuration> retrieveStorageConfig() {
    return Option.empty();
  }
}
