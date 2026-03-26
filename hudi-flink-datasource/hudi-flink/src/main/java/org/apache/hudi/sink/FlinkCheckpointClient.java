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

package org.apache.hudi.sink;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.sink.checkpoint.CheckpointService;
import org.apache.hudi.sink.checkpoint.NoOpCheckpointService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Client for fetching checkpoint information using a pluggable {@link CheckpointService}.
 *
 * <p>This client provides a clean interface for checkpoint tracking while delegating
 * the actual implementation to a configurable checkpoint service. By default, it uses
 * {@link NoOpCheckpointService}, but can be configured with custom implementations for
 * enterprise checkpoint systems.
 *
 * <p>This abstraction allows Apache Hudi to support various checkpoint backends without
 * coupling the core codebase to specific internal systems.
 */
public class FlinkCheckpointClient {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkCheckpointClient.class);

  private final CheckpointService checkpointService;

  /**
   * Constructor using the default no-op checkpoint service.
   * This is suitable for standard Apache Hudi deployments that don't require
   * external checkpoint tracking.
   */
  public FlinkCheckpointClient() {
    this(new NoOpCheckpointService());
  }

  /**
   * Constructor accepting a custom checkpoint service implementation.
   *
   * @param checkpointService The checkpoint service to use for fetching checkpoint info
   */
  public FlinkCheckpointClient(CheckpointService checkpointService) {
    this.checkpointService = checkpointService;
  }

  /**
   * Fetches checkpoint information using the configured checkpoint service.
   *
   * @param request The checkpoint request parameters
   * @return Optional containing checkpoint info if successful, empty otherwise
   * @throws IOException if the request fails
   */
  public Option<CheckpointService.CheckpointInfo> getCheckpointInfo(
      CheckpointService.CheckpointRequest request) throws IOException {
    LOG.debug("getCheckpointInfo called with request: {}", request);

    try {
      Option<CheckpointService.CheckpointInfo> result = checkpointService.getCheckpointInfo(request);
      LOG.debug("Checkpoint service returned result. Present: {}", result.isPresent());
      return result;
    } catch (IOException e) {
      LOG.error("IOException in getCheckpointInfo: {}", e.getMessage(), e);
      throw e;
    } catch (Exception e) {
      LOG.error("Unexpected exception in getCheckpointInfo: {}", e.getMessage(), e);
      throw new IOException("Unexpected error getting checkpoint info", e);
    }
  }
}
