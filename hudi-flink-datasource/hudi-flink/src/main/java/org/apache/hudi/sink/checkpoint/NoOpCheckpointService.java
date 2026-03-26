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

package org.apache.hudi.sink.checkpoint;

import org.apache.hudi.common.util.Option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * No-op implementation of {@link CheckpointService}.
 *
 * <p>This is the default implementation used when no external checkpoint service is configured.
 * It always returns empty checkpoint information, allowing Hudi to function without
 * external checkpoint tracking.
 */
public class NoOpCheckpointService implements CheckpointService {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(NoOpCheckpointService.class);

  @Override
  public Option<CheckpointInfo> getCheckpointInfo(CheckpointRequest request) {
    LOG.debug("NoOpCheckpointService called - returning empty checkpoint info. "
        + "Configure a custom CheckpointService implementation if checkpoint tracking is needed.");
    return Option.empty();
  }
}
