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

package org.apache.hudi.sink.utils;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.sink.StreamWriteOperatorCoordinator;
import org.apache.hudi.sink.event.Correspondent;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A mock {@link Correspondent} that always return the latest instant.
 *
 * <p>A timeout is set up there to avoid the request hangs forever.
 */
public class MockCorrespondentWithTimeout extends Correspondent {
  private final StreamWriteOperatorCoordinator coordinator;
  private final long commitAckTimeout;

  public MockCorrespondentWithTimeout(StreamWriteOperatorCoordinator coordinator, Configuration conf) {
    this.coordinator = coordinator;
    this.commitAckTimeout = conf.get(FlinkOptions.WRITE_COMMIT_ACK_TIMEOUT);
  }

  @Override
  public String requestInstantTime(long checkpointId) {
    try {
      CompletableFuture<CoordinationResponse> future = this.coordinator.handleCoordinationRequest(InstantTimeRequest.getInstance(checkpointId));
      InstantTimeResponse response = CoordinationResponseSerDe.unwrap(future.get(commitAckTimeout, TimeUnit.MILLISECONDS));
      return response.getInstant();
    } catch (Exception e) {
      throw new HoodieException("Error requesting the instant time from the coordinator", e);
    }
  }
}
