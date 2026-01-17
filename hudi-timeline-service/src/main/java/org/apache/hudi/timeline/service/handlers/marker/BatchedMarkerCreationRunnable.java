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

package org.apache.hudi.timeline.service.handlers.marker;

import org.apache.hudi.common.util.HoodieTimer;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * A runnable for batch processing marker creation requests.
 */
@Slf4j
public class BatchedMarkerCreationRunnable implements Runnable {

  private final List<BatchedMarkerCreationContext> requestContextList;

  public BatchedMarkerCreationRunnable(List<BatchedMarkerCreationContext> requestContextList) {
    this.requestContextList = requestContextList;
  }

  @Override
  public void run() {
    log.debug("Start processing create marker requests");
    HoodieTimer timer = HoodieTimer.start();

    for (BatchedMarkerCreationContext requestContext : requestContextList) {
      requestContext.getMarkerDirState().processMarkerCreationRequests(
          requestContext.getFutures(), requestContext.getFileIndex());
    }
    log.debug("Finish batch processing of create marker requests in {} ms", timer.endTimer());
  }
}
