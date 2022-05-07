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

import io.javalin.Context;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.CompletableFuture;

/**
 * Future for async marker creation request.
 */
public class MarkerCreationFuture extends CompletableFuture<String> {
  private static final Logger LOG = LogManager.getLogger(MarkerCreationFuture.class);
  private final Context context;
  private final String markerDirPath;
  private final String markerName;
  private boolean result;
  private final HoodieTimer timer;

  public MarkerCreationFuture(Context context, String markerDirPath, String markerName) {
    super();
    this.timer = new HoodieTimer().startTimer();
    this.context = context;
    this.markerDirPath = markerDirPath;
    this.markerName = markerName;
    this.result = false;
  }

  public Context getContext() {
    return context;
  }

  public String getMarkerDirPath() {
    return markerDirPath;
  }

  public String getMarkerName() {
    return markerName;
  }

  public boolean isSuccessful() {
    return result;
  }

  public void setResult(boolean result) {
    LOG.debug("Request queued for " + timer.endTimer() + " ms");
    this.result = result;
  }
}
