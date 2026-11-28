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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.util.ValidationUtils;

/**
 * Why one run reached no verdict: the cause, and the reading that established it.
 *
 * <p>Both halves travel together because neither is usable alone — the cause chooses the marker and
 * the beacon, and the reason is the only thing that tells an operator which archive version moved
 * or how many tasks re-listed.
 */
public final class Inconclusive {

  private final InconclusiveCause cause;
  private final String reason;

  public Inconclusive(InconclusiveCause cause, String reason) {
    ValidationUtils.checkArgument(cause != null, "an inconclusive run must name its cause");
    ValidationUtils.checkArgument(reason != null, "an inconclusive run must carry its reason");
    this.cause = cause;
    this.reason = reason;
  }

  public InconclusiveCause getCause() {
    return cause;
  }

  public String getReason() {
    return reason;
  }

  @Override
  public String toString() {
    return cause + ": " + reason;
  }
}
