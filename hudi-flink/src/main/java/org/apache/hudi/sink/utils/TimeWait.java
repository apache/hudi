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

import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Tool used for time waiting.
 */
public class TimeWait {
  private static final Logger LOG = LoggerFactory.getLogger(TimeWait.class);

  private final long timeout;    // timeout in SECONDS
  private final long interval;   // interval in MILLISECONDS
  private final String action;   // action to report error message
  private final boolean throwsT; // whether to throw when timeout

  private long waitingTime = 0L;

  private TimeWait(long timeout, long interval, String action, boolean throwsT) {
    this.timeout = timeout;
    this.interval = interval;
    this.action = action;
    this.throwsT = throwsT;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Wait for an interval time.
   *
   * @return true if is timed out
   */
  public boolean waitFor() {
    try {
      if (waitingTime > timeout) {
        final String msg = "Timeout(" + waitingTime + "ms) while waiting for " + action;
        if (this.throwsT) {
          throw new HoodieException(msg);
        } else {
          LOG.warn(msg);
          return true;
        }
      }
      TimeUnit.MILLISECONDS.sleep(interval);
      waitingTime += interval;
      return false;
    } catch (InterruptedException e) {
      throw new HoodieException("Error while waiting for " + action, e);
    }
  }

  /**
   * Builder.
   */
  public static class Builder {
    private long timeout = 5 * 60 * 1000L; // default 5 minutes
    private long interval = 1000;
    private String action;
    private boolean throwsT = false;

    private Builder() {
    }

    public Builder timeout(long timeout) {
      if (timeout > 0) {
        this.timeout = timeout;
      }
      return this;
    }

    public Builder interval(long interval) {
      this.interval = interval;
      return this;
    }

    public Builder action(String action) {
      this.action = action;
      return this;
    }

    public Builder throwsT(boolean throwsT) {
      this.throwsT = throwsT;
      return this;
    }

    public TimeWait build() {
      Objects.requireNonNull(this.action);
      return new TimeWait(this.timeout, this.interval, this.action, this.throwsT);
    }
  }
}
