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

package org.apache.hudi.sink.meta;

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.hadoop.fs.FileStatus;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A checkpoint message.
 */
public class CkpMessage implements Serializable, Comparable<CkpMessage> {
  private static final long serialVersionUID = 1L;

  public static final Comparator<CkpMessage> COMPARATOR = Comparator.comparing(CkpMessage::getInstant)
      .thenComparing(CkpMessage::getState);

  private final String instant; // the instant time
  private final State state;   // the checkpoint state

  public CkpMessage(String instant, String state) {
    this.instant = instant;
    this.state = State.valueOf(state);
  }

  public CkpMessage(FileStatus fileStatus) {
    String fileName = fileStatus.getPath().getName();
    String[] nameAndExt = fileName.split("\\.");
    ValidationUtils.checkState(nameAndExt.length == 2);
    String name = nameAndExt[0];
    String ext = nameAndExt[1];

    this.instant = name;
    this.state = State.valueOf(ext);
  }

  public String getInstant() {
    return instant;
  }

  public State getState() {
    return state;
  }

  public boolean isAborted() {
    return State.ABORTED == this.state;
  }

  public boolean isComplete() {
    return State.COMPLETED == this.state;
  }

  public boolean isInflight() {
    return State.INFLIGHT == this.state;
  }

  public static String getFileName(String instant, State state) {
    return instant + "." + state.name();
  }

  public static List<String> getAllFileNames(String instant) {
    return Arrays.stream(State.values())
        .map(state -> getFileName(instant, state))
        .collect(Collectors.toList());
  }

  @Override
  public int compareTo(@NotNull CkpMessage o) {
    return COMPARATOR.compare(this, o);
  }

  /**
   * Instant State.
   */
  public enum State {
    // Inflight instant
    INFLIGHT,
    // Aborted instant
    // An instant can be aborted then be reused again, so it has lower priority
    // than COMPLETED
    ABORTED,
    // Committed instant
    COMPLETED
  }

  @Override
  public String toString() {
    return "Ckp{" + "instant='" + instant + '\'' + ", state='" + state + '\'' + '}';
  }
}
