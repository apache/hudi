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

package org.apache.hudi.testutils;

import org.apache.hudi.callback.HoodieWriteCommitCallback;
import org.apache.hudi.callback.common.HoodieWriteCommitCallbackMessage;
import org.apache.hudi.config.HoodieWriteConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link HoodieWriteCommitCallback} that records every fired message, so tests can assert that
 * the callback fires for a commit and with which action type. Register it with
 * {@code HoodieWriteCommitCallbackConfig.withCallbackClass(RecordingCommitCallback.class.getName())}.
 *
 * <p>The write client loads the callback reflectively from the config, so the recorded messages
 * have to live in static state; call {@link #reset()} at the start of every test that asserts on
 * them.
 */
public class RecordingCommitCallback implements HoodieWriteCommitCallback {

  private static final List<HoodieWriteCommitCallbackMessage> MESSAGES = new CopyOnWriteArrayList<>();

  public RecordingCommitCallback(HoodieWriteConfig config) {
    // config arg required for reflective instantiation
  }

  @Override
  public void call(HoodieWriteCommitCallbackMessage callbackMessage) {
    MESSAGES.add(callbackMessage);
  }

  /**
   * Snapshot of the messages recorded since the last {@link #reset()}, in the order they fired.
   */
  public static List<HoodieWriteCommitCallbackMessage> messages() {
    return new ArrayList<>(MESSAGES);
  }

  /**
   * Drops every recorded message.
   */
  public static void reset() {
    MESSAGES.clear();
  }
}
