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
 * A recording {@link HoodieWriteCommitCallback} that captures every fired message so tests can
 * assert the callback fires for table-service (compaction/clustering) commits with the expected
 * action type. Loaded reflectively from the write config, so it needs a public
 * {@code (HoodieWriteConfig)} constructor, and the messages have to live in static state; call
 * {@link #reset()} at the start of every test that asserts on them.
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

  public static List<HoodieWriteCommitCallbackMessage> messages() {
    return new ArrayList<>(MESSAGES);
  }

  public static void reset() {
    MESSAGES.clear();
  }
}
