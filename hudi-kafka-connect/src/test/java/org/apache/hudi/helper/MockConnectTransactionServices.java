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

package org.apache.hudi.helper;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.connect.writers.ConnectTransactionServices;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Helper class for {@link ConnectTransactionServices} to generate
 * a unique commit time for testing purposes.
 */
public class MockConnectTransactionServices implements ConnectTransactionServices {

  private int commitTime;

  public MockConnectTransactionServices() {
    commitTime = 100;
  }

  @Override
  public String startCommit() {
    commitTime++;
    return String.valueOf(commitTime);
  }

  @Override
  public boolean endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {
    assertEquals(String.valueOf(this.commitTime), commitTime);
    return true;
  }

  @Override
  public Map<String, String> fetchLatestExtraCommitMetadata() {
    return new HashMap<>();
  }
}
