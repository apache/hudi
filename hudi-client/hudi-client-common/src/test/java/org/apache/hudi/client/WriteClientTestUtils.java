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

package org.apache.hudi.client;

import org.apache.hudi.common.model.TableServiceType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.TimeGenerator;
import org.apache.hudi.common.util.Option;

import java.util.Map;
import java.util.function.Consumer;

public class WriteClientTestUtils {
  private WriteClientTestUtils() {
  }

  public static void startCommitWithTime(BaseHoodieWriteClient<?, ?, ?, ?> writeClient, String instantTime, String actionType) {
    writeClient.startCommit(Option.of(instantTime), actionType, writeClient.createMetaClient(false));
  }

  public static void startCommitWithTime(BaseHoodieWriteClient<?, ?, ?, ?> writeClient, String instantTime) {
    HoodieTableMetaClient metaClient = writeClient.createMetaClient(false);
    writeClient.startCommit(Option.of(instantTime), metaClient.getCommitActionType(), metaClient);
  }

  public static Option<String> scheduleTableService(BaseHoodieWriteClient<?, ?, ?, ?> writeClient, String instantTime, Option<Map<String, String>> extraMetadata, TableServiceType tableServiceType) {
    return writeClient.scheduleTableService(Option.of(instantTime), extraMetadata, tableServiceType);
  }

  public static String createNewInstantTime() {
    return HoodieInstantTimeGenerator.createNewInstantTime(false, TestTimeGenerator.INSTANCE, 0);
  }

  private static class TestTimeGenerator implements TimeGenerator {
    private static final TimeGenerator INSTANCE = new TestTimeGenerator();

    @Override
    public long generateTime(boolean skipLocking) {
      return System.currentTimeMillis();
    }

    @Override
    public void consumeTime(boolean skipLocking, Consumer<Long> func) {
    }
  }
}
