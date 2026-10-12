/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class TestListingBasedRollbackStrategy extends HoodieClientRollbackTestBase {

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @Test
  void testMergeOnReadRollbackRequestsDoNotReloadTimeline() throws IOException {
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(false).build();
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    twoUpsertCommitDataWithTwoPartitions(new ArrayList<>(), new ArrayList<>(), cfg, true, client);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    HoodieInstant instantToRollback = metaClient.getActiveTimeline().getDeltaCommitTimeline().lastInstant().get();
    String rollbackTime = InProcessTimeGenerator.createNewInstantTime();
    List<HoodieRollbackRequest> expected = sorted(new ListingBasedRollbackStrategy(
        getHoodieTable(metaClient, cfg), context, cfg, rollbackTime, false).getRollbackRequests(instantToRollback));

    // the local engine runs the tasks on this meta client, so the reloads the tasks do are visible
    HoodieTableMetaClient spyMetaClient = spy(HoodieTableMetaClient.reload(metaClient));
    List<HoodieRollbackRequest> requests = sorted(new ListingBasedRollbackStrategy(HoodieSparkTable.create(cfg, context, spyMetaClient),
        new HoodieLocalEngineContext(storageConf), cfg, rollbackTime, false).getRollbackRequests(instantToRollback));

    assertFalse(requests.isEmpty());
    assertEquals(expected, requests);
    verify(spyMetaClient, never()).reloadActiveTimeline();
  }

  private static List<HoodieRollbackRequest> sorted(List<HoodieRollbackRequest> requests) {
    return requests.stream()
        .sorted(Comparator.comparing(HoodieRollbackRequest::getPartitionPath).thenComparing(request -> String.valueOf(request.getFileId())))
        .collect(Collectors.toList());
  }
}
