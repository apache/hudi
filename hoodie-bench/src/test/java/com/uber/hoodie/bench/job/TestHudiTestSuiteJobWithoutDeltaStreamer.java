/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.bench.job;

import static org.junit.Assert.assertEquals;

import com.uber.hoodie.bench.dag.TestInsertUpsertDag;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class TestHudiTestSuiteJobWithoutDeltaStreamer extends TestHudiTestSuiteJob {

  @Test
  public void testInsertUpdateWithActions() throws Exception {
    dfs.delete(new Path(dfsBasePath + "/input"), true);
    dfs.delete(new Path(dfsBasePath + "/result"), true);
    String inputBasePath = dfsBasePath + "/input/" + UUID.randomUUID().toString();
    String outputBasePath = dfsBasePath + "/result/" + UUID.randomUUID().toString();
    HudiTestSuiteJob.HudiTestSuiteConfig cfg = makeConfig(inputBasePath, outputBasePath);
    cfg.workloadDagGenerator = TestInsertUpsertDag.class.getName();
    cfg.useDeltaStreamer = false;
    HudiTestSuiteJob hudiTestSuiteJob = new HudiTestSuiteJob(cfg, jsc);
    hudiTestSuiteJob.runTestSuite();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(new Configuration(), cfg.targetBasePath);
    assertEquals(metaClient.getActiveTimeline().getCommitsTimeline().getInstants().count(), 2);
  }

  @Override
  protected HudiTestSuiteJob.HudiTestSuiteConfig makeConfig(String inputBasePath, String outputBasePath) {
    HudiTestSuiteJob.HudiTestSuiteConfig cfg = super.makeConfig(inputBasePath, outputBasePath);
    cfg.useDeltaStreamer = false;
    return cfg;
  }

}
