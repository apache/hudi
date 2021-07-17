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

package org.apache.hudi.sink;

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.TestData;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Test cases for delta stream write.
 */
public class TestWriteMergeOnRead extends TestWriteCopyOnWrite {
  private FileSystem fs;
  private HoodieWriteConfig writeConfig;
  private HoodieFlinkEngineContext context;

  @BeforeEach
  public void before() throws Exception {
    super.before();
    fs = FSUtils.getFs(tempFile.getAbsolutePath(), new org.apache.hadoop.conf.Configuration());
    writeConfig = StreamerUtil.getHoodieClientConfig(conf);
    context = new HoodieFlinkEngineContext(
        new SerializableConfiguration(StreamerUtil.getHadoopConf()),
        new FlinkTaskContextSupplier(null));
  }

  @Override
  protected void setUp(Configuration conf) {
    conf.setBoolean(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
  }

  @Override
  protected void checkWrittenData(File baseFile, Map<String, String> expected, int partitions) throws Exception {
    HoodieTableMetaClient metaClient = HoodieFlinkTable.create(writeConfig, context).getMetaClient();
    Schema schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    String latestInstant = metaClient.getCommitsTimeline().filterCompletedInstants()
        .getInstants()
        .filter(x -> x.getAction().equals(HoodieActiveTimeline.DELTA_COMMIT_ACTION))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList()).stream()
        .max(Comparator.naturalOrder())
        .orElse(null);
    TestData.checkWrittenDataMOR(fs, latestInstant, baseFile, expected, partitions, schema);
  }

  @Override
  protected Map<String, String> getExpectedBeforeCheckpointComplete() {
    return EXPECTED1;
  }

  protected Map<String, String> getMiniBatchExpected() {
    Map<String, String> expected = new HashMap<>();
    // MOR mode merges the messages with the same key.
    expected.put("par1", "[id1,par1,id1,Danny,23,1,par1]");
    return expected;
  }

  protected Map<String, String> getUpsertWithDeleteExpected() {
    Map<String, String> expected = new HashMap<>();
    // id3, id5 were deleted and id9 is ignored
    expected.put("par1", "[id1,par1,id1,Danny,24,1,par1, id2,par1,id2,Stephen,34,2,par1]");
    expected.put("par2", "[id3,par2,id3,Julian,53,3,par2, id4,par2,id4,Fabian,31,4,par2]");
    expected.put("par3", "[id5,par3,id5,Sophia,18,5,par3, id6,par3,id6,Emma,20,6,par3, id9,par3,id9,Jane,19,6,par3]");
    expected.put("par4", "[id7,par4,id7,Bob,44,7,par4, id8,par4,id8,Han,56,8,par4]");
    return expected;
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
