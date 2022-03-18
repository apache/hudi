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

package org.apache.hudi.table.action.commit;

import org.apache.avro.Schema;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieMetaServerConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCopyOnWriteActionBasedOnMetastore extends HoodieClientTestBase {
  private static final Schema SCHEMA = getSchemaFromResource(TestCopyOnWriteActionBasedOnMetastore.class, "/exampleSchema.avsc");

  @Test
  public void testInsert() throws Exception {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withProps(initTableConfig()).withProps(initMetastoreConfig())
        .withPath(basePath).withSchema(SCHEMA.toString()).withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withRemoteServerPort(timelineServicePort).build()).build();
    String instantTime = HoodieTestTable.makeNewCommitTime();
    SparkRDDWriteClient writeClient = getHoodieWriteClient(config);
    writeClient.startCommitWithTime(instantTime);
    assertEquals(1, metaClient.reloadActiveTimeline().getInstants().count());

    // Get some records belong to the same partition (2016/01/31)
    String recordStr1 = "{\"_row_key\":\"8eb5b87a-1feh-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":12}";
    String recordStr2 = "{\"_row_key\":\"8eb5b87b-1feu-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:20:41.415Z\",\"number\":100}";
    String recordStr3 = "{\"_row_key\":\"8eb5b87c-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":15}";
    String recordStr4 = "{\"_row_key\":\"8eb5b87d-1fej-4edd-87b4-6ec96dc405a0\","
        + "\"time\":\"2016-01-31T03:16:41.415Z\",\"number\":51}";

    List<HoodieRecord> records = new ArrayList<>();
    RawTripTestPayload rowChange1 = new RawTripTestPayload(recordStr1);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange1.getRowKey(), rowChange1.getPartitionPath()), rowChange1));
    RawTripTestPayload rowChange2 = new RawTripTestPayload(recordStr2);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange2.getRowKey(), rowChange2.getPartitionPath()), rowChange2));
    RawTripTestPayload rowChange3 = new RawTripTestPayload(recordStr3);
    records.add(new HoodieAvroRecord(new HoodieKey(rowChange3.getRowKey(), rowChange3.getPartitionPath()), rowChange3));
    writeClient.insert(jsc.parallelize(records, 1), instantTime);
    assertEquals(1, metaClient.reloadActiveTimeline().getInstants().count());
  }

  @Override
  protected void initMetaClient(HoodieTableType tableType, Properties properties) throws IOException {
    properties.putAll(initTableConfig());
    properties = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(getTableType())
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(properties)
        .build();
    properties.putAll(initMetastoreConfig());
    properties.put(HoodieWriteConfig.BASE_PATH.key(), basePath);
    metaClient = HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, properties);
  }

  private Map<String, String> initMetastoreConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(HoodieMetaServerConfig.META_SERVER_ENABLE.key(), "true");
    props.put(HoodieMetaServerConfig.META_SERVER_URLS.key(), "");
    // metadata table
    props.put(HoodieMetadataConfig.ENABLE.key(), "false");
    return props;
  }

  private Map<String, String> initTableConfig() {
    Map<String, String> props = new HashMap<>();
    props.put(HoodieTableConfig.NAME.key(), "test");
    props.put(HoodieTableConfig.DATABASE_NAME.key(), "default");
    return props;
  }
}
