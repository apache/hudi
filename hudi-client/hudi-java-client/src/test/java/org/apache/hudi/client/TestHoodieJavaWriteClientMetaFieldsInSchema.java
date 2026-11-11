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

package org.apache.hudi.client;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.model.PartialUpdateAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.apache.hudi.common.testutils.HoodieTestTable.makeNewCommitTime;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Writes with a schema that declares the {@code _hoodie_} meta fields itself and merges against
 * persisted data, where the payload is decoded with a schema other than the one it was built with.
 */
public class TestHoodieJavaWriteClientMetaFieldsInSchema extends HoodieJavaClientTestHarness {

  private static final Schema SCHEMA = new Schema.Parser().parse("{\"type\":\"record\",\"name\":\"metaRecord\","
      + "\"namespace\":\"org.apache.hudi\",\"fields\":["
      + "{\"name\":\"_hoodie_commit_time\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"_hoodie_commit_seqno\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"_hoodie_record_key\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"_hoodie_partition_path\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"_hoodie_file_name\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"id\",\"type\":[\"null\",\"string\"]},"
      + "{\"name\":\"name\",\"type\":[\"null\",\"string\"]},"
      + "{\"name\":\"age\",\"type\":[\"null\",\"int\"]},"
      + "{\"name\":\"ts\",\"type\":[\"null\",\"long\"]},"
      + "{\"name\":\"part\",\"type\":[\"null\",\"string\"]}]}");

  @Override
  protected void initMetaClient() {
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testMergeWithPersistedDataKeepsDataFields(boolean partialUpdate) throws Exception {
    Properties props = new Properties();
    props.put(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "id");
    props.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "part");
    props.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), "id");
    props.put(HoodieTableConfig.PARTITION_FIELDS.key(), "part");
    props.put(HoodieTableConfig.ORDERING_FIELDS.key(), "ts");
    String payloadClass = partialUpdate ? PartialUpdateAvroPayload.class.getName() : OverwriteWithLatestAvroPayload.class.getName();
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withEngineType(EngineType.JAVA)
        .withProps(props)
        .forTable("meta_fields_in_schema")
        .withPath(basePath)
        .withSchema(SCHEMA.toString())
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder().withPayloadClass(payloadClass).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(props).withIndexType(HoodieIndex.IndexType.SIMPLE).build())
        .withPopulateMetaFields(true)
        .withEmbeddedTimelineServerEnabled(false)
        .build();
    metaClient = HoodieTestUtils.init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE, config.getProps());

    HoodieJavaWriteClient client = getHoodieWriteClient(config);
    write(client, 1, "id1", "Danny", 1, 1L, partialUpdate, false);
    // A second key in the same partition merges into the small base file of the first commit.
    write(client, 2, "id2", "Betty", 2, 2L, partialUpdate, false);
    write(client, 3, "id1", "Danny", 11, 3L, partialUpdate, true);

    Map<String, String> expected = new TreeMap<>();
    expected.put("id1", "id1,Danny,11,3,par1");
    expected.put("id2", "id2,Betty,2,2,par1");
    assertEquals(expected, readLatest());
  }

  private void write(HoodieJavaWriteClient client, int commit, String id, String name, int age, long ts,
                     boolean partialUpdate, boolean upsert) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("id", id);
    record.put("name", name);
    record.put("age", age);
    record.put("ts", ts);
    record.put("part", "par1");
    HoodieRecordPayload<?> payload = partialUpdate ? new PartialUpdateAvroPayload(record, ts) : new OverwriteWithLatestAvroPayload(record, ts);
    List<HoodieRecord> records = Collections.singletonList(new HoodieAvroRecord<>(new HoodieKey(id, "par1"), payload));
    String instant = makeNewCommitTime(commit, "%09d");
    WriteClientTestUtils.startCommitWithTime(client, instant);
    List<WriteStatus> statuses = upsert ? client.upsert(records, instant) : client.insert(records, instant);
    statuses.forEach(status -> assertFalse(status.hasErrors()));
    client.commit(instant, statuses);
  }

  private Map<String, String> readLatest() {
    metaClient = HoodieTableMetaClient.reload(metaClient);
    Map<String, String> rows = new TreeMap<>();
    for (HoodieBaseFile baseFile : getLatestBaseFiles(basePath, storage, basePath + "/par1/*")) {
      for (GenericRecord row : HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(baseFile.getStoragePath())
          .readAvroRecords(storage, baseFile.getStoragePath())) {
        rows.put(String.valueOf(row.get("id")), row.get("id") + "," + row.get("name") + "," + row.get("age")
            + "," + row.get("ts") + "," + row.get("part"));
      }
    }
    return rows;
  }
}
