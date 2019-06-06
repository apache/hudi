/*
 * Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io.storage;

import static org.junit.Assert.assertTrue;

import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.model.HoodieFileFormat;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.table.HoodieTable;
import java.io.File;
import java.lang.reflect.Field;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestHoodieStorageWriterFactory {
  private JavaSparkContext jsc = null;
  private String basePath = null;
  private String schemaStr;
  private Schema schema;

  @Before
  public void init() throws Exception {
    jsc = new JavaSparkContext(HoodieClientTestUtils.getSparkConfForTest("TestHoodieStorageWriterFactory"));
    TemporaryFolder folder = new TemporaryFolder();
    folder.create();
    basePath = folder.getRoot().getAbsolutePath();
    HoodieTestUtils.init(jsc.hadoopConfiguration(), basePath);
    schemaStr = IOUtils.toString(getClass().getResourceAsStream("/exampleSchema.txt"), "UTF-8");
    schema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schemaStr));
  }

  @Test
  public void testMakeNewPath() throws Exception {
    String commitTime = HoodieTestUtils.makeNewCommitTime();
    HoodieWriteConfig config = makeHoodieClientConfig();
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);

    HoodieStorageWriter parquetWriter = HoodieStorageWriterFactory
        .getStorageWriter(commitTime, new Path(basePath + "/test_data.parquet"), table, config, schema);
    assertTrue(parquetWriter instanceof HoodieParquetWriter);

    Class<?> metaClientClazz = metaClient.getClass();
    Field fileFormat = metaClientClazz.getDeclaredField("hoodieFileFormat");
    fileFormat.setAccessible(true);
    fileFormat.set(metaClient, HoodieFileFormat.ORC);

    Class<?> tableClazz = table.getClass().getSuperclass();
    Field metaClientField = tableClazz.getDeclaredField("metaClient");
    metaClientField.setAccessible(true);
    metaClientField.set(table, metaClient);

    HoodieStorageWriter orcWriter = HoodieStorageWriterFactory
        .getStorageWriter(commitTime, new Path(basePath + "/test_data.orc"), table, config, schema);
    assertTrue(orcWriter instanceof HoodieOrcWriter);
  }

  @After
  public void clean() {
    if (basePath != null) {
      new File(basePath).delete();
    }
    if (jsc != null) {
      jsc.stop();
    }
  }

  private HoodieWriteConfig makeHoodieClientConfig() throws Exception {
    return makeHoodieClientConfigBuilder().build();
  }

  private HoodieWriteConfig.Builder makeHoodieClientConfigBuilder() throws Exception {
    return HoodieWriteConfig.newBuilder().withPath(basePath).withSchema(schemaStr);
  }
}
