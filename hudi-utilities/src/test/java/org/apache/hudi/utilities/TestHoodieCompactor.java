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

package org.apache.hudi.utilities;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Properties;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doReturn;

public class TestHoodieCompactor {
  @TempDir
  protected static java.nio.file.Path sharedTempDir;
  protected static Configuration hadoopConf = HoodieTestUtils.getDefaultHadoopConf();
  private final FileSystem fs;
  private JavaSparkContext jsc;
  private String base;
  private String schemaFile;
  private String basePath;

  public TestHoodieCompactor() throws IOException {
    fs = FileSystem.getLocal(hadoopConf);
    basePath = sharedTempDir.toUri().toString();
  }

  @AfterEach
  public void tearDown() {
    if (null != jsc) {
      jsc.close();
    }
  }

  @BeforeEach
  public void setUp() throws Exception {
    jsc = UtilHelpers.buildSparkContext(
        TestHoodieCompactor.class + "-hoodie", "local[4]");
    base = basePath + "base";
    schemaFile = basePath + "schema.avro";

    HoodieWriteConfig writeConfig = getWriteConfig(base);
    Properties metaClientProps = HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ)
        .setPayloadClass(HoodieAvroPayload.class)
        .fromProperties(writeConfig.getProps())
        .build();
    HoodieTableMetaClient.initTableAndGetMetaClient(
        jsc.hadoopConfiguration(), base, metaClientProps);
  }

  public static HoodieWriteConfig getWriteConfig(String basePath) {
    return HoodieWriteConfig.newBuilder()
        .forTable("compactor_test_table")
        .withPath(basePath)
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .build();
  }

  public static void writeSchema(FileSystem fs, String path, String schema) throws IOException {
    PrintStream os = new PrintStream(fs.create(new Path(path), false), true);
    os.println(schema);
    os.flush();
    os.close();
  }

  @Test
  public void testGetSchemaWithSchemaFile() throws Exception {
    HoodieCompactor.Config config = new HoodieCompactor.Config();
    config.basePath = base;
    config.schemaFile = schemaFile;
    HoodieCompactor compactor = new HoodieCompactor(jsc, config);
    writeSchema(fs, schemaFile, TRIP_EXAMPLE_SCHEMA);

    String schemaStr = compactor.getSchema();
    assertTrue(schemaStr.startsWith(TRIP_EXAMPLE_SCHEMA));
  }

  @Test
  public void testGetSchemaWithoutSchemaFile() throws Exception {
    HoodieCompactor.Config config = new HoodieCompactor.Config();
    config.basePath = base;
    HoodieCompactor compactor = new HoodieCompactor(jsc, config);

    HoodieCompactor spyCompactor = Mockito.spy(compactor);
    doReturn(TRIP_EXAMPLE_SCHEMA).when(spyCompactor).getSchemaFromLatestInstant();
    String schemaStr = spyCompactor.getSchema();
    assertEquals(TRIP_EXAMPLE_SCHEMA, schemaStr);
  }
}

