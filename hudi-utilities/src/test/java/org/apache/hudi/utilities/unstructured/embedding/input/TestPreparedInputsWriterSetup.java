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

package org.apache.hudi.utilities.unstructured.embedding.input;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PayloadDestination} over a real embedding table whose job sets direct markers, which the shared out-of-line writer
 * refuses: the destination refuses on the driver, before any payload is prepared, naming the setting that turns the markers
 * off; no payload file is written.
 */
class TestPreparedInputsWriterSetup {

  @TempDir
  Path temp;

  @Test
  void directMarkersAreRefusedOnTheDriver() throws IOException {
    String base = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
        .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), base);
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.MARKERS_TYPE.key(), MarkerType.DIRECT.name());

    StorageConfiguration<?> conf = HadoopFSUtils.getStorageConf(new Configuration());
    HoodieValidationException failure = assertThrows(HoodieValidationException.class, () -> new PayloadDestination(conf, base, props));
    assertTrue(failure.getMessage().contains("TIMELINE_SERVER_BASED"), failure.getMessage());
    assertTrue(failure.getMessage().contains(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key() + "=false"), failure.getMessage());
    assertFalse(Files.exists(temp.resolve("embedding").resolve(HoodieTableMetaClient.METAFOLDER_NAME + "/.blob")), "no payload file");
  }
}
