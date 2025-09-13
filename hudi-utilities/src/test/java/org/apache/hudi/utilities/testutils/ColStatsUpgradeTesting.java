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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Disabled;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordsToStrings;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * spark-submit \
 *   --class org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer \
 *   $HUDI_UTILITIES_BUNDLE \
 *   --table-type COPY_ON_WRITE \
 *   --source-class org.apache.hudi.utilities.sources.JsonDFSSource \
 *   --source-ordering-field timestamp \
 *   --target-base-path /tmp/hudi_trips_logical_types_json \
 *   --target-table trips_logical_types_json \
 *   --props file:///tmp/colstats-upgrade-test/hudi.properties \
 *   --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \
 *   --source-input-path file:///tmp/colstats-upgrade-test
 *
 */
public class ColStatsUpgradeTesting {

  @Disabled
  public void generate() throws IOException {
    generateTestAssets("/tmp/colstats-upgrade-test", 6);
  }

  public void generateDsScript(StoragePath runScript, int version) throws IOException {
    HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    String baseDir = "/tmp/hudi_trips_logical_types_json_v" + version;
    String runscript = "rm -rf " + baseDir + " \n"
        + "for i in {0..4}; do\n"
        + "  spark-submit \\\n"
        + "    --class org.apache.hudi.utilities.streamer.HoodieStreamer \\\n"
        + "    hudi-utilities-bundle_2.12-0.14.1.jar \\\n"
        + "    --table-type MERGE_ON_READ \\\n"
        + "    --source-class org.apache.hudi.utilities.sources.JsonDFSSource \\\n"
        + "    --source-ordering-field timestamp \\\n"
        + "    --target-base-path " + baseDir + "  \\\n"
        + "    --target-table trips_logical_types_json \\\n"
        + "    --props file:///tmp/colstats-upgrade-test/hudi.properties \\\n"
        + "    --schemaprovider-class org.apache.hudi.utilities.schema.FilebasedSchemaProvider \\\n"
        + "    --op UPSERT \\\n"
        + "    --hoodie-conf hoodie.streamer.source.dfs.root=file:///tmp/colstats-upgrade-test/data/data_$i\n"
        + "done";

    try (Writer writer = new OutputStreamWriter(storage.create(runScript))) {
      writer.write(runscript);
      writer.write("\n");
    }
  }

  public void generateTestAssets(String assetDirectory, int version) throws IOException {
    HoodieStorage storage = HoodieTestUtils.getDefaultStorage();
    StoragePath directory = new StoragePath(assetDirectory);
    if (storage.exists(directory)) {
      storage.deleteDirectory(directory);
    }
    assertTrue(storage.createDirectory(directory));
    Schema schema = HoodieTestDataGenerator.AVRO_TRIP_LOGICAL_TYPES_SCHEMA;
    String schemaStr = HoodieTestDataGenerator.TRIP_LOGICAL_TYPES_SCHEMA;
    if (version == 6) {
      schema = HoodieTestDataGenerator.AVRO_TRIP_LOGICAL_TYPES_SCHEMA_V6;
      schemaStr = HoodieTestDataGenerator.TRIP_LOGICAL_TYPES_SCHEMA_V6;
    }

    StoragePath schemaFile = new StoragePath(directory, "schema.avsc");

    try (Writer writer = new OutputStreamWriter(storage.create(schemaFile))) {
      writer.write(schema.toString(true));
      writer.write("\n");
    }

    HoodieTestDataGenerator datagen = new HoodieTestDataGenerator();
    StoragePath dataDirectory = new StoragePath(directory, "data");
    assertTrue(storage.createDirectory(dataDirectory));

    StoragePath propsFile = new StoragePath(directory, "hudi.properties");
    try (Writer writer = new OutputStreamWriter(storage.create(propsFile))) {
      writer.write("hoodie.table.name=trips_logical_types_json\n");
      writer.write("hoodie.datasource.write.table.type=MERGE_ON_READ\n");
      writer.write("hoodie.datasource.write.recordkey.field=_row_key\n");
      writer.write("hoodie.datasource.write.partitionpath.field=partition_path\n");
      writer.write("hoodie.datasource.write.keygenerator.class=org.apache.hudi.keygen.SimpleKeyGenerator\n");
      writer.write("hoodie.datasource.write.precombine.field=timestamp\n");
      writer.write("hoodie.cleaner.policy=KEEP_LATEST_COMMITS\n");
      writer.write("hoodie.cleaner.commits.retained=2\n");
      writer.write("hoodie.upsert.shuffle.parallelism=2\n");
      writer.write("hoodie.insert.shuffle.parallelism=2\n");
      writer.write("hoodie.streamer.schemaprovider.source.schema.file=" + schemaFile.makeQualified(storage.getUri()).toUri().toString() + "\n");
      writer.write("hoodie.streamer.schemaprovider.target.schema.file=" + schemaFile.makeQualified(storage.getUri()).toUri().toString() + "\n");
      writer.write("hoodie.metadata.index.column.stats.enable=true\n");
    }


    // generate extra data that we can use to ingest with latest hudi in legacy mode
    for (int i = 0; i < 10; i++) {
      StoragePath dataCheckpointDir = new StoragePath(dataDirectory, "data_" + i);
      StoragePath dataFile = new StoragePath(dataCheckpointDir, "data.json");
      List<String> records = recordsToStrings(i == 0
          ? datagen.generateInsertsAsPerSchema("00" + i, 20, schemaStr)
          : datagen.generateUniqueUpdatesAsPerSchema("00" + i, 10, schemaStr));
      try (Writer writer = new OutputStreamWriter(storage.create(dataFile))) {
        for (String record : records) {
          writer.write(record);
          writer.write("\n");
        }
      }
    }
    StoragePath scriptFile = new StoragePath(directory, "runscript.sh");
    generateDsScript(scriptFile, 6);
  }
}