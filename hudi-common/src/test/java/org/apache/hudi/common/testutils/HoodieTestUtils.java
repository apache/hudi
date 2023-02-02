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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.metadata.HoodieTableMetadata;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

/**
 * A utility class for testing.
 */
public class HoodieTestUtils {

  public static final String HOODIE_DATABASE = "test_incremental";
  public static final String RAW_TRIPS_TEST_NAME = "raw_trips";
  public static final String DEFAULT_WRITE_TOKEN = "1-0-1";
  public static final int DEFAULT_LOG_VERSION = 1;
  public static final String[] DEFAULT_PARTITION_PATHS = {"2016/03/15", "2015/03/16", "2015/03/17"};

  public static Configuration getDefaultHadoopConf() {
    return new Configuration();
  }

  public static HoodieTableMetaClient init(String basePath) throws IOException {
    return init(basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType) throws IOException {
    return init(getDefaultHadoopConf(), basePath, tableType);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, Properties properties) throws IOException {
    return init(getDefaultHadoopConf(), basePath, tableType, properties);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath, boolean bootstrapIndexEnable) throws IOException {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.BOOTSTRAP_BASE_PATH.key(), bootstrapBasePath);
    props.put(HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE.key(), bootstrapIndexEnable);
    return init(getDefaultHadoopConf(), basePath, tableType, props);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieFileFormat baseFileFormat) throws IOException {
    return init(getDefaultHadoopConf(), basePath, HoodieTableType.COPY_ON_WRITE, baseFileFormat);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath) throws IOException {
    return init(hadoopConf, basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType)
      throws IOException {
    return init(hadoopConf, basePath, tableType, new Properties());
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           String tableName)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.NAME.key(), tableName);
    return init(hadoopConf, basePath, tableType, properties);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           HoodieFileFormat baseFileFormat, String databaseName)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.toString());
    return init(hadoopConf, basePath, tableType, properties, databaseName);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           HoodieFileFormat baseFileFormat) throws IOException {
    return init(hadoopConf, basePath, tableType, baseFileFormat, false, null, true);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           HoodieFileFormat baseFileFormat, boolean setKeyGen, String keyGenerator, boolean populateMetaFields)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.toString());
    if (setKeyGen) {
      properties.setProperty("hoodie.datasource.write.keygenerator.class", keyGenerator);
    }
    properties.setProperty("hoodie.populate.meta.fields", Boolean.toString(populateMetaFields));
    return init(hadoopConf, basePath, tableType, properties);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           Properties properties) throws IOException {
    return init(hadoopConf, basePath, tableType, properties, null);
  }

  public static HoodieTableMetaClient init(Configuration hadoopConf, String basePath, HoodieTableType tableType,
                                           Properties properties, String databaseName)
      throws IOException {
    HoodieTableMetaClient.PropertyBuilder builder =
        HoodieTableMetaClient.withPropertyBuilder()
            .setDatabaseName(databaseName)
            .setTableName(RAW_TRIPS_TEST_NAME)
            .setTableType(tableType)
            .setPayloadClass(HoodieAvroPayload.class);

    String keyGen = properties.getProperty("hoodie.datasource.write.keygenerator.class");
    if (!Objects.equals(keyGen, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")) {
      builder.setPartitionFields("some_nonexistent_field");
    }

    Properties processedProperties = builder.fromProperties(properties).build();

    return HoodieTableMetaClient.initTableAndGetMetaClient(hadoopConf, basePath, processedProperties);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath, HoodieFileFormat baseFileFormat) throws IOException {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.BOOTSTRAP_BASE_PATH.key(), bootstrapBasePath);
    props.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.name());
    return init(getDefaultHadoopConf(), basePath, tableType, props);
  }

  public static <T extends Serializable> T serializeDeserialize(T object, Class<T> clazz) {
    // Using Kryo as the default serializer in Spark Jobs
    Kryo kryo = new Kryo();
    kryo.register(HoodieTableMetaClient.class, new JavaSerializer());

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeObject(output, object);
    output.close();

    Input input = new Input(new ByteArrayInputStream(baos.toByteArray()));
    T deserializedObject = kryo.readObject(input, clazz);
    input.close();
    return deserializedObject;
  }

  public static List<HoodieWriteStat> generateFakeHoodieWriteStat(int limit) {
    List<HoodieWriteStat> writeStatList = new ArrayList<>();
    for (int i = 0; i < limit; i++) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setFileId(UUID.randomUUID().toString());
      writeStat.setNumDeletes(0);
      writeStat.setNumUpdateWrites(100);
      writeStat.setNumWrites(100);
      writeStat.setPath("/some/fake/path" + i);
      writeStat.setPartitionPath("/some/fake/partition/path" + i);
      writeStat.setTotalLogFilesCompacted(100L);
      RuntimeStats runtimeStats = new RuntimeStats();
      runtimeStats.setTotalScanTime(100);
      runtimeStats.setTotalCreateTime(100);
      runtimeStats.setTotalUpsertTime(100);
      writeStat.setRuntimeStats(runtimeStats);
      writeStatList.add(writeStat);
    }
    return writeStatList;
  }

  public static void createCompactionCommitInMetadataTable(
      Configuration hadoopConf, HoodieWrapperFileSystem wrapperFs, String basePath,
      String instantTime) throws IOException {
    // This is to simulate a completed compaction commit in metadata table timeline,
    // so that the commits on data table timeline can be archived
    // Note that, if metadata table is enabled, instants in data table timeline,
    // which are more recent than the last compaction on the metadata table,
    // are not archived (HoodieTimelineArchiveLog::getInstantsToArchive)
    String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    HoodieTestUtils.init(hadoopConf, metadataTableBasePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestDataGenerator.createCommitFile(metadataTableBasePath, instantTime + "001", wrapperFs.getConf());
  }
}
