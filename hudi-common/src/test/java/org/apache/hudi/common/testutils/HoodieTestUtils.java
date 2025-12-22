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

import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.Assumptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;

import static org.apache.hudi.storage.HoodieStorageUtils.DEFAULT_URI;

/**
 * A utility class for testing.
 */
public class HoodieTestUtils {

  public static final String HOODIE_DATABASE = "test_incremental";
  public static final String RAW_TRIPS_TEST_NAME = "raw_trips";
  public static final String DEFAULT_WRITE_TOKEN = "1-0-1";
  public static final int DEFAULT_LOG_VERSION = 1;
  public static final String[] DEFAULT_PARTITION_PATHS = {"2016/03/15", "2015/03/16", "2015/03/17"};
  public static final String HADOOP_STORAGE_CONF = "org.apache.hudi.storage.hadoop.HadoopStorageConfiguration";

  public static StorageConfiguration<Configuration> getDefaultStorageConf() {
    return (StorageConfiguration<Configuration>) ReflectionUtils.loadClass(HADOOP_STORAGE_CONF,
        new Class<?>[] {Boolean.class}, false);
  }

  public static StorageConfiguration<Configuration> getDefaultStorageConfWithDefaults() {
    return (StorageConfiguration<Configuration>) ReflectionUtils.loadClass(HADOOP_STORAGE_CONF,
        new Class<?>[] {Boolean.class}, true);
  }

  public static HoodieStorage getDefaultStorage() {
    return getStorage(DEFAULT_URI);
  }

  public static HoodieStorage getStorage(String path) {
    return HoodieStorageUtils.getStorage(path, getDefaultStorageConf());
  }

  public static HoodieStorage getStorage(StoragePath path) {
    return HoodieStorageUtils.getStorage(path, getDefaultStorageConf());
  }

  public static HoodieTableMetaClient init(String basePath) throws IOException {
    return init(basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType) throws IOException {
    return init(getDefaultStorageConf(), basePath, tableType);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, Properties properties) throws IOException {
    return init(getDefaultStorageConf(), basePath, tableType, properties);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath, boolean bootstrapIndexEnable, String keyGenerator) throws IOException {
    return init(basePath, tableType, bootstrapBasePath, bootstrapIndexEnable, keyGenerator, "datestr");
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath, boolean bootstrapIndexEnable, String keyGenerator,
                                             String partitionFieldConfigValue) throws IOException {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.BOOTSTRAP_BASE_PATH.key(), bootstrapBasePath);
    props.put(HoodieTableConfig.BOOTSTRAP_INDEX_ENABLE.key(), bootstrapIndexEnable);
    if (keyGenerator != null) {
      props.put("hoodie.datasource.write.keygenerator.class", keyGenerator);
    }
    if (keyGenerator != null && !keyGenerator.equals("org.apache.hudi.keygen.NonpartitionedKeyGenerator")) {
      props.put("hoodie.datasource.write.partitionpath.field", partitionFieldConfigValue);
      props.put(HoodieTableConfig.PARTITION_FIELDS.key(), partitionFieldConfigValue);
    }
    return init(getDefaultStorageConf(), basePath, tableType, props);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath, boolean bootstrapIndexEnable) throws IOException {
    return init(basePath, tableType, bootstrapBasePath, bootstrapIndexEnable, null);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieFileFormat baseFileFormat) throws IOException {
    return init(getDefaultStorageConf(), basePath, HoodieTableType.COPY_ON_WRITE, baseFileFormat);
  }

  public static HoodieTableMetaClient init(StorageConfiguration<?> storageConf, String basePath) throws IOException {
    return init(storageConf, basePath, HoodieTableType.COPY_ON_WRITE);
  }

  public static HoodieTableMetaClient init(StorageConfiguration<?> storageConf, String basePath, HoodieTableType tableType)
      throws IOException {
    return init(storageConf, basePath, tableType, new Properties());
  }

  public static HoodieTableMetaClient init(StorageConfiguration<?> storageConf, String basePath, HoodieTableType tableType,
                                           String tableName)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.NAME.key(), tableName);
    return init(storageConf, basePath, tableType, properties);
  }

  public static HoodieTableMetaClient init(StorageConfiguration<?> storageConf, String basePath, HoodieTableType tableType,
                                           HoodieFileFormat baseFileFormat, String databaseName)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.toString());
    return init(storageConf, basePath, tableType, properties, databaseName);
  }

  public static HoodieTableMetaClient init(StorageConfiguration<?> storageConf, String basePath, HoodieTableType tableType,
                                           HoodieFileFormat baseFileFormat) throws IOException {
    return init(storageConf, basePath, tableType, baseFileFormat, false, null, true);
  }

  public static HoodieTableMetaClient init(StorageConfiguration<?> storageConf, String basePath, HoodieTableType tableType,
                                           HoodieFileFormat baseFileFormat, boolean setKeyGen, String keyGenerator, boolean populateMetaFields)
      throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.toString());
    if (setKeyGen) {
      properties.setProperty("hoodie.datasource.write.keygenerator.class", keyGenerator);
    }
    properties.setProperty("hoodie.populate.meta.fields", Boolean.toString(populateMetaFields));
    return init(storageConf, basePath, tableType, properties);
  }

  public static HoodieTableMetaClient init(StorageConfiguration<?> storageConf, String basePath, HoodieTableType tableType,
                                           Properties properties) throws IOException {
    return init(storageConf, basePath, tableType, properties, null);
  }

  public static HoodieTableMetaClient init(StorageConfiguration<?> storageConf, String basePath, HoodieTableType tableType,
                                           Properties properties, String databaseName)
      throws IOException {
    HoodieTableMetaClient.PropertyBuilder builder =
        HoodieTableMetaClient.withPropertyBuilder()
            .setDatabaseName(databaseName)
            .setTableName(RAW_TRIPS_TEST_NAME)
            .setTableType(tableType)
            .setPayloadClass(HoodieAvroPayload.class);

    String keyGen = properties.getProperty("hoodie.datasource.write.keygenerator.class");
    if (!Objects.equals(keyGen, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        && !properties.containsKey("hoodie.datasource.write.partitionpath.field")) {
      builder.setPartitionFields("some_nonexistent_field");
    }

    Properties processedProperties = builder.fromProperties(properties).build();

    return HoodieTableMetaClient.initTableAndGetMetaClient(storageConf.newInstance(), basePath, processedProperties);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath, HoodieFileFormat baseFileFormat, String keyGenerator) throws IOException {
    Properties props = new Properties();
    props.setProperty(HoodieTableConfig.BOOTSTRAP_BASE_PATH.key(), bootstrapBasePath);
    props.setProperty(HoodieTableConfig.BASE_FILE_FORMAT.key(), baseFileFormat.name());
    if (keyGenerator != null) {
      props.put("hoodie.datasource.write.keygenerator.class", keyGenerator);
      props.put("hoodie.datasource.write.partitionpath.field", "datestr");
    }
    return init(getDefaultStorageConf(), basePath, tableType, props);
  }

  /**
   * @param storageConf storage configuration.
   * @param basePath    base path of the Hudi table.
   * @return a new {@link HoodieTableMetaClient} instance.
   */
  public static HoodieTableMetaClient createMetaClient(StorageConfiguration<?> storageConf,
                                                       String basePath) {
    return HoodieTableMetaClient.builder()
        .setConf(storageConf).setBasePath(basePath).build();
  }

  public static HoodieTableMetaClient createMetaClient(StorageConfiguration<?> storageConf,
                                                       StoragePath basePath) {
    return HoodieTableMetaClient.builder()
            .setConf(storageConf).setBasePath(basePath).build();
  }

  /**
   * @param storage  {@link HoodieStorage} instance.
   * @param basePath base path of the Hudi table.
   * @return a new {@link HoodieTableMetaClient} instance.
   */
  public static HoodieTableMetaClient createMetaClient(HoodieStorage storage,
                                                       String basePath) {
    return createMetaClient(storage.getConf().newInstance(), basePath);
  }

  /**
   * @param context  Hudi engine context.
   * @param basePath base path of the Hudi table.
   * @return a new {@link HoodieTableMetaClient} instance.
   */
  public static HoodieTableMetaClient createMetaClient(HoodieEngineContext context,
                                                       String basePath) {
    return createMetaClient(context.getStorageConf().newInstance(), basePath);
  }

  /**
   * @param basePath base path of the Hudi table.
   * @return a new {@link HoodieTableMetaClient} instance with default configuration for tests.
   */
  public static HoodieTableMetaClient createMetaClient(String basePath) {
    return createMetaClient(getDefaultStorageConf(), basePath);
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
      StorageConfiguration<?> storageConf, String basePath, String instantTime) throws IOException {
    // This is to simulate a completed compaction commit in metadata table timeline,
    // so that the commits on data table timeline can be archived
    // Note that, if metadata table is enabled, instants in data table timeline,
    // which are more recent than the last compaction on the metadata table,
    // are not archived (HoodieTimelineArchiveLog::getInstantsToArchive)
    String metadataTableBasePath = HoodieTableMetadata.getMetadataTableBasePath(basePath);
    HoodieTestUtils.init(storageConf, metadataTableBasePath, HoodieTableType.MERGE_ON_READ);
    HoodieTestDataGenerator.createCommitFile(metadataTableBasePath, instantTime + "001",
        storageConf);
  }

  public static int getJavaVersion() {
    String version = System.getProperty("java.version");
    if (version.startsWith("1.")) {
      version = version.substring(2, 3);
    } else {
      int dot = version.indexOf(".");
      if (dot != -1) {
        version = version.substring(0, dot);
      }
    }
    return Integer.parseInt(version);
  }

  public static boolean shouldUseExternalHdfs() {
    return getJavaVersion() == 11 || getJavaVersion() == 17;
  }

  public static DistributedFileSystem useExternalHdfs() throws IOException {
    // For Java 17, this unit test has to run in Docker
    // Need to set -Duse.external.hdfs=true in mvn command to run this test
    Assumptions.assumeTrue(Boolean.valueOf(System.getProperty("use.external.hdfs", "false")));
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://localhost:9000");
    conf.set("dfs.replication", "3");
    return (DistributedFileSystem) DistributedFileSystem.get(conf);
  }
}
