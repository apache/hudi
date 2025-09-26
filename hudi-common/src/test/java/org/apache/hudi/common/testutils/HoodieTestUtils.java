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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.HoodieWriteStat.RuntimeStats;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.CommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantComparator;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.InstantFileNameParser;
import org.apache.hudi.common.table.timeline.InstantGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.DefaultCommitMetadataSerDe;
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantComparator;
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantFileNameParser;
import org.apache.hudi.common.table.timeline.versioning.DefaultInstantGenerator;
import org.apache.hudi.common.table.timeline.versioning.DefaultTimelineFactory;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.jupiter.api.Assumptions;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.apache.hudi.common.testutils.SchemaTestUtil.getSchemaFromResource;
import static org.apache.hudi.storage.HoodieStorageUtils.DEFAULT_URI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
  public static final InstantGenerator INSTANT_GENERATOR = new DefaultInstantGenerator();
  public static final TimelineFactory TIMELINE_FACTORY = new DefaultTimelineFactory();
  public static final InstantFileNameGenerator INSTANT_FILE_NAME_GENERATOR = new DefaultInstantFileNameGenerator();
  public static final InstantFileNameParser INSTANT_FILE_NAME_PARSER = new DefaultInstantFileNameParser();
  public static final CommitMetadataSerDe COMMIT_METADATA_SER_DE = new DefaultCommitMetadataSerDe();
  public static final InstantComparator INSTANT_COMPARATOR = new DefaultInstantComparator();
  public static final Schema SIMPLE_RECORD_SCHEMA = getSchemaFromResource(HoodieTestUtils.class, "/exampleSchema.avsc", false);

  public static HoodieAvroIndexedRecord createSimpleRecord(String rowKey, String time, Integer number) {
    return createSimpleRecord(rowKey, time, number, Option.empty());
  }

  public static HoodieAvroIndexedRecord createSimpleRecord(String rowKey, String time, Integer number, Option<String> partitionPath) {
    GenericRecord record = new GenericData.Record(SIMPLE_RECORD_SCHEMA);
    record.put("_row_key", rowKey);
    record.put("time", time);
    record.put("number", number);
    String partition = partitionPath.orElseGet(() -> extractPartitionFromTimeField(time));
    return new HoodieAvroIndexedRecord(new HoodieKey(rowKey, partition), record, null, Option.of(Collections.singletonMap("InputRecordCount_1506582000", "2")), null, null);
  }

  public static String extractPartitionFromTimeField(String timeField) {
    return timeField.split("T")[0].replace("-", "/");
  }

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
    return init(basePath, tableType, bootstrapBasePath, bootstrapIndexEnable, keyGenerator, partitionFieldConfigValue, Option.empty());
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath, boolean bootstrapIndexEnable, String keyGenerator,
                                           String partitionFieldConfigValue, Option<HoodieTableVersion> tableVersionOption) throws IOException {
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
    if (tableVersionOption.isPresent()) {
      HoodieTableVersion tableVersion = tableVersionOption.get();
      props.put("hoodie.table.version", Integer.valueOf(tableVersion.versionCode()).toString());
      props.put("hoodie.timeline.layout.version", tableVersion.getTimelineLayoutVersion().getVersion());
    }
    return init(getDefaultStorageConf(), basePath, tableType, props);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, String bootstrapBasePath, boolean bootstrapIndexEnable) throws IOException {
    return init(basePath, tableType, bootstrapBasePath, bootstrapIndexEnable, null);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieFileFormat baseFileFormat) throws IOException {
    return init(getDefaultStorageConf(), basePath, HoodieTableType.COPY_ON_WRITE, baseFileFormat);
  }

  public static HoodieTableMetaClient init(String basePath, HoodieTableType tableType, HoodieTableVersion version) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.VERSION.key(), String.valueOf(version.versionCode()));
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition");
    return init(getDefaultStorageConf(), basePath, tableType, properties);
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
    return getMetaClientBuilder(tableType, properties, databaseName).initTable(storageConf.newInstance(), basePath);
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
    return getMetaClientBuilder(tableType, properties, null).initTable(storageConf.newInstance(), basePath);
  }

  public static HoodieTableMetaClient.TableBuilder getMetaClientBuilder(HoodieTableType tableType, Properties properties, String databaseName) {
    HoodieTableMetaClient.TableBuilder builder =
        HoodieTableMetaClient.newTableBuilder()
            .setDatabaseName(databaseName)
            .setTableName(RAW_TRIPS_TEST_NAME)
            .setTableType(tableType);

    if (properties.getProperty(HoodieTableConfig.KEY_GENERATOR_TYPE.key()) != null) {
      builder.setKeyGeneratorType(properties.getProperty(HoodieTableConfig.KEY_GENERATOR_TYPE.key()));
    }

    if (properties.containsKey("hoodie.write.table.version")) {
      builder.setTableVersion(Integer.parseInt(properties.getProperty("hoodie.write.table.version")));
    }
    if (properties.containsKey(HoodieTableConfig.TABLE_FORMAT.key())) {
      builder.setTableFormat(properties.getProperty(HoodieTableConfig.TABLE_FORMAT.key()));
    }

    String keyGen = properties.getProperty("hoodie.datasource.write.keygenerator.class");
    if (!Objects.equals(keyGen, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        && !properties.containsKey("hoodie.datasource.write.partitionpath.field")) {
      builder.setPartitionFields("partition_path");
    }
    builder.fromProperties(properties);
    return builder;
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
                                                       StoragePath basePath, HoodieTableVersion tableVersion) {
    return HoodieTableMetaClient.builder().setLayoutVersion(Option.of(tableVersion.getTimelineLayoutVersion()))
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

  public static List<String> getLogFileListFromFileSlice(FileSlice fileSlice) {
    return fileSlice.getLogFiles().map(logFile -> logFile.getPath().toString())
        .collect(Collectors.toList());
  }

  public static HoodieInstant getCompleteInstant(HoodieStorage storage, StoragePath parent,
                                                 String instantTime, String action) {
    return INSTANT_GENERATOR.createNewInstant(getCompleteInstantFileInfo(storage, parent, instantTime, action));
  }

  public static StoragePath getCompleteInstantPath(HoodieStorage storage, StoragePath parent,
                                                   String instantTime, String action) {
    return getCompleteInstantPath(storage, parent, instantTime, action, HoodieTableVersion.current());
  }

  public static StoragePath getCompleteInstantPath(HoodieStorage storage, StoragePath parent,
                                                   String instantTime, String action, HoodieTableVersion tableVersion) {
    return getCompleteInstantFileInfo(storage, parent, instantTime, action, tableVersion).getPath();
  }

  public static <T> byte[] convertMetadataToByteArray(T metadata) {
    return TimelineMetadataUtils.convertMetadataToByteArray(metadata, COMMIT_METADATA_SER_DE);
  }

  private static StoragePathInfo getCompleteInstantFileInfo(HoodieStorage storage,
                                                            StoragePath parent,
                                                            String instantTime, String action) {
    return getCompleteInstantFileInfo(storage, parent, instantTime, action, HoodieTableVersion.current());
  }

  private static StoragePathInfo getCompleteInstantFileInfo(HoodieStorage storage,
                                                            StoragePath parent,
                                                            String instantTime, String action,
                                                            HoodieTableVersion tableVersion) {
    try {
      String actionSuffix = "." + action;
      StoragePath wildcardPath = new StoragePath(parent, tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT) ? instantTime + "_*" + actionSuffix :  instantTime + actionSuffix);
      List<StoragePathInfo> pathInfoList = storage.globEntries(wildcardPath);
      if (pathInfoList.size() != 1) {
        throw new IOException("Error occur when finding path " + wildcardPath);
      }

      return pathInfoList.get(0);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to get instant file status", e);
    }
  }

  /**
   * Gets the pair of partition and cleaned file from the clean metadata.
   */
  public static List<Pair<String, String>> getCleanedFiles(HoodieTableMetaClient metaClient, HoodieInstant cleanInstant) throws IOException {
    HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(metaClient, cleanInstant);
    List<Pair<String, String>> deleteFileList = new ArrayList<>();
    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getDeletePathPatterns();
      deletedFiles.forEach(entry -> deleteFileList.add(Pair.of(partition, entry)));
    });
    return deleteFileList;
  }

  /**
   * Extracts a ZIP file from resources to a target directory.
   * 
   * @param resourcePath the path to the ZIP resource (relative to classpath)
   * @param targetDirectory the target directory to extract files to
   * @param resourceClass the class to use for resource loading
   * @throws IOException if extraction fails
   */
  public static void extractZipToDirectory(String resourcePath, Path targetDirectory, Class<?> resourceClass) throws IOException {
    InputStream resourceStream = resourceClass.getClassLoader().getResourceAsStream(resourcePath);
    if (resourceStream == null) {
      // Fallback to getResourceAsStream if getClassLoader().getResourceAsStream() fails
      resourceStream = resourceClass.getResourceAsStream(resourcePath);
    }
    
    if (resourceStream == null) {
      throw new IOException("Resource not found at: " + resourcePath);
    }

    try (ZipInputStream zip = new ZipInputStream(resourceStream)) {
      ZipEntry entry;
      while ((entry = zip.getNextEntry()) != null) {
        File file = targetDirectory.resolve(entry.getName()).toFile();
        if (entry.isDirectory()) {
          file.mkdirs();
          continue;
        }
        
        // Create parent directories if they don't exist
        file.getParentFile().mkdirs();
        
        // Extract file content
        byte[] buffer = new byte[10000];
        try (BufferedOutputStream out = new BufferedOutputStream(Files.newOutputStream(file.toPath()))) {
          int count;
          while ((count = zip.read(buffer)) != -1) {
            out.write(buffer, 0, count);
          }
        }
      }
    }
  }

  public static void validateTableConfig(HoodieStorage storage,
                                         String basePath,
                                         Map<String, String> expectedConfigs,
                                         List<String> nonExistentConfigs) {
    HoodieTableConfig tableConfig = HoodieTableConfig.loadFromHoodieProps(storage, basePath);
    expectedConfigs.forEach((key, value) -> {
      String actual = tableConfig.getString(key);
      assertEquals(value, actual,
          String.format("Table config %s should be %s but is %s}",
              key, value, actual));
    });
    nonExistentConfigs.forEach(key -> assertFalse(
        tableConfig.contains(key), key + " should not be present in the table config"));
  }
}
