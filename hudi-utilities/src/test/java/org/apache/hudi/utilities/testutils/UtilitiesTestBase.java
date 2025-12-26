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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.common.testutils.minicluster.ZookeeperTestService;
import org.apache.hudi.common.util.AvroOrcUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TestAvroOrcUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.ddl.JDBCExecutor;
import org.apache.hudi.hive.ddl.QueryBasedDDLExecutor;
import org.apache.hudi.hive.testutils.HiveTestService;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.sources.TestDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hive.service.server.HiveServer2;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.recordToString;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_PASS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_USER;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_TABLE_NAME;

/**
 * Abstract test that provides a dfs & spark contexts.
 *
 */
public class UtilitiesTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(UtilitiesTestBase.class);
  @TempDir
  protected static java.nio.file.Path sharedTempDir;
  protected static FileSystem fs;
  protected static HoodieStorage storage;
  protected static String basePath;
  protected static HdfsTestService hdfsTestService;
  protected static MiniDFSCluster dfsCluster;
  protected static HiveServer2 hiveServer;
  protected static HiveTestService hiveTestService;
  protected static ZookeeperTestService zookeeperTestService;
  private static final ObjectMapper MAPPER = new ObjectMapper();

  protected static JavaSparkContext jsc;
  protected static HoodieSparkEngineContext context;
  protected static SparkSession sparkSession;
  protected static SQLContext sqlContext;
  protected static Configuration hadoopConf;

  @BeforeAll
  public static void setLogLevel() {
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
    rootLogger.setLevel(org.apache.log4j.Level.ERROR);
    org.apache.log4j.Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN);
  }

  public static void initTestServices() throws Exception {
    initTestServices(false, false, false);
  }

  public static void initTestServices(boolean needsHdfs, boolean needsHive, boolean needsZookeeper) throws Exception {
    hadoopConf = HoodieTestUtils.getDefaultStorageConf().unwrap();

    if (needsZookeeper) {
      zookeeperTestService = new ZookeeperTestService(hadoopConf);
      zookeeperTestService.start();
    }

    if (needsHdfs) {
      hdfsTestService = new HdfsTestService(hadoopConf);
      dfsCluster = hdfsTestService.start(true);
      fs = dfsCluster.getFileSystem();
      basePath = fs.getWorkingDirectory().toString();
      fs.mkdirs(new Path(basePath));
    } else {
      fs = FileSystem.getLocal(hadoopConf);
      basePath = sharedTempDir.toUri().toString();
    }
    storage = new HoodieHadoopStorage(fs);

    hadoopConf.set("hive.exec.scratchdir", basePath + "/.tmp/hive");
    if (needsHive) {
      hiveTestService = new HiveTestService(hadoopConf);
      hiveServer = hiveTestService.start();
      clearHiveDb(basePath + "/dummy" + System.currentTimeMillis());
    }

    jsc = UtilHelpers.buildSparkContext(UtilitiesTestBase.class.getName() + "-hoodie", "local[4,1]", sparkConf());
    context = new HoodieSparkEngineContext(jsc);
    sqlContext = SQLContext.getOrCreate(jsc.sc());
    sparkSession = SparkSession.builder().config(jsc.getConf()).getOrCreate();
  }

  @AfterAll
  public static void cleanUpUtilitiesTestServices() {
    List<String> failedReleases = new ArrayList<>();
    try {
      if (sparkSession != null) {
        sparkSession.close();
        sparkSession = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      failedReleases.add("SparkSession");
    }

    if (context != null) {
      context = null;
    }

    try {
      if (jsc != null) {
        jsc.stop();
        jsc = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      failedReleases.add("JSC");
    }

    try {
      if (hiveServer != null) {
        hiveServer.stop();
        hiveServer = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      failedReleases.add("HiveServer");
    }

    try {
      if (hiveTestService != null) {
        hiveTestService.stop();
        hiveTestService = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      failedReleases.add("HiveTestService");
    }

    try {
      if (fs != null) {
        fs.delete(new Path(basePath), true);
        fs.close();
        fs = null;
      }
    } catch (IOException ie) {
      ie.printStackTrace();
      failedReleases.add("FileSystem");
    }

    try {
      if (hdfsTestService != null) {
        hdfsTestService.stop();
        hdfsTestService = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      failedReleases.add("HdfsTestService");
    }

    try {
      if (zookeeperTestService != null) {
        zookeeperTestService.stop();
        zookeeperTestService = null;
      }
    } catch (Exception e) {
      e.printStackTrace();
      failedReleases.add("ZooKeeperTestService");
    }

    if (!failedReleases.isEmpty()) {
      LOG.error("Exception happened during releasing: " + String.join(",", failedReleases));
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    TestDataSource.initDataGen();
    // This prevents test methods from using existing files or folders.
    if (fs != null) {
      fs.delete(new Path(basePath), true);
    }
  }

  @AfterEach
  public void teardown() throws Exception {
    TestDataSource.resetDataGen();
  }

  private static Map<String, String> sparkConf() {
    Map<String, String> conf = new HashMap<>();
    conf.put("spark.default.parallelism", "2");
    conf.put("spark.sql.shuffle.partitions", "2");
    conf.put("spark.executor.memory", "1G");
    conf.put("spark.driver.memory", "1G");
    conf.put("spark.hadoop.mapred.output.compress", "true");
    conf.put("spark.ui.enable", "false");
    return conf;
  }

  /**
   * Helper to get hive sync config.
   * 
   * @param basePath
   * @param tableName
   * @return
   */
  protected static HiveSyncConfig getHiveSyncConfig(String basePath, String tableName) {
    Properties props = new Properties();
    props.setProperty(HIVE_URL.key(), hiveTestService.getJdbcHive2Url());
    props.setProperty(HIVE_USER.key(), "");
    props.setProperty(HIVE_PASS.key(), "");
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "testdb1");
    props.setProperty(META_SYNC_TABLE_NAME.key(), tableName);
    props.setProperty(META_SYNC_BASE_PATH.key(), basePath);
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "datestr");
    return new HiveSyncConfig(props);
  }

  /**
   * Initialize Hive DB.
   * 
   * @throws IOException
   */
  private static void clearHiveDb(String tempWriteablePath) throws Exception {
    // Create Dummy hive sync config
    HiveSyncConfig hiveSyncConfig = getHiveSyncConfig(tempWriteablePath, "dummy");
    hiveSyncConfig.setHadoopConf(hiveTestService.getHiveConf());
    HoodieTableMetaClient.newTableBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName(hiveSyncConfig.getString(META_SYNC_TABLE_NAME))
        .initTable(storage.getConf().newInstance(), hiveSyncConfig.getString(META_SYNC_BASE_PATH));

    QueryBasedDDLExecutor ddlExecutor = new JDBCExecutor(hiveSyncConfig);
    ddlExecutor.runSQL("drop database if exists " + hiveSyncConfig.getString(META_SYNC_DATABASE_NAME));
    ddlExecutor.runSQL("create database " + hiveSyncConfig.getString(META_SYNC_DATABASE_NAME));
    ddlExecutor.close();
  }

  public static class Helpers {

    // to get hold of resources bundled with jar
    private static ClassLoader classLoader = Helpers.class.getClassLoader();

    public static String readFile(String testResourcePath) {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(testResourcePath)));
      StringBuffer sb = new StringBuffer();
      reader.lines().forEach(line -> sb.append(line).append("\n"));
      return sb.toString();
    }

    public static String readFileFromAbsolutePath(String absolutePathForResource)
        throws IOException {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(new FileInputStream(absolutePathForResource)));
      StringBuffer sb = new StringBuffer();
      reader.lines().forEach(line -> sb.append(line).append("\n"));
      return sb.toString();
    }

    public static void copyToDFS(String testResourcePath, HoodieStorage storage, String targetPath)
        throws IOException {
      PrintStream os = new PrintStream(storage.create(new StoragePath(targetPath), true));
      os.print(readFile(testResourcePath));
      os.flush();
      os.close();
    }

    public static void copyToDFSFromAbsolutePath(String absolutePathForResource, FileSystem fs,
                                                 String targetPath)
        throws IOException {
      PrintStream os = new PrintStream(fs.create(new Path(targetPath), true));
      os.print(readFileFromAbsolutePath(absolutePathForResource));
      os.flush();
      os.close();
    }

    public static void deleteFileFromDfs(FileSystem fs, String targetPath) throws IOException {
      if (fs.exists(new Path(targetPath))) {
        fs.delete(new Path(targetPath), true);
      }
    }

    public static void savePropsToDFS(TypedProperties props, HoodieStorage storage, String targetPath) throws IOException {
      String[] lines = props.keySet().stream().map(k -> String.format("%s=%s", k, props.get(k))).toArray(String[]::new);
      saveStringsToDFS(lines, storage, targetPath);
    }

    public static void saveStringsToDFS(String[] lines, HoodieStorage storage, String targetPath) throws IOException {
      PrintStream os = new PrintStream(storage.create(new StoragePath(targetPath), true));
      for (String l : lines) {
        os.println(l);
      }
      os.flush();
      os.close();
    }

    /**
     * Converts the json records into CSV format and writes to a file.
     *
     * @param hasHeader  whether the CSV file should have a header line.
     * @param sep  the column separator to use.
     * @param lines  the records in JSON format.
     * @param fs  {@link FileSystem} instance.
     * @param targetPath  File path.
     * @throws IOException
     */
    public static void saveCsvToDFS(
        boolean hasHeader, char sep,
        String[] lines, FileSystem fs, String targetPath) throws IOException {
      Builder csvSchemaBuilder = CsvSchema.builder();

      ArrayNode arrayNode = MAPPER.createArrayNode();
      Arrays.stream(lines).forEachOrdered(
          line -> {
            try {
              arrayNode.add(MAPPER.readValue(line, ObjectNode.class));
            } catch (IOException e) {
              throw new HoodieIOException(
                  "Error converting json records into CSV format: " + e.getMessage());
            }
          });
      arrayNode.get(0).fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
      ObjectWriter csvObjWriter = new CsvMapper()
          .writerFor(JsonNode.class)
          .with(csvSchemaBuilder.setUseHeader(hasHeader).setColumnSeparator(sep).build());
      PrintStream os = new PrintStream(fs.create(new Path(targetPath), true));
      csvObjWriter.writeValue(os, arrayNode);
      os.flush();
      os.close();
    }

    public static void saveParquetToDFS(List<GenericRecord> records, Path targetFile) throws IOException {
      saveParquetToDFS(records, targetFile, HoodieTestDataGenerator.AVRO_SCHEMA);
    }

    public static void saveParquetToDFS(List<GenericRecord> records, Path targetFile, Schema schema) throws IOException {
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(targetFile)
          .withSchema(schema)
          .withConf(HoodieTestUtils.getDefaultStorageConf().unwrap())
          .withWriteMode(Mode.OVERWRITE)
          .build()) {
        for (GenericRecord record : records) {
          writer.write(record);
        }
      }
    }

    public static void saveORCToDFS(List<GenericRecord> records, Path targetFile) throws IOException {
      saveORCToDFS(records, targetFile, TestAvroOrcUtils.ORC_SCHEMA);
    }

    public static void saveORCToDFS(List<GenericRecord> records, Path targetFile, TypeDescription schema) throws IOException {
      OrcFile.WriterOptions options = OrcFile.writerOptions(
          HoodieTestUtils.getDefaultStorageConf().unwrap()).setSchema(schema);
      try (Writer writer = OrcFile.createWriter(targetFile, options)) {
        VectorizedRowBatch batch = schema.createRowBatch();
        for (GenericRecord record : records) {
          addAvroRecord(batch, record, schema);
          batch.size++;
          if (batch.size % records.size() == 0 || batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
            batch.size = 0;
          }
        }
        writer.addRowBatch(batch);
      }
    }

    public static void saveAvroToDFS(List<GenericRecord> records, Path targetFile) throws IOException {
      saveAvroToDFS(records,targetFile,HoodieTestDataGenerator.AVRO_SCHEMA);
    }

    public static void saveAvroToDFS(List<GenericRecord> records, Path targetFile, Schema schema) throws IOException {
      FileSystem fs = targetFile.getFileSystem(HoodieTestUtils.getDefaultStorageConf().unwrap());
      OutputStream output = fs.create(targetFile);
      try (DataFileWriter<IndexedRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter(schema)).create(schema, output)) {
        for (GenericRecord record : records) {
          dataFileWriter.append(record);
        }
      }
    }

    public static TypedProperties setupSchemaOnDFS() throws IOException {
      return setupSchemaOnDFS("streamer-config", "source.avsc");
    }

    public static TypedProperties setupSchemaOnDFS(String scope, String filename) throws IOException {
      UtilitiesTestBase.Helpers.copyToDFS(scope + "/" + filename, storage,
          basePath + "/" + filename);
      TypedProperties props = new TypedProperties();
      props.setProperty("hoodie.streamer.schemaprovider.source.schema.file", basePath + "/" + filename);
      return props;
    }

    public static TypedProperties setupSchemaOnDFSWithAbsoluteScope(String scope, String filename) throws IOException {
      UtilitiesTestBase.Helpers.copyToDFSFromAbsolutePath(scope + "/" + filename, fs, basePath + "/" + filename);
      TypedProperties props = new TypedProperties();
      props.setProperty("hoodie.streamer.schemaprovider.source.schema.file", basePath + "/" + filename);
      return props;
    }

    public static List<GenericRecord> toGenericRecords(List<HoodieRecord> hoodieRecords) {
      return toGenericRecords(hoodieRecords, HoodieTestDataGenerator.AVRO_SCHEMA);
    }

    public static List<GenericRecord> toGenericRecords(List<HoodieRecord> hoodieRecords, Schema schema) {
      List<GenericRecord> records = new ArrayList<>();
      for (HoodieRecord hoodieRecord : hoodieRecords) {
        records.add((GenericRecord) hoodieRecord.getData());
      }
      return records;
    }

    public static String[] jsonifyRecords(List<HoodieRecord> records) {
      return records.stream().map(HoodieTestDataGenerator::recordToString).filter(Option::isPresent).map(Option::get).toArray(String[]::new);
    }

    public static Tuple2<String, String>[] jsonifyRecordsByPartitions(List<HoodieRecord> records, int partitions) {
      Tuple2<String, String>[] data = new Tuple2[records.size()];
      for (int i = 0; i < records.size(); i++) {
        int key = i % partitions;
        String value = recordToString(records.get(i)).get();
        data[i] = new Tuple2<>(Long.toString(key), value);
      }
      return data;
    }

    public static Tuple2<String, String>[] jsonifyRecordsByPartitionsWithNullKafkaKey(List<HoodieRecord> records, int partitions) {
      Tuple2<String, String>[] data = new Tuple2[records.size()];
      for (int i = 0; i < records.size(); i++) {
        String value = recordToString(records.get(i)).get();
        data[i] = new Tuple2<>(null, value);
      }
      return data;
    }

    private static void addAvroRecord(
            VectorizedRowBatch batch,
            GenericRecord record,
            TypeDescription orcSchema
    ) {
      for (int c = 0; c < batch.numCols; c++) {
        ColumnVector colVector = batch.cols[c];
        final String thisField = orcSchema.getFieldNames().get(c);
        final TypeDescription type = orcSchema.getChildren().get(c);

        Object fieldValue = record.get(thisField);
        HoodieSchemaField field = HoodieSchema.fromAvroSchema(record.getSchema()).getField(thisField)
            .orElseThrow(() -> new HoodieSchemaException("Could not find field: " + thisField));
        AvroOrcUtils.addToVector(type, colVector, field.schema(), fieldValue, batch.size);
      }
    }
  }
}
