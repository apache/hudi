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

package org.apache.hudi.cli;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieJsonPayload;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hudi.table.action.compact.strategy.CompactionStrategy;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hudi.common.util.StringUtils.fromUTF8Bytes;

/**
 * Loads data from Parquet Sources.
 */
public class HDFSParquetImporterUtils implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HDFSParquetImporterUtils.class);
  private static final DateTimeFormatter PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd")
      .withZone(ZoneId.systemDefault());

  private final String command;
  private final String srcPath;
  private final String targetPath;
  private final String tableName;
  private final String tableType;
  private final String rowKey;
  private final String partitionKey;
  private final int parallelism;
  private final String schemaFile;
  private int retry;
  private final String propsFilePath;
  private final List<String> configs = new ArrayList<>();
  private TypedProperties props;

  public HDFSParquetImporterUtils(
      String command,
      String srcPath,
      String targetPath,
      String tableName,
      String tableType,
      String rowKey,
      String partitionKey,
      int parallelism,
      String schemaFile,
      int retry,
      String propsFilePath) {
    this.command = command;
    this.srcPath = srcPath;
    this.targetPath = targetPath;
    this.tableName = tableName;
    this.tableType = tableType;
    this.rowKey = rowKey;
    this.partitionKey = partitionKey;
    this.parallelism = parallelism;
    this.schemaFile = schemaFile;
    this.retry = retry;
    this.propsFilePath = propsFilePath;
  }

  public boolean isUpsert() {
    return "upsert".equalsIgnoreCase(this.command);
  }

  public int dataImport(JavaSparkContext jsc) {
    FileSystem fs = HadoopFSUtils.getFs(this.targetPath, jsc.hadoopConfiguration());
    this.props = this.propsFilePath == null || this.propsFilePath.isEmpty() ? buildProperties(this.configs)
        : readConfig(fs.getConf(), new StoragePath(this.propsFilePath), this.configs).getProps(true);
    LOG.info("Starting data import with configs : " + props.toString());
    int ret = -1;
    try {
      // Verify that targetPath is not present.
      if (fs.exists(new Path(this.targetPath)) && !isUpsert()) {
        throw new HoodieIOException(String.format("Make sure %s is not present.", this.targetPath));
      }
      do {
        ret = dataImport(jsc, fs);
      } while (ret != 0 && retry-- > 0);
    } catch (Throwable t) {
      LOG.error("dataImport failed", t);
    }
    return ret;
  }

  public int dataImport(JavaSparkContext jsc, FileSystem fs) {
    try {
      if (fs.exists(new Path(this.targetPath)) && !isUpsert()) {
        // cleanup target directory.
        fs.delete(new Path(this.targetPath), true);
      }

      if (!fs.exists(new Path(this.targetPath))) {
        // Initialize target hoodie table.
        HoodieTableMetaClient.newTableBuilder()
            .setTableName(this.tableName)
            .setTableType(this.tableType)
            .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), this.targetPath);
      }

      // Get schema.
      String schemaStr = parseSchema(fs, this.schemaFile);

      SparkRDDWriteClient<HoodieRecordPayload> client =
          createHoodieClient(jsc, this.targetPath, schemaStr, this.parallelism, Option.empty(), props);

      JavaRDD<HoodieRecord<HoodieRecordPayload>> hoodieRecords = buildHoodieRecordsForImport(jsc, schemaStr);
      // Get instant time.
      String instantTime = client.startCommit();
      JavaRDD<WriteStatus> writeResponse = load(client, instantTime, hoodieRecords);
      return handleErrors(jsc, instantTime, writeResponse);
    } catch (Throwable t) {
      LOG.error("Error occurred.", t);
    }
    return -1;
  }

  public JavaRDD<HoodieRecord<HoodieRecordPayload>> buildHoodieRecordsForImport(JavaSparkContext jsc,
                                                                                String schemaStr) throws IOException {
    Job job = Job.getInstance(jsc.hadoopConfiguration());
    // Allow recursive directories to be found
    job.getConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    // To parallelize reading file status.
    job.getConfiguration().set(FileInputFormat.LIST_STATUS_NUM_THREADS, "1024");
    AvroReadSupport.setAvroReadSchema(jsc.hadoopConfiguration(), (new Schema.Parser().parse(schemaStr)));
    ParquetInputFormat.setReadSupportClass(job, (AvroReadSupport.class));

    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    context.setJobStatus(this.getClass().getSimpleName(), "Build records for import: " + this.tableName);
    return jsc.newAPIHadoopFile(this.srcPath, ParquetInputFormat.class, Void.class, GenericRecord.class,
            job.getConfiguration())
        // To reduce large number of tasks.
        .coalesce(16 * this.parallelism).map(entry -> {
          GenericRecord genericRecord = ((scala.Tuple2<Void, GenericRecord>) entry)._2();
          Object partitionField = genericRecord.get(this.partitionKey);
          if (partitionField == null) {
            throw new HoodieIOException("partition key is missing. :" + this.partitionKey);
          }
          Object rowField = genericRecord.get(this.rowKey);
          if (rowField == null) {
            throw new HoodieIOException("row field is missing. :" + this.rowKey);
          }
          String partitionPath = partitionField.toString();
          LOG.debug("Row Key : {}, Partition Path is ({}})", rowField, partitionPath);
          if (partitionField instanceof Number) {
            try {
              long ts = (long) (Double.parseDouble(partitionField.toString()) * 1000L);
              partitionPath = PARTITION_FORMATTER.format(Instant.ofEpochMilli(ts));
            } catch (NumberFormatException nfe) {
              LOG.warn("Unable to parse date from partition field. Assuming partition as ({})", partitionField);
            }
          }
          return new HoodieAvroRecord<>(new HoodieKey(rowField.toString(), partitionPath),
              new HoodieJsonPayload(genericRecord.toString()));
        });
  }

  /**
   * Imports records to Hoodie table.
   *
   * @param client        Hoodie Client
   * @param instantTime   Instant Time
   * @param hoodieRecords Hoodie Records
   * @param <T>           Type
   */
  public <T extends HoodieRecordPayload> JavaRDD<WriteStatus> load(SparkRDDWriteClient<T> client, String instantTime,
                                                                   JavaRDD<HoodieRecord<T>> hoodieRecords) {
    switch (this.command.toLowerCase()) {
      case "upsert": {
        JavaRDD<WriteStatus> writeStatusJavaRDD = client.upsert(hoodieRecords, instantTime);
        client.commit(instantTime, writeStatusJavaRDD);
        return writeStatusJavaRDD;
      }
      case "bulkinsert": {
        JavaRDD<WriteStatus> writeStatusJavaRDD = client.bulkInsert(hoodieRecords, instantTime);
        client.commit(instantTime, writeStatusJavaRDD);
        return writeStatusJavaRDD;
      }
      default: {
        JavaRDD<WriteStatus> writeStatusJavaRDD = client.insert(hoodieRecords, instantTime);
        client.commit(instantTime, writeStatusJavaRDD);
        return writeStatusJavaRDD;
      }
    }
  }

  public static TypedProperties buildProperties(List<String> props) {
    TypedProperties properties = DFSPropertiesConfiguration.getGlobalProps();
    props.forEach(x -> {
      String[] kv = x.split("=");
      ValidationUtils.checkArgument(kv.length == 2);
      properties.setProperty(kv[0], kv[1]);
    });
    return properties;
  }

  public static DFSPropertiesConfiguration readConfig(Configuration hadoopConfig, StoragePath cfgPath, List<String> overriddenProps) {
    DFSPropertiesConfiguration conf = new DFSPropertiesConfiguration(new HadoopStorageConfiguration(hadoopConfig), cfgPath);
    try {
      if (!overriddenProps.isEmpty()) {
        LOG.info("Adding overridden properties to file properties.");
        conf.addPropsFromStream(new BufferedReader(new StringReader(String.join("\n", overriddenProps))), cfgPath);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  /**
   * Build Hoodie write client.
   *
   * @param jsc         Java Spark Context
   * @param basePath    Base Path
   * @param schemaStr   Schema
   * @param parallelism Parallelism
   */
  public static SparkRDDWriteClient<HoodieRecordPayload> createHoodieClient(JavaSparkContext jsc, String basePath, String schemaStr,
                                                                            int parallelism, Option<String> compactionStrategyClass, TypedProperties properties) {
    Option<CompactionStrategy> strategyOpt = compactionStrategyClass.map(ReflectionUtils::loadClass);
    HoodieCompactionConfig compactionConfig = strategyOpt
        .map(strategy -> HoodieCompactionConfig.newBuilder().withInlineCompaction(false)
            .withCompactionStrategy(strategy).build())
        .orElseGet(() -> HoodieCompactionConfig.newBuilder().withInlineCompaction(false).build());
    HoodieWriteConfig config =
        HoodieWriteConfig.newBuilder().withPath(basePath)
            .withParallelism(parallelism, parallelism)
            .withBulkInsertParallelism(parallelism)
            .withDeleteParallelism(parallelism)
            .withSchema(schemaStr).combineInput(true, true).withCompactionConfig(compactionConfig)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
            .withProps(properties).build();
    return new SparkRDDWriteClient<>(new HoodieSparkEngineContext(jsc), config);
  }

  /**
   * Parse Schema from file.
   *
   * @param fs         File System
   * @param schemaFile Schema File
   */
  public static String parseSchema(FileSystem fs, String schemaFile) throws Exception {
    // Read schema file.
    Path p = new Path(schemaFile);
    if (!fs.exists(p)) {
      throw new Exception(String.format("Could not find - %s - schema file.", schemaFile));
    }
    long len = fs.getFileStatus(p).getLen();
    ByteBuffer buf = ByteBuffer.allocate((int) len);
    try (FSDataInputStream inputStream = fs.open(p)) {
      inputStream.readFully(0, buf.array(), 0, buf.array().length);
    }
    return fromUTF8Bytes(buf.array());
  }

  public static int handleErrors(JavaSparkContext jsc, String instantTime, JavaRDD<WriteStatus> writeResponse) {
    LongAccumulator errors = jsc.sc().longAccumulator();
    writeResponse.foreach(writeStatus -> {
      if (writeStatus.hasErrors()) {
        errors.add(1);
        LOG.error("Error processing records :writeStatus:{}", writeStatus.getStat());
      }
    });
    if (errors.value() == 0) {
      LOG.info("Table imported into hoodie with {} instant time.", instantTime);
      return 0;
    }
    LOG.error("Import failed with {} errors.", errors.value());
    return -1;
  }
}
