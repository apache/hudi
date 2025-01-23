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

package org.apache.hudi.cli.utils;

import org.apache.hudi.SparkAdapterSupport$;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieJsonPayload;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Loads data from Parquet Sources. This class is moved to hudi-cli from hudi-utilities.
 * This feature, as separate utility, was deprecated in 0.10.0 and should be removed in 0.11.0.
 * Now it is used only by hudi-cli SparkMain IMPORT and UPSERT commands (for compatibility support of hudi-cli API).
 */
public class HDFSParquetImporter implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(HDFSParquetImporter.class);

  private static final DateTimeFormatter PARTITION_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd")
      .withZone(ZoneId.systemDefault());
  private final Config cfg;
  private transient FileSystem fs;
  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  private TypedProperties props;

  public HDFSParquetImporter(Config cfg) {
    this.cfg = cfg;
  }

  private boolean isUpsert() {
    return "upsert".equalsIgnoreCase(cfg.command);
  }

  public int dataImport(JavaSparkContext jsc, int retry) {
    this.fs = HadoopFSUtils.getFs(cfg.targetPath, jsc.hadoopConfiguration());
    this.props = cfg.propsFilePath == null ? UtilHelpers.buildProperties(cfg.configs)
        : UtilHelpers.readConfig(fs.getConf(), new Path(cfg.propsFilePath), cfg.configs).getProps(true);
    LOG.info("Starting data import with configs : {}", props.toString());
    int ret = -1;
    try {
      // Verify that targetPath is not present.
      if (fs.exists(new Path(cfg.targetPath)) && !isUpsert()) {
        throw new HoodieIOException(String.format("Make sure %s is not present.", cfg.targetPath));
      }
      do {
        ret = dataImport(jsc);
      } while (ret != 0 && retry-- > 0);
    } catch (Throwable t) {
      LOG.error("Import data error", t);
    }
    return ret;
  }

  protected int dataImport(JavaSparkContext jsc) {
    try {
      if (fs.exists(new Path(cfg.targetPath)) && !isUpsert()) {
        // cleanup target directory.
        fs.delete(new Path(cfg.targetPath), true);
      }

      if (!fs.exists(new Path(cfg.targetPath))) {
        // Initialize target hoodie table.
        HoodieTableMetaClient.newTableBuilder()
            .setTableName(cfg.tableName)
            .setTableType(cfg.tableType)
            .setTableVersion(cfg.tableVersion)
            .initTable(HadoopFSUtils.getStorageConfWithCopy(jsc.hadoopConfiguration()), cfg.targetPath);
      }

      // Get schema.
      String schemaStr = UtilHelpers.parseSchema(fs, cfg.schemaFile);

      SparkRDDWriteClient<HoodieRecordPayload> client =
          UtilHelpers.createHoodieClient(jsc, cfg.targetPath, schemaStr, cfg.parallelism, Option.empty(), props);

      JavaRDD<HoodieRecord<HoodieRecordPayload>> hoodieRecords = buildHoodieRecordsForImport(jsc, schemaStr);
      // Get instant time.
      String instantTime = client.startCommit();
      JavaRDD<WriteStatus> writeResponse = load(client, instantTime, hoodieRecords);
      return UtilHelpers.handleErrors(jsc, instantTime, writeResponse);
    } catch (Throwable t) {
      LOG.error("Error occurred.", t);
    }
    return -1;
  }

  protected JavaRDD<HoodieRecord<HoodieRecordPayload>> buildHoodieRecordsForImport(JavaSparkContext jsc,
      String schemaStr) throws IOException {
    Job job = Job.getInstance(jsc.hadoopConfiguration());
    // Allow recursive directories to be found
    job.getConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    // To parallelize reading file status.
    job.getConfiguration().set(FileInputFormat.LIST_STATUS_NUM_THREADS, "1024");
    AvroReadSupport.setAvroReadSchema(jsc.hadoopConfiguration(), (new Schema.Parser().parse(schemaStr)));
    ParquetInputFormat.setReadSupportClass(job, (AvroReadSupport.class));

    HoodieEngineContext context = new HoodieSparkEngineContext(jsc);
    context.setJobStatus(this.getClass().getSimpleName(), "Build records for import: " + cfg.tableName);
    return jsc.newAPIHadoopFile(cfg.srcPath, ParquetInputFormat.class, Void.class, GenericRecord.class,
            job.getConfiguration())
        // To reduce large number of tasks.
        .coalesce(16 * cfg.parallelism).map(entry -> {
          GenericRecord genericRecord = ((scala.Tuple2<Void, GenericRecord>) entry)._2();
          Object partitionField = genericRecord.get(cfg.partitionKey);
          if (partitionField == null) {
            throw new HoodieIOException("partition key is missing:" + cfg.partitionKey);
          }
          Object rowField = genericRecord.get(cfg.rowKey);
          if (rowField == null) {
            throw new HoodieIOException("row key field is missing:" + cfg.rowKey);
          }
          String partitionPath = partitionField.toString();
          LOG.debug("Row Key : {}, Partition Path is ({})", rowField, partitionPath);
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
   * @param client Hoodie Client
   * @param instantTime Instant Time
   * @param hoodieRecords Hoodie Records
   * @param <T> Type
   */
  protected <T extends HoodieRecordPayload> JavaRDD<WriteStatus> load(SparkRDDWriteClient<T> client, String instantTime,
                                                                      JavaRDD<HoodieRecord<T>> hoodieRecords) {
    switch (cfg.command.toLowerCase()) {
      case "upsert": {
        return client.upsert(hoodieRecords, instantTime);
      }
      case "bulkinsert": {
        return client.bulkInsert(hoodieRecords, instantTime);
      }
      default: {
        return client.insert(hoodieRecords, instantTime);
      }
    }
  }

  public static class Config implements Serializable {

    public String command = "INSERT"; // Write command Valid values are insert(default)/upsert/bulkinsert
    public String srcPath = null; // Base path for the input table
    public String targetPath = null; // Base path for the target hoodie table
    public String tableName = null; // Table name
    public String tableType = null; // Table type
    public int tableVersion = HoodieTableVersion.current().versionCode(); // Table version
    public String rowKey = null; // Row key field name
    public String partitionKey = null; // Partition key field name
    public int parallelism = 1; // Parallelism for hoodie insert(default)/upsert/bulkinsert
    public String schemaFile = null; // path for Avro schema file
    public String format = null; // Format for the input data
    public String propsFilePath = null; // path to properties file on localfs or dfs, with configurations for hoodie client for importing
    public List<String> configs = new ArrayList<>(); // Any configuration that can be set in the properties file
    // can also be passed command line using this parameter. This can be repeated
  }
}
