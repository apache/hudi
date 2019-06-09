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

package com.uber.hoodie.utilities;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.HoodieJsonPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableConfig;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Loads data from Parquet Sources
 */
public class HDFSParquetImporter implements Serializable {
  private static volatile Logger log = LogManager.getLogger(HDFSParquetImporter.class);

  public static final SimpleDateFormat PARTITION_FORMATTER = new SimpleDateFormat("yyyy/MM/dd");
  private static volatile Logger logger = LogManager.getLogger(HDFSParquetImporter.class);
  private final Config cfg;
  private transient FileSystem fs;
  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  private TypedProperties props;

  public HDFSParquetImporter(Config cfg) throws IOException {
    this.cfg = cfg;
    this.props = cfg.propsFilePath == null ? UtilHelpers.buildProperties(cfg.configs) :
        UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();
    log.info("Creating Cleaner with configs : " + props.toString());
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    HDFSParquetImporter dataImporter = new HDFSParquetImporter(cfg);
    dataImporter
        .dataImport(UtilHelpers.buildSparkContext("data-importer-" + cfg.tableName, cfg.sparkMaster, cfg.sparkMemory),
            cfg.retry);
  }

  public int dataImport(JavaSparkContext jsc, int retry) throws Exception {
    this.fs = FSUtils.getFs(cfg.targetPath, jsc.hadoopConfiguration());
    int ret = -1;
    try {
      // Verify that targetPath is not present.
      if (fs.exists(new Path(cfg.targetPath))) {
        throw new HoodieIOException(String.format("Make sure %s is not present.", cfg.targetPath));
      }
      do {
        ret = dataImport(jsc);
      } while (ret != 0 && retry-- > 0);
    } catch (Throwable t) {
      logger.error(t);
    }
    return ret;
  }

  @VisibleForTesting
  protected int dataImport(JavaSparkContext jsc) throws IOException {
    try {
      if (fs.exists(new Path(cfg.targetPath))) {
        // cleanup target directory.
        fs.delete(new Path(cfg.targetPath), true);
      }

      //Get schema.
      String schemaStr = UtilHelpers.parseSchema(fs, cfg.schemaFile);

      // Initialize target hoodie table.
      Properties properties = new Properties();
      properties.put(HoodieTableConfig.HOODIE_TABLE_NAME_PROP_NAME, cfg.tableName);
      properties.put(HoodieTableConfig.HOODIE_TABLE_TYPE_PROP_NAME, cfg.tableType);
      HoodieTableMetaClient
          .initializePathAsHoodieDataset(jsc.hadoopConfiguration(), cfg.targetPath, properties);

      HoodieWriteClient client = UtilHelpers.createHoodieClient(jsc, cfg.targetPath, schemaStr,
          cfg.parallelism, Optional.empty(), props);

      JavaRDD<HoodieRecord<HoodieRecordPayload>> hoodieRecords = buildHoodieRecordsForImport(jsc, schemaStr);
      // Get instant time.
      String instantTime = client.startCommit();
      JavaRDD<WriteStatus> writeResponse = load(client, instantTime, hoodieRecords);
      return UtilHelpers.handleErrors(jsc, instantTime, writeResponse);
    } catch (Throwable t) {
      logger.error("Error occurred.", t);
    }
    return -1;
  }

  protected JavaRDD<HoodieRecord<HoodieRecordPayload>> buildHoodieRecordsForImport(
      JavaSparkContext jsc, String schemaStr) throws IOException {
    Job job = Job.getInstance(jsc.hadoopConfiguration());
    // Allow recursive directories to be found
    job.getConfiguration().set(FileInputFormat.INPUT_DIR_RECURSIVE, "true");
    // To parallelize reading file status.
    job.getConfiguration().set(FileInputFormat.LIST_STATUS_NUM_THREADS, "1024");
    AvroReadSupport
        .setAvroReadSchema(jsc.hadoopConfiguration(), (new Schema.Parser().parse(schemaStr)));
    ParquetInputFormat.setReadSupportClass(job, (AvroReadSupport.class));

    return jsc.newAPIHadoopFile(cfg.srcPath,
        ParquetInputFormat.class, Void.class, GenericRecord.class, job.getConfiguration())
        // To reduce large number of
        // tasks.
        .coalesce(16 * cfg.parallelism)
        .map(entry -> {
          GenericRecord genericRecord
              = ((Tuple2<Void, GenericRecord>) entry)._2();
          Object partitionField =
              genericRecord.get(cfg.partitionKey);
          if (partitionField == null) {
            throw new HoodieIOException(
                "partition key is missing. :"
                    + cfg.partitionKey);
          }
          Object rowField = genericRecord.get(cfg.rowKey);
          if (rowField == null) {
            throw new HoodieIOException(
                "row field is missing. :" + cfg.rowKey);
          }
          String partitionPath = partitionField.toString();
          logger.info("Row Key : " + rowField + ", Partition Path is (" + partitionPath + ")");
          if (partitionField instanceof Number) {
            try {
              long ts = (long) (Double.parseDouble(partitionField.toString()) * 1000L);
              partitionPath =
                  PARTITION_FORMATTER.format(new Date(ts));
            } catch (NumberFormatException nfe) {
              logger.warn("Unable to parse date from partition field. Assuming partition as (" + partitionField + ")");
            }
          }
          return new HoodieRecord<>(
              new HoodieKey(
                  (String) rowField, partitionPath),
              new HoodieJsonPayload(
                  genericRecord.toString()));
        });
  }

  /**
   * Imports records to Hoodie dataset
   *
   * @param client        Hoodie Client
   * @param instantTime   Instant Time
   * @param hoodieRecords Hoodie Records
   * @param <T>           Type
   */
  protected <T extends HoodieRecordPayload> JavaRDD<WriteStatus> load(HoodieWriteClient client,
      String instantTime, JavaRDD<HoodieRecord<T>> hoodieRecords) {
    if (cfg.command.toLowerCase().equals("insert")) {
      return client.insert(hoodieRecords, instantTime);
    }
    return client.upsert(hoodieRecords, instantTime);
  }

  public static class FormatValidator implements IValueValidator<String> {

    List<String> validFormats = Arrays.asList("parquet");

    @Override
    public void validate(String name, String value) throws ParameterException {
      if (value == null || !validFormats.contains(value)) {
        throw new ParameterException(String.format(
            "Invalid format type: value:%s: supported formats:%s", value, validFormats));
      }
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--command", "-c"},
        description = "Write command Valid values are insert(default)/upsert",
        required = false)
    public String command = "INSERT";
    @Parameter(names = {"--src-path",
        "-sp"}, description = "Base path for the input dataset", required = true)
    public String srcPath = null;
    @Parameter(names = {"--target-path",
        "-tp"}, description = "Base path for the target hoodie dataset", required = true)
    public String targetPath = null;
    @Parameter(names = {"--table-name", "-tn"}, description = "Table name", required = true)
    public String tableName = null;
    @Parameter(names = {"--table-type", "-tt"}, description = "Table type", required = true)
    public String tableType = null;
    @Parameter(names = {"--row-key-field",
        "-rk"}, description = "Row key field name", required = true)
    public String rowKey = null;
    @Parameter(names = {"--partition-key-field",
        "-pk"}, description = "Partition key field name", required = true)
    public String partitionKey = null;
    @Parameter(names = {"--parallelism",
        "-pl"}, description = "Parallelism for hoodie insert", required = true)
    public int parallelism = 1;
    @Parameter(names = {"--schema-file",
        "-sf"}, description = "path for Avro schema file", required = true)
    public String schemaFile = null;
    @Parameter(names = {"--format",
        "-f"}, description = "Format for the input data.", required = false, validateValueWith =
        FormatValidator.class)
    public String format = null;
    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;
    @Parameter(names = {"--spark-memory",
        "-sm"}, description = "spark memory to use", required = true)
    public String sparkMemory = null;
    @Parameter(names = {"--retry", "-rt"}, description = "number of retries", required = false)
    public int retry = 0;
    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client for importing")
    public String propsFilePath = null;
    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--propsFilePath\") can also be passed command line using this parameter")
    public List<String> configs = new ArrayList<>();
    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }
}
