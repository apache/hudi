/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.deltastreamer;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.HoodieWriteClient;
import com.uber.hoodie.KeyGenerator;
import com.uber.hoodie.OverwriteWithLatestAvroPayload;
import com.uber.hoodie.SimpleKeyGenerator;
import com.uber.hoodie.WriteStatus;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.utilities.HiveIncrementalPuller;
import com.uber.hoodie.utilities.UtilHelpers;
import com.uber.hoodie.utilities.exception.HoodieDeltaStreamerException;
import com.uber.hoodie.utilities.schema.FilebasedSchemaProvider;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.JsonDFSSource;
import com.uber.hoodie.utilities.sources.Source;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.collection.JavaConversions;

/**
 * An Utility which can incrementally take the output from {@link HiveIncrementalPuller} and apply
 * it to the target dataset. Does not maintain any state, queries at runtime to see how far behind
 * the target dataset is from the source dataset. This can be overriden to force sync from a
 * timestamp.
 */
public class HoodieDeltaStreamer implements Serializable {

  private static volatile Logger log = LogManager.getLogger(HoodieDeltaStreamer.class);

  public static String CHECKPOINT_KEY = "deltastreamer.checkpoint.key";

  private final Config cfg;

  /**
   * Source to pull deltas from
   */
  private transient Source source;

  /**
   * Schema provider that supplies the command for reading the input and writing out the target
   * table.
   */
  private transient SchemaProvider schemaProvider;

  /**
   * Extract the key for the target dataset
   */
  private KeyGenerator keyGenerator;

  /**
   * Filesystem used
   */
  private transient FileSystem fs;

  /**
   * Timeline with completed commits
   */
  private transient Optional<HoodieTimeline> commitTimelineOpt;

  /**
   * Spark context
   */
  private transient JavaSparkContext jssc;


  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;


  public HoodieDeltaStreamer(Config cfg, JavaSparkContext jssc) throws IOException {
    this.cfg = cfg;
    this.jssc = jssc;
    this.fs = FSUtils.getFs(cfg.targetBasePath, jssc.hadoopConfiguration());

    if (fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient meta = new HoodieTableMetaClient(fs.getConf(), cfg.targetBasePath);
      this.commitTimelineOpt = Optional.of(meta.getActiveTimeline().getCommitsTimeline()
          .filterCompletedInstants());
    } else {
      this.commitTimelineOpt = Optional.empty();
    }

    this.props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath)).getConfig();
    log.info("Creating delta streamer with configs : " + props.toString());
    this.schemaProvider = UtilHelpers.createSchemaProvider(cfg.schemaProviderClassName, props, jssc);
    this.keyGenerator = DataSourceUtils.createKeyGenerator(cfg.keyGeneratorClass, props);
    this.source = UtilHelpers.createSource(cfg.sourceClassName, props, jssc, schemaProvider);

    // register the schemas, so that shuffle does not serialize the full schemas
    List<Schema> schemas = Arrays.asList(schemaProvider.getSourceSchema(),
        schemaProvider.getTargetSchema());
    jssc.sc().getConf().registerAvroSchemas(JavaConversions.asScalaBuffer(schemas).toList());
  }

  public void sync() throws Exception {
    // Retrieve the previous round checkpoints, if any
    Optional<String> resumeCheckpointStr = Optional.empty();
    if (commitTimelineOpt.isPresent()) {
      Optional<HoodieInstant> lastCommit = commitTimelineOpt.get().lastInstant();
      if (lastCommit.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            commitTimelineOpt.get().getInstantDetails(lastCommit.get()).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getMetadata(CHECKPOINT_KEY) != null) {
          resumeCheckpointStr = Optional.of(commitMetadata.getMetadata(CHECKPOINT_KEY));
        } else {
          throw new HoodieDeltaStreamerException(
              "Unable to find previous checkpoint. Please double check if this table "
                  + "was indeed built via delta streamer ");
        }
      }
    } else {
      HoodieTableMetaClient.initTableType(jssc.hadoopConfiguration(), cfg.targetBasePath,
          cfg.storageType, cfg.targetTableName, "archived");
    }
    log.info("Checkpoint to resume from : " + resumeCheckpointStr);

    // Pull the data from the source & prepare the write
    Pair<Optional<JavaRDD<GenericRecord>>, String> dataAndCheckpoint = source.fetchNewData(
        resumeCheckpointStr, cfg.sourceLimit);

    if (!dataAndCheckpoint.getKey().isPresent()) {
      log.info("No new data, nothing to commit.. ");
      return;
    }

    JavaRDD<GenericRecord> avroRDD = dataAndCheckpoint.getKey().get();
    JavaRDD<HoodieRecord> records = avroRDD.map(gr -> {
      HoodieRecordPayload payload = DataSourceUtils.createPayload(cfg.payloadClassName, gr,
          (Comparable) gr.get(cfg.sourceOrderingField));
      return new HoodieRecord<>(keyGenerator.getKey(gr), payload);
    });

    // filter dupes if needed
    HoodieWriteConfig hoodieCfg = getHoodieClientConfig();
    if (cfg.filterDupes) {
      // turn upserts to insert
      cfg.operation = cfg.operation == Operation.UPSERT ? Operation.INSERT : cfg.operation;
      records = DataSourceUtils.dropDuplicates(jssc, records, hoodieCfg);
    }

    if (records.isEmpty()) {
      log.info("No new data, nothing to commit.. ");
      return;
    }

    // Perform the write
    HoodieWriteClient client = new HoodieWriteClient<>(jssc, hoodieCfg);
    String commitTime = client.startCommit();
    log.info("Starting commit  : " + commitTime);

    JavaRDD<WriteStatus> writeStatusRDD;
    if (cfg.operation == Operation.INSERT) {
      writeStatusRDD = client.insert(records, commitTime);
    } else if (cfg.operation == Operation.UPSERT) {
      writeStatusRDD = client.upsert(records, commitTime);
    } else if (cfg.operation == Operation.BULK_INSERT) {
      writeStatusRDD = client.bulkInsert(records, commitTime);
    } else {
      throw new HoodieDeltaStreamerException("Unknown operation :" + cfg.operation);
    }

    // Simply commit for now. TODO(vc): Support better error handlers later on
    HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
    checkpointCommitMetadata.put(CHECKPOINT_KEY, dataAndCheckpoint.getValue());

    boolean success = client.commit(commitTime, writeStatusRDD,
        Optional.of(checkpointCommitMetadata));
    if (success) {
      log.info("Commit " + commitTime + " successful!");
      // TODO(vc): Kick off hive sync from here.
    } else {
      log.info("Commit " + commitTime + " failed!");
    }
    client.close();
  }

  private HoodieWriteConfig getHoodieClientConfig() throws Exception {
    return HoodieWriteConfig.newBuilder().combineInput(true, true).withPath(cfg.targetBasePath)
        .withAutoCommit(false)
        .withSchema(schemaProvider.getTargetSchema().toString())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withPayloadClass(cfg.payloadClassName).build())
        .forTable(cfg.targetTableName)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withProps(props).build();
  }

  public enum Operation {
    UPSERT, INSERT, BULK_INSERT
  }

  private class OperationConvertor implements IStringConverter<Operation> {
    @Override
    public Operation convert(String value) throws ParameterException {
      return Operation.valueOf(value);
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--target-base-path"}, description = "base path for the target hoodie dataset. "
        + "(Will be created if did not exist first time around. If exists, expected to be a hoodie dataset)",
        required = true)
    public String targetBasePath;

    // TODO: How to obtain hive configs to register?
    @Parameter(names = {"--target-table"}, description = "name of the target table in Hive", required = true)
    public String targetTableName;

    @Parameter(names = {"--storage-type"}, description = "Type of Storage. "
        + "COPY_ON_WRITE (or) MERGE_ON_READ", required = true)
    public String storageType;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
        + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
        + "to individual classes, for supported properties.")
    public String propsFilePath =
        "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/dfs-source.properties";

    @Parameter(names = {"--source-class"}, description = "Subclass of com.uber.hoodie.utilities.sources to read data. "
        + "Built-in options: com.uber.hoodie.utilities.sources.{JsonDFSSource (default), AvroDFSSource, "
        + "JsonKafkaSource, AvroKafkaSource, HiveIncrPullSource}")
    public String sourceClassName = JsonDFSSource.class.getName();

    @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
        + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record")
    public String sourceOrderingField = "ts";

    @Parameter(names = {"--key-generator-class"}, description = "Subclass of com.uber.hoodie.KeyGenerator "
        + "to generate a HoodieKey from the given avro record. Built in: SimpleKeyGenerator (uses "
        + "provided field names as recordkey & partitionpath. Nested fields specified via dot notation, e.g: a.b.c)")
    public String keyGeneratorClass = SimpleKeyGenerator.class.getName();

    @Parameter(names = {"--payload-class"}, description = "subclass of HoodieRecordPayload, that works off "
        + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value")
    public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

    @Parameter(names = {"--schemaprovider-class"}, description = "subclass of com.uber.hoodie.utilities.schema"
        + ".SchemaProvider to attach schemas to input & target table data, built in options: FilebasedSchemaProvider")
    public String schemaProviderClassName = FilebasedSchemaProvider.class.getName();

    @Parameter(names = {"--source-limit"}, description = "Maximum amount of data to read from source. "
        + "Default: No limit For e.g: DFSSource => max bytes to read, KafkaSource => max events to read")
    public long sourceLimit = Long.MAX_VALUE;

    @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
        + "is purely new data/inserts to gain speed)",
        converter = OperationConvertor.class)
    public Operation operation = Operation.UPSERT;

    @Parameter(names = {"--filter-dupes"}, description = "Should duplicate records from source be dropped/filtered out"
        + "before insert/bulk-insert")
    public Boolean filterDupes = false;

    @Parameter(names = {"--spark-master"}, description = "spark master to use.")
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    JavaSparkContext jssc = UtilHelpers.buildSparkContext("delta-streamer-" + cfg.targetTableName, cfg.sparkMaster);
    new HoodieDeltaStreamer(cfg, jssc).sync();
  }
}
