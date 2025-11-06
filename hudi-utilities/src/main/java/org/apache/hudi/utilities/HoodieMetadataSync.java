package org.apache.hudi.utilities;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;

public class HoodieMetadataSync implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataSync.class);

  // Spark context
  private transient JavaSparkContext jsc;
  // config
  private Config cfg;
  // Properties with source, hoodie client, key generator etc.
  private TypedProperties props;

  public HoodieMetadataSync(JavaSparkContext jsc, Config cfg) {
    this.jsc = jsc;
    this.cfg = cfg;
    this.props = UtilHelpers.buildProperties(cfg.configs);
  }

  /**
   * Reads config from the file system.
   *
   * @param jsc {@link JavaSparkContext} instance.
   * @param cfg {@link TableSizeStats.Config} instance.
   * @return the {@link TypedProperties} instance.
   */
  private TypedProperties readConfigFromFileSystem(JavaSparkContext jsc, TableSizeStats.Config cfg) {
    return UtilHelpers.readConfig(jsc.hadoopConfiguration(), new Path(cfg.propsFilePath), cfg.configs)
        .getProps(true);
  }

  public static class Config implements Serializable {
    @Parameter(names = {"--source-base-path", "-sbp"}, description = "Source Base path for the table", required = true)
    public String sourceBasePath = null;

    @Parameter(names = {"--target-base-path", "-tbp"}, description = "Target Base path for the table", required = true)
    public String targetBasePath = null;

    @Parameter(names = {"--target-table-name", "-ttn"}, description = "Target table name", required = true)
    public String targetTableName = null;

    @Parameter(names = {"--commit-to-sync", "-cts"}, description = "Commit of interest to sync", required = false)
    public String commitToSync = null;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = null;

    @Parameter(names = {"--spark-memory", "-sm"}, description = "spark memory to use", required = false)
    public String sparkMemory = "1g";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--props\") can also be passed command line using this parameter. This can be repeated",
        splitter = IdentitySplitter.class)
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    @Override
    public String toString() {
      return "TableSizeStats {\n"
          + "   --source-base-path " + sourceBasePath + ", \n"
          + "   --target-base-path " + targetBasePath + ", \n"
          + "   --commit-to-sync " + commitToSync + ", \n"
          + "   --hoodie-conf " + configs
          + "\n}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Config)) {
        return false;
      }
      Config config = (Config) o;
      return sourceBasePath.equals(config.sourceBasePath) && targetBasePath.equals(config.targetBasePath) && Objects.equals(commitToSync, config.commitToSync) &&
          Objects.equals(sparkMaster, config.sparkMaster) && Objects.equals(sparkMemory, config.sparkMemory);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceBasePath, targetBasePath, commitToSync, sparkMaster, sparkMemory);
    }
  }

  public static void main(String[] args) {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);

    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    SparkConf sparkConf = UtilHelpers.buildSparkConf("HoodieMetadataSync", cfg.sparkMaster);
    sparkConf.set("spark.executor.memory", cfg.sparkMemory);
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);

    try {
      HoodieMetadataSync hoodieMetadataSync = new HoodieMetadataSync(jsc, cfg);
      hoodieMetadataSync.run();
    } catch (TableNotFoundException e) {
      LOG.warn(String.format("The Hudi data table is not found: [%s].", cfg.sourceBasePath), e);
    } catch (Throwable throwable) {
      LOG.error("Failed to get table size stats for " + cfg, throwable);
    } finally {
      jsc.stop();
    }
  }

  public void run() throws Exception {
    Path sourceTablePath = new Path(cfg.targetBasePath);
    FileSystem fs = sourceTablePath.getFileSystem(jsc.hadoopConfiguration());
    HoodieTableMetaClient sourceTableMetaClient = HoodieTableMetaClient.builder().setBasePath(cfg.sourceBasePath).setConf(jsc.hadoopConfiguration())
        .build();
    HoodieCommitMetadata sourceCommitMetadata = getHoodieCommitMetadata(cfg.commitToSync, sourceTableMetaClient);

    Path targetTablePath = new Path(cfg.targetBasePath);
    boolean targetTableExists = fs.exists(targetTablePath);
    if (!targetTableExists) {
      LOG.info("Initializing target table for first time", cfg.targetBasePath);
      TypedProperties props = sourceTableMetaClient.getTableConfig().getProps();
      props.remove("hoodie.table.metadata.partitions");
      props.setProperty(HoodieTableConfig.ALLOW_BASE_PATH_OVERRIDES_WITH_METADATA.key() ,"true");
      props.setProperty(HoodieTableConfig.NUM_PARTITION_PATH_LEVELS.key(), "1");
      HoodieTableMetaClient.withPropertyBuilder()
          .fromProperties(props)
          .setTableName(cfg.targetTableName)
          .initTable(jsc.hadoopConfiguration(), cfg.targetBasePath);
    }
    HoodieTableMetaClient targetTableMetaClient = HoodieTableMetaClient.builder().setBasePath(cfg.targetBasePath)
        .setConf(jsc.hadoopConfiguration()).build();
    Schema schema = new TableSchemaResolver(sourceTableMetaClient).getTableAvroSchema(false);
    HoodieWriteConfig writeConfig = getWriteConfig(schema, targetTableMetaClient, cfg.sourceBasePath);
    HoodieSparkEngineContext hoodieSparkEngineContext = new HoodieSparkEngineContext(jsc);
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(hoodieSparkEngineContext, writeConfig)) {
      String commitTime = writeClient.startCommit(); // single writer. will rollback any pending commits from previous round.
      targetTableMetaClient
          .reloadActiveTimeline()
          .transitionRequestedToInflight(
                  HoodieTimeline.COMMIT_ACTION, // to be fixed.
                  commitTime);
      HoodieCommitMetadata commitMetadata = fixHoodieCommitMetadata(sourceCommitMetadata, commitTime, schema.toString());
      HoodieSparkTable sparkTable = HoodieSparkTable.create(writeConfig, hoodieSparkEngineContext, targetTableMetaClient);
      try (HoodieTableMetadataWriter hoodieTableMetadataWriter =
               (HoodieTableMetadataWriter) sparkTable.getMetadataWriter(commitTime).get()) {
        hoodieTableMetadataWriter.update(commitMetadata, hoodieSparkEngineContext.emptyHoodieData(), commitTime);
      }
      targetTableMetaClient
          .reloadActiveTimeline()
          .saveAsComplete(new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime),
              Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    }
    LOG.info("Completed syncing " + cfg.commitToSync +" to target table");
  }

  private HoodieCommitMetadata getHoodieCommitMetadata(String instantTime, HoodieTableMetaClient metaClient) throws IOException {
    Option<HoodieInstant> instantOpt = metaClient.getActiveTimeline().filterCompletedInstants().filter(instant -> instant.getTimestamp().equalsIgnoreCase(instantTime)).firstInstant();
    if (instantOpt.isPresent()) {
      HoodieInstant instant = instantOpt.get();
      return HoodieCommitMetadata.fromBytes(
          metaClient.getCommitsTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
    } else {
      throw new HoodieException("Could not find " + instantTime + " in source table timeline ");
    }
  }

  private HoodieCommitMetadata fixHoodieCommitMetadata(HoodieCommitMetadata sourceCommitMetadata, String commitTime, String schemaStr) {
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();

    for (HoodieWriteStat writeStat : sourceCommitMetadata.getWriteStats()) {
      String partition = writeStat.getPartitionPath();
      fixWriteStatusForExternalPaths(writeStat, commitTime);
      commitMetadata.addWriteStat(partition, writeStat);
    }

    commitMetadata.setOperationType(WriteOperationType.UPSERT); // to be fixed.
    commitMetadata.setCompacted(false);
    commitMetadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, schemaStr);
    if (!sourceCommitMetadata.getExtraMetadata().isEmpty()) {
      sourceCommitMetadata.getExtraMetadata().entrySet().forEach(entry ->  { sourceCommitMetadata.addMetadata(entry.getKey(), entry.getValue());});
    }
    return commitMetadata;
  }

  private void fixWriteStatusForExternalPaths(HoodieWriteStat writeStat, String commitTime) {
    String originalPath = writeStat.getPath();
    writeStat.setPath(ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(originalPath, commitTime));
  }

  private HoodieWriteConfig getWriteConfig(
      Schema schema,
      HoodieTableMetaClient metaClient,
      String basePathOverride) {
    return getWriteConfig(schema, Integer.parseInt(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.defaultValue()),
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.defaultValue(), 120, metaClient,
        basePathOverride);
  }

  private HoodieWriteConfig getWriteConfig(
      Schema schema,
      int numCommitsToKeep,
      int maxNumDeltaCommitsBeforeCompaction,
      int timelineRetentionInHours,
      HoodieTableMetaClient metaClient,
      String basePathOverride) {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
    return HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePathV2().toString())
        .withPopulateMetaFields(metaClient.getTableConfig().populateMetaFields())
        .withEmbeddedTimelineServerEnabled(false)
        .withSchema(schema == null ? "" : schema.toString())
        .withArchivalConfig(
            HoodieArchivalConfig.newBuilder()
                .archiveCommitsWith(
                    Math.max(0, numCommitsToKeep - 1), Math.max(1, numCommitsToKeep))
                .withAutoArchive(false)
                .build())
        .withCleanConfig(
            HoodieCleanConfig.newBuilder()
                .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS)
                .cleanerNumHoursRetained(timelineRetentionInHours)
                .withAutoClean(false)
                .build())
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder()
                .enable(true)
                .withProperties(properties)
                .withMetadataIndexColumnStats(false)
                .withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction)
                .withEnableBasePathForPartitions(true)
                .withBasePathOverride(basePathOverride)
                .build())
        .build();
  }
}
