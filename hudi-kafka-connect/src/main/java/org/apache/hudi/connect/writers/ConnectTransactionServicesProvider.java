package org.apache.hudi.connect.writers;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.sync.common.AbstractSyncTool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ConnectTransactionServicesProvider extends HudiConnectWriterProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ConnectTransactionServicesProvider.class);
  private static final String TABLE_FORMAT = "PARQUET";

  private Option<HoodieTableMetaClient> tableMetaClient;

  public ConnectTransactionServicesProvider(
      HudiConnectConfigs connectConfigs) throws HoodieException {
    super(connectConfigs, new TopicPartition("", -1));
    // initialize the table, if not done already
    Path path = new Path(tableBasePath);
    syncHive();
    // ToDo shld try to avoid spark dependency
    //String partitionColumns = HoodieSparkUtils.getPartitionColumns(keyGenerator, connectConfigs.getProps());
    try {
      tableMetaClient = Option.of(HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE.name())
          .setTableName(tableName)
          .setPayloadClassName(HoodieAvroPayload.class.getName())
          //.setBaseFileFormat()
          //.setPartitionFields(partitionColumns)
          //.setRecordKeyFields()
          .setKeyGeneratorClassProp(writeConfig.getKeyGeneratorClass())
          //.setPreCombineField()
          .initTable(hadoopConf, tableBasePath));
    } catch (Exception exception) {
      throw new HoodieException("Fatal error instantiating Hudi Transaction Services ", exception);
    }
  }

  public String startCommit() {
    String newCommitTime = hoodieJavaWriteClient.startCommit();
    hoodieJavaWriteClient.preBulkWrite(newCommitTime);
    LOG.info("Starting Hudi commit " + newCommitTime);
    return newCommitTime;
  }

  public void endCommit(String commitTime, List<WriteStatus> writeStatuses, Map<String, String> extraMetadata) {
    hoodieJavaWriteClient.commit(commitTime, writeStatuses, Option.of(extraMetadata),
        HoodieActiveTimeline.COMMIT_ACTION, Collections.emptyMap());
    LOG.info("Ending Hudi commit " + commitTime);
    syncMeta();
  }

  public Map<String, String> loadLatestCommitMetadata() {
    if (tableMetaClient.isPresent()) {
      Option<HoodieCommitMetadata> metadata = CommitUtils.getCommitMetadataForLatestInstant(tableMetaClient.get());
      if (metadata.isPresent()) {
        return metadata.get().getExtraMetadata();
      } else {
        LOG.info("Hoodie Extra Metadata from latest commit is absent");
        return Collections.emptyMap();
      }
    }
    throw new HoodieException("Fatal error retrieving Hoodie Extra Metadata since Table Meta Client is absent");
  }

  public void syncMeta() {
    Set<String> syncClientToolClasses = new HashSet<>(
        Arrays.asList(connectConfigs.getMetaSyncClasses().split(",")));
    if (connectConfigs.isMetaSyncEnabled()) {
      for (String impl : syncClientToolClasses) {
        impl = impl.trim();
        switch (impl) {
          case "org.apache.hudi.hive.HiveSyncTool":
            syncHive();
            break;
          default:
            FileSystem fs = FSUtils.getFs(tableBasePath, new Configuration());
            Properties properties = new Properties();
            properties.putAll(connectConfigs.getProps());
            properties.put("basePath", tableBasePath);
            properties.put("baseFileFormat", TABLE_FORMAT);
            AbstractSyncTool syncTool = (AbstractSyncTool) ReflectionUtils.loadClass(impl, new Class[] {Properties.class, FileSystem.class}, properties, fs);
            syncTool.syncHoodieTable();
        }
      }
    }
  }

  private void syncHive() {
    HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(
        new TypedProperties(connectConfigs.getProps()),
        tableBasePath,
        TABLE_FORMAT);
    LOG.info("Syncing target hoodie table with hive table("
        + hiveSyncConfig.tableName
        + "). Hive metastore URL :"
        + hiveSyncConfig.jdbcUrl
        + ", basePath :" + tableBasePath);
    LOG.info("Hive Sync Conf => " + hiveSyncConfig.toString());
    HiveConf hiveConf = new HiveConf(fs.getConf(), HiveConf.class);
    LOG.info("Hive Conf => " + hiveConf.getAllProperties().toString());
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable();
  }
}
