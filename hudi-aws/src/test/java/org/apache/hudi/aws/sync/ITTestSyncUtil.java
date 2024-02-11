package org.apache.hudi.aws.sync;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.index.HoodieIndex;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;

public class ITTestSyncUtil {
  protected static final String TABLE_PATH = "file:///tmp/hoodie/sample-table";
  protected static final String TABLE_TYPE = "COPY_ON_WRITE";
  protected static final String DB_NAME = "db_name";
  protected static final String TABLE_NAME = "tbl_name";
  protected final Configuration hadoopConf = new Configuration();

  protected static void addMetaSyncProps(Properties hiveProps, String parts) {
    hiveProps.setProperty(META_SYNC_BASE_PATH.key(), TABLE_PATH);
    hiveProps.setProperty(META_SYNC_DATABASE_NAME.key(), DB_NAME);
    hiveProps.setProperty(HiveSyncConfig.META_SYNC_DATABASE_NAME.key(), DB_NAME);
    hiveProps.setProperty(HiveSyncConfig.META_SYNC_TABLE_NAME.key(), TABLE_NAME);
    hiveProps.setProperty(HiveSyncConfig.META_SYNC_BASE_PATH.key(), TABLE_PATH);
    hiveProps.setProperty(HiveSyncConfig.META_SYNC_PARTITION_FIELDS.key(), parts);
  }


  protected HoodieJavaWriteClient<HoodieAvroPayload> clientCOW(String avroSchema, Optional<String> hudiPartitions) throws IOException {
    HoodieTableMetaClient.PropertyBuilder propertyBuilder = HoodieTableMetaClient.withPropertyBuilder();
    if(hudiPartitions.isPresent()) {
      propertyBuilder=propertyBuilder.setPartitionFields(hudiPartitions.get());
    }
    propertyBuilder
            .setTableType(TABLE_TYPE)
            .setTableName(TABLE_NAME)
            .setPayloadClassName(HoodieAvroPayload.class.getName())
            .initTable(hadoopConf, TABLE_PATH);

    HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(TABLE_PATH)
            .withSchema(avroSchema).withParallelism(1, 1)
            .withDeleteParallelism(1).forTable(TABLE_NAME)
            .withEmbeddedTimelineServerEnabled(false)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();

    return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);
  }

  protected static List<HoodieRecord<HoodieAvroPayload>> getHoodieRecords(String newCommitTime, int numRecords, String...partitionPath) {
    HoodieDataGenerator<HoodieAvroPayload> dataGen = new HoodieDataGenerator<>(partitionPath);
    List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, numRecords);
    List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
    List<HoodieRecord<HoodieAvroPayload>> writeRecords =
            recordsSoFar.stream().map(r -> new HoodieAvroRecord<>(r)).collect(Collectors.toList());
    return writeRecords;
  }
  protected FileSystem getFs() {
    return HadoopFSUtils.getFs(TABLE_PATH, hadoopConf);
  }
  @BeforeEach
  public void cleanUp() {
    try {
      getFs().delete(new Path(TABLE_PATH), true);
    } catch (IOException e) {
      throw new RuntimeException("Failed to delete table path " + TABLE_PATH);
    }
  }
}
