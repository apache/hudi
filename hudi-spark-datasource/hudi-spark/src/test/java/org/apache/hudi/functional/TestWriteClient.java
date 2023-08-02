package org.apache.hudi.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.testutils.HoodieSparkClientTestBase;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests write client functionality.
 */
@Tag("functional")
public class TestWriteClient extends HoodieSparkClientTestBase {

  @Test
  public void testInertsWithEmptyCommitsHavingWriterSchemaAsNull() throws Exception {
    HoodieWriteConfig.Builder cfgBuilder = getConfigBuilder().withAutoCommit(false);
    addConfigsForPopulateMetaFields(cfgBuilder, true);
    SparkRDDWriteClient client = getHoodieWriteClient(cfgBuilder.build());
    try {
      String firstCommit = "001";
      int numRecords = 200;
      JavaRDD<WriteStatus> result = insertFirstBatch(cfgBuilder.build(), client, firstCommit, "000", numRecords, SparkRDDWriteClient::insert,
          false, false, numRecords);
      assertTrue(client.commit(firstCommit, result), "Commit should succeed");

      // Re-init client with null writer schema.
      cfgBuilder = getConfigBuilder((String) null).withAutoCommit(false);
      client = getHoodieWriteClient(cfgBuilder.build());
      String secondCommit = "002";
      client.startCommitWithTime(secondCommit);
      JavaRDD<HoodieRecord> emptyRdd = context.emptyRDD();
      result = client.insert(emptyRdd, secondCommit);
      assertTrue(client.commit(secondCommit, result), "Commit should succeed");
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(jsc.hadoopConfiguration()).setBasePath(basePath).build();
      HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromBytes(timeline.getInstantDetails(timeline.lastInstant().get()).get(), HoodieCommitMetadata.class);
      assertTrue(metadata.getExtraMetadata().get("schema").isEmpty());
      TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
      assertEquals(Schema.parse(TRIP_EXAMPLE_SCHEMA), tableSchemaResolver.getTableAvroSchema(false));
      assertEquals(numRecords, sparkSession.read().format("org.apache.hudi").load(basePath).count());
    } finally {
      client.close();
    }
  }
}
