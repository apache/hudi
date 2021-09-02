package org.apache.hudi.writers;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.connect.writers.HudiConnectBufferedWriter;
import org.apache.hudi.connect.writers.HudiConnectConfigs;
import org.apache.hudi.schema.SchemaProvider;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.Comparator;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHudiConnectBufferedWriter {

  private static final int NUM_RECORDS = 10;
  private static final String COMMIT_TIME = "101";

  private HoodieJavaWriteClient mockHoodieJavaWriteClient;
  private HoodieJavaEngineContext javaEngineContext;
  private HudiConnectConfigs configs;
  private HoodieWriteConfig writeConfig;
  private SchemaProvider schemaProvider;

  @BeforeEach
  public void setUp() throws Exception {
    mockHoodieJavaWriteClient = mock(HoodieJavaWriteClient.class);
    Configuration hadoopConf = new Configuration();
    javaEngineContext = new HoodieJavaEngineContext(hadoopConf);
    configs = HudiConnectConfigs.newBuilder().build();
    schemaProvider = new TestAbstractHudiConnectWriter.TestSchemaProvider();
    writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp")
        .withSchema(schemaProvider.getSourceSchema().toString())
        .build();
  }

  @Test
  public void testSimpleWriteAndFlush() throws Exception {
    String partitionPath = HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS[0];
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator(new String[] {partitionPath});
    List<HoodieRecord> records = dataGen.generateInserts(COMMIT_TIME, NUM_RECORDS);

    HudiConnectBufferedWriter writer = new HudiConnectBufferedWriter(
        javaEngineContext,
        mockHoodieJavaWriteClient,
        COMMIT_TIME,
        configs,
        writeConfig,
        null,
        schemaProvider);

    for (int i = 0; i < NUM_RECORDS; i++) {
      writer.writeHudiRecord(records.get(i));
    }
    Mockito.verify(mockHoodieJavaWriteClient, times(0))
        .bulkInsertPreppedRecords(anyList(), eq(COMMIT_TIME), eq(Option.empty()));

    writer.flushHudiRecords();
    final ArgumentCaptor<List<HoodieRecord>> actualRecords = ArgumentCaptor.forClass(List.class);
    Mockito.verify(mockHoodieJavaWriteClient, times(1))
        .bulkInsertPreppedRecords(actualRecords.capture(), eq(COMMIT_TIME), eq(Option.empty()));

    actualRecords.getValue().sort(Comparator.comparing(HoodieRecord::getRecordKey));
    records.sort(Comparator.comparing(HoodieRecord::getRecordKey));

    assertEquals(records, actualRecords.getValue());
  }
}
